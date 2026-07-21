"""Stream framing — server-side StreamWriter and client-side Stream iterator.

Mirrors pkg/bus/stream.go. A streaming response is a sequence of envelopes
sharing the same ``correlation_id``; the runtime tags each frame with a
monotonic ``seq`` and a terminal frame (``ResponseFinal`` or ``ResponseError``)
sets ``is_final = True``.
"""
from __future__ import annotations

import asyncio
import enum
from typing import Any

from openagentio.codec.json_codec import Codec
from openagentio.event.envelope import Envelope
from openagentio.bus.errors import (
    BackpressureDropError,
    error_code_for,
    is_retryable_for,
)
from openagentio.event.payload import ErrorPayload
from openagentio.event.types import (
    ResponseDelta,
    ResponseError,
    ResponseFinal,
    ResponseStarted,
)
from openagentio.transport.base import Inbox, RawMessage, Transport


# Defaults for the client-side out-of-order buffer. Mirror the Go SDK.
DEFAULT_MAX_PENDING_FRAMES = 256
DEFAULT_MAX_SEQUENCE_GAP = 1024


class ErrIdleTimeout(Exception):
    """Raised when the gap between two streaming frames exceeds the idle timeout."""


class _WriterState(enum.Enum):
    """Lifecycle of :class:`StreamWriter`.

    Mirrors ``writerState`` in ``pkg/bus/stream.go``. Prior to P0#3 the writer
    kept a single ``_closed`` boolean that was flipped before encode+publish;
    a codec/transport failure would then wedge the writer with no terminal
    frame on the wire. Modelling ``closing`` and ``failed`` as first-class
    states lets the runtime react (fallback error, structured logging) instead
    of silently swallowing the failure.
    """

    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"
    FAILED = "failed"


class StreamWriter:
    """Server-side stream emitter.

    Each method publishes one frame back to the request's ``reply_to`` subject.
    ``started`` may be called at most once. ``final`` and ``error`` are
    terminal — after either succeeds the writer transitions to ``CLOSED`` and
    subsequent calls raise. If encoding or publishing a terminal frame fails
    the writer transitions to ``FAILED`` instead: :meth:`final` / :meth:`error`
    raise the underlying exception, and the runtime may retry via
    :meth:`error` (permitted from both ``OPEN`` and ``FAILED``) to still land
    a terminal frame on the wire.
    """

    def __init__(
        self,
        transport: Transport,
        codec: Codec,
        agent_id: str,
        request: Envelope,
    ) -> None:
        self._transport = transport
        self._codec = codec
        self._agent_id = agent_id
        self._req = request
        self._lock = asyncio.Lock()
        self._seq = 0
        self._started = False
        self._state = _WriterState.OPEN
        # ``_last_err`` captures the most recent codec/publish failure so
        # bus._run_stream_handler can surface it when handler returned cleanly
        # but the terminal publish did not.
        self._last_err: BaseException | None = None
        # ``_reserved_seq`` holds the seq that a failed terminal frame
        # consumed. A fallback ``error(...)`` from ``FAILED`` must reuse that
        # seq so the client's reorder buffer sees a contiguous sequence —
        # otherwise the previously-reserved-but-never-published seq stays
        # missing forever and the client idle-times-out despite receiving
        # this terminal frame. ``_reserved_valid`` distinguishes "no
        # reservation" from "reservation is 0".
        self._reserved_seq: int = 0
        self._reserved_valid = False

    # --- state introspection ---------------------------------------------

    @property
    def state(self) -> _WriterState:
        return self._state

    @property
    def closed(self) -> bool:
        """Backwards-compat: returns True for any terminal state (closed OR
        failed). Callers that need to distinguish should check :attr:`state`.
        """
        return self._state in (_WriterState.CLOSED, _WriterState.FAILED)

    @property
    def last_error(self) -> BaseException | None:
        """Last codec/publish failure; ``None`` unless state is FAILED."""
        return self._last_err

    # --- terminal frames -------------------------------------------------

    async def started(self, meta: Any = None) -> None:
        async with self._lock:
            self._require_open_locked()
            if self._started:
                raise RuntimeError("stream: started already emitted")
            self._started = True
            seq = self._next_seq_locked()

        env = new_reply_shell(self._agent_id, self._req, ResponseStarted)
        env.seq = seq
        env.payload = self._codec.encode_payload(meta)
        await self._publish(env)

    async def delta(self, chunk: Any = None) -> None:
        async with self._lock:
            self._require_open_locked()
            seq = self._next_seq_locked()

        env = new_reply_shell(self._agent_id, self._req, ResponseDelta)
        env.seq = seq
        env.payload = self._codec.encode_payload(chunk)
        await self._publish(env)

    async def final(self, result: Any = None) -> None:
        """Publish a terminal ResponseFinal frame.

        Unlike the pre-P0#3 implementation, ``_closed`` is not set until
        after publish succeeds. If encoding or publishing fails the writer
        transitions to :attr:`_WriterState.FAILED`, the exception propagates,
        and the runtime is free to attempt a fallback :meth:`error`.
        """
        async with self._lock:
            self._require_open_locked()
            self._state = _WriterState.CLOSING
            seq = self._next_seq_locked()

        env = new_reply_shell(self._agent_id, self._req, ResponseFinal)
        env.seq = seq
        env.is_final = True
        try:
            env.payload = self._codec.encode_payload(result)
        except BaseException as e:
            await self._mark_failed(e, reserved_seq=seq)
            raise
        try:
            await self._publish(env)
        except BaseException as e:
            await self._mark_failed(e, reserved_seq=seq)
            raise
        await self._mark_closed()

    async def error(self, exc: BaseException) -> None:
        """Publish a terminal ResponseError frame.

        Permitted from ``OPEN`` (handler chose to fail) and ``FAILED``
        (previous Final could not reach the wire — try an error frame
        instead so the peer still sees a terminal). A publish failure here
        leaves the writer in ``FAILED`` and re-raises.
        """
        if exc is None:  # type: ignore[unreachable]
            raise ValueError("stream: error requires a non-None exception")

        async with self._lock:
            if self._state not in (_WriterState.OPEN, _WriterState.FAILED):
                raise self._terminal_state_error_locked()
            # Reuse the reserved seq from a prior failed terminal — see
            # ``__init__`` for the rationale. Without this, the fallback
            # error frame lands with the *next* seq while the previously
            # reserved seq stays missing on the wire; the client's reorder
            # buffer waits forever for a frame that will never arrive.
            if self._state is _WriterState.FAILED and self._reserved_valid:
                seq = self._reserved_seq
                self._reserved_valid = False
            else:
                seq = self._next_seq_locked()
            self._state = _WriterState.CLOSING

        env = new_reply_shell(self._agent_id, self._req, ResponseError)
        env.seq = seq
        env.is_final = True
        code = error_code_for(exc)
        retryable = is_retryable_for(exc)
        payload = ErrorPayload(code=code, message=str(exc), retryable=retryable)
        try:
            env.payload = self._codec.encode_payload(payload)
        except BaseException as e:
            await self._mark_failed(e, reserved_seq=seq)
            raise
        try:
            await self._publish(env)
        except BaseException as e:
            await self._mark_failed(e, reserved_seq=seq)
            raise
        await self._mark_closed()

    # --- internals -------------------------------------------------------

    def _require_open_locked(self) -> None:
        if self._state is _WriterState.OPEN:
            return
        raise self._terminal_state_error_locked()

    def _terminal_state_error_locked(self) -> RuntimeError:
        if self._state is _WriterState.CLOSING:
            return RuntimeError("stream: terminal frame in flight")
        if self._state is _WriterState.CLOSED:
            return RuntimeError("stream: already closed")
        if self._state is _WriterState.FAILED:
            return RuntimeError("stream: writer failed; only error is permitted")
        return RuntimeError(f"stream: unexpected state {self._state!r}")

    async def _mark_closed(self) -> None:
        async with self._lock:
            self._state = _WriterState.CLOSED
            self._last_err = None
            # A successful terminal consumed its seq; clear any prior
            # reservation defensively.
            self._reserved_valid = False

    async def _mark_failed(
        self, err: BaseException, *, reserved_seq: int | None = None
    ) -> None:
        async with self._lock:
            self._state = _WriterState.FAILED
            self._last_err = err
            if reserved_seq is not None:
                self._reserved_seq = reserved_seq
                self._reserved_valid = True
            else:
                self._reserved_valid = False

    def _next_seq_locked(self) -> int:
        s = self._seq
        self._seq += 1
        return s

    async def _publish(self, env: Envelope) -> None:
        data = self._codec.encode_envelope(env)
        await self._transport.publish(
            RawMessage(subject=self._req.reply_to, data=data)
        )


class Stream:
    """Async iterator over a streaming response.

    Frames are reordered by :py:attr:`Envelope.seq`; the iterator stops after
    yielding a frame with ``is_final = True``.

    Two timeouts coexist (mirroring the Go SDK):

    * ``idle_timeout`` — maximum gap between two frames; expiring raises
      :class:`ErrIdleTimeout`.
    * ``deadline`` — absolute wall-clock deadline for the whole stream
      (in :py:meth:`asyncio.AbstractEventLoop.time` units); expiring raises
      :class:`asyncio.TimeoutError`. The overall deadline always wins when
      both timers would fire — the iterator checks the deadline before and
      after each ``recv`` call.
    """

    def __init__(
        self,
        inbox: Inbox,
        codec: Codec,
        idle_timeout: float | None = None,
        deadline: float | None = None,
        max_pending_frames: int | None = None,
        max_sequence_gap: int | None = None,
    ) -> None:
        self._inbox = inbox
        self._codec = codec
        self._idle = idle_timeout
        self._deadline = deadline
        self._max_pending = (
            max_pending_frames
            if max_pending_frames is not None and max_pending_frames > 0
            else DEFAULT_MAX_PENDING_FRAMES
        )
        self._max_gap = (
            max_sequence_gap
            if max_sequence_gap is not None and max_sequence_gap > 0
            else DEFAULT_MAX_SEQUENCE_GAP
        )
        self._expected = 0
        self._pending: dict[int, Envelope] = {}
        self._exhausted = False

    def __aiter__(self) -> "Stream":
        return self

    async def __anext__(self) -> Envelope:
        if self._exhausted:
            raise StopAsyncIteration

        while True:
            ready = self._pending.pop(self._expected, None)
            if ready is not None:
                self._expected += 1
                if ready.is_final:
                    self._exhausted = True
                return ready

            wait = self._idle
            if self._deadline is not None:
                remaining = self._deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    self._exhausted = True
                    raise asyncio.TimeoutError("bus: stream overall timeout")
                wait = remaining if wait is None else min(wait, remaining)

            try:
                msg = await self._inbox.recv(timeout=wait)
            except asyncio.TimeoutError:
                self._exhausted = True
                if (
                    self._deadline is not None
                    and asyncio.get_running_loop().time() >= self._deadline
                ):
                    raise asyncio.TimeoutError(
                        "bus: stream overall timeout"
                    ) from None
                raise ErrIdleTimeout("bus: stream idle timeout") from None
            except RuntimeError:
                # inbox closed mid-iteration — clean exit.
                self._exhausted = True
                raise StopAsyncIteration from None

            env = self._codec.decode_envelope(msg.data)
            # Reject negative Seq outright. Unlike the Go SDK (uint64), Python
            # envelopes carry Seq as a plain int and would otherwise treat a
            # negative value as a "late" frame that silently gets dropped;
            # that's a protocol violation, not a routing hiccup.
            if env.seq < 0:
                self._exhausted = True
                raise BackpressureDropError(
                    f"bus: negative seq={env.seq}"
                )
            if env.seq < self._expected:
                continue  # late / duplicate frame
            if env.seq in self._pending:
                continue
            # Backpressure guards — see pkg/bus/stream.go for rationale.
            #   1. Seq jumps too far ahead of expected: bound pending growth
            #      when a low-Seq frame is permanently missing.
            #   2. Pending is at capacity and this frame is not the expected
            #      one: accepting it would exceed the buffer. If it *is* the
            #      expected Seq, we take it — flushing it drains the buffer.
            #
            # Gap check uses subtraction rather than ``expected + max_gap`` for
            # symmetry with the Go SDK, where the same expression avoids a
            # uint64 overflow when ``expected`` is near ``MaxUint64``. Python's
            # ints are arbitrary-precision so overflow is not a concern here,
            # but keeping the two implementations line-for-line identical makes
            # them easier to audit together.
            if env.seq - self._expected >= self._max_gap:
                self._exhausted = True
                raise BackpressureDropError(
                    f"bus: stream seq gap too large (seq={env.seq}, "
                    f"expected={self._expected}, gap={self._max_gap})"
                )
            if env.seq != self._expected and len(self._pending) >= self._max_pending:
                self._exhausted = True
                raise BackpressureDropError(
                    f"bus: stream pending buffer full "
                    f"(max={self._max_pending}, seq={env.seq})"
                )
            self._pending[env.seq] = env

    async def close(self) -> None:
        if self._exhausted:
            return
        self._exhausted = True
        try:
            await self._inbox.close()
        except Exception:
            pass


def _inherit_metadata(src: dict[str, Any] | None) -> dict[str, Any] | None:
    if src is None:
        return None
    dst = {k: v for k, v in src.items() if not k.startswith("acp.")}
    return dst if dst else None


def new_reply_shell(agent_id: str, req: Envelope, event_type: str) -> Envelope:
    """Pre-populate a response envelope with correlation metadata copied from req.

    Non-acp metadata keys are inherited so business context (e.g. dingtalk.*)
    flows back through cascading invocations without manual copying.
    """
    resp = Envelope.new(event_type)
    resp.from_ = agent_id
    resp.to = req.from_
    resp.session_id = req.session_id
    resp.conversation_id = req.conversation_id
    resp.tenant_id = req.tenant_id
    resp.user_id = req.user_id
    resp.channel = req.channel
    resp.trace_id = req.trace_id
    resp.span_id = req.span_id
    resp.traceparent = req.traceparent
    resp.correlation_id = req.event_id
    resp.metadata = _inherit_metadata(req.metadata)
    return resp
