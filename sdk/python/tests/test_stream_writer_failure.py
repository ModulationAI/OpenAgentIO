"""P0#3 regression suite for StreamWriter terminal-state handling.

Mirrors ``pkg/bus/stream_writer_failure_test.go``.

Before P0#3 the writer flipped ``_closed = True`` before encode+publish, so a
codec/transport failure left the writer wedged in ``closed`` with no terminal
frame on the wire — the peer had no way to distinguish "handler done" from
"handler exploded mid-terminate" and would only find out via idle timeout.
These tests exercise the state machine directly.
"""
from __future__ import annotations

import logging
from typing import Any

import pytest

from openagentio import (
    Bus,
    Envelope,
    MessageReceived,
    ResponseDelta,
    ResponseError,
    ResponseFinal,
    StreamWriter,
)
from openagentio.bus.stream import _WriterState
from openagentio.codec.json_codec import JSONCodec
from openagentio.transport.base import Capabilities, RawMessage


class _ScriptedTransport:
    """Transport whose publish() delegates to an override function.

    ``publish_fn`` receives ``(subject, data)`` and returns an optional
    exception. When set to raise, the publish fails. Successful publishes are
    accumulated in :attr:`published` for later assertions.
    """

    def __init__(self) -> None:
        self.published: list[RawMessage] = []
        self.publish_fn = None  # type: ignore[assignment]

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    def capabilities(self) -> Capabilities:
        return Capabilities()

    async def publish(self, msg: RawMessage) -> None:
        if self.publish_fn is not None:
            self.publish_fn(msg)
        self.published.append(
            RawMessage(subject=msg.subject, data=bytes(msg.data), headers=msg.headers)
        )

    async def subscribe(self, *a: Any, **kw: Any) -> Any:  # pragma: no cover - not used
        raise NotImplementedError

    async def request(self, *a: Any, **kw: Any) -> Any:  # pragma: no cover - not used
        raise NotImplementedError

    async def open_inbox(self, *a: Any, **kw: Any) -> Any:  # pragma: no cover - not used
        raise NotImplementedError


class _FailingPayloadCodec:
    """Codec wrapper that raises on encode_payload for a marker sentinel.

    Envelope encoding still works, so tests can distinguish "payload encode
    failed" from "envelope encode failed".
    """

    def __init__(self, inner: JSONCodec, trip: Any) -> None:
        self._inner = inner
        self._trip = trip

    def name(self) -> str:
        return self._inner.name()

    def encode_envelope(self, e: Envelope) -> bytes:
        return self._inner.encode_envelope(e)

    def decode_envelope(self, data: bytes) -> Envelope:
        return self._inner.decode_envelope(data)

    def encode_payload(self, v: Any) -> bytes:
        if v is self._trip:
            raise RuntimeError("codec: intentional payload encode failure")
        return self._inner.encode_payload(v)

    def decode_payload(self, raw: bytes, cls: type) -> Any:
        return self._inner.decode_payload(raw, cls)


def _make_writer(
    transport: _ScriptedTransport | None = None,
    codec: Any | None = None,
) -> tuple[StreamWriter, _ScriptedTransport]:
    tr = transport or _ScriptedTransport()
    cd = codec or JSONCodec()
    req = Envelope.new(MessageReceived)
    req.event_id = "corr-1"
    req.from_ = "caller"
    req.to = "stream-target"
    req.reply_to = "reply.inbox.1"
    w = StreamWriter(transport=tr, codec=cd, agent_id="test-agent", request=req)
    return w, tr


# --- Final: payload codec failure -------------------------------------------


async def test_stream_writer_final_payload_codec_failure_transitions_to_failed() -> None:
    """Final(...) with a payload the codec rejects must:

    * raise the codec error to the caller
    * NOT publish any frame (encode failed before publish)
    * leave the writer in FAILED (not CLOSED)
    * allow a subsequent error(...) to still land a terminal frame
    """
    sentinel = object()
    codec = _FailingPayloadCodec(JSONCodec(), trip=sentinel)
    w, tr = _make_writer(codec=codec)

    with pytest.raises(RuntimeError, match="intentional payload encode failure"):
        await w.final(sentinel)

    assert w.state is _WriterState.FAILED
    assert tr.published == []

    # Recovery: caller may still emit an error frame.
    await w.error(RuntimeError("handler observed codec failure"))
    assert w.state is _WriterState.CLOSED
    assert len(tr.published) == 1
    assert ResponseError.encode() in tr.published[0].data


# --- Final: transport publish failure ---------------------------------------


async def test_stream_writer_final_publish_failure_returns_error_and_allows_recovery() -> None:
    tr = _ScriptedTransport()
    call_count = {"n": 0}

    def fail_first(_msg: RawMessage) -> None:
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise ConnectionError("transport: intentional failure")

    tr.publish_fn = fail_first  # type: ignore[assignment]
    w, _ = _make_writer(transport=tr)

    with pytest.raises(ConnectionError):
        await w.final(None)
    assert w.state is _WriterState.FAILED

    # Recovery: a follow-up error must succeed and reach the wire.
    await w.error(RuntimeError("handler surfaces failure"))
    assert w.state is _WriterState.CLOSED
    assert len(tr.published) == 1
    assert ResponseError.encode() in tr.published[0].data


# --- Error: publish failure is surfaced -------------------------------------


async def test_stream_writer_error_publish_failure_returns_error() -> None:
    tr = _ScriptedTransport()

    def always_fail(_msg: RawMessage) -> None:
        raise ConnectionError("transport: intentional failure")

    tr.publish_fn = always_fail  # type: ignore[assignment]
    w, _ = _make_writer(transport=tr)

    with pytest.raises(ConnectionError):
        await w.error(RuntimeError("handler err"))
    assert w.state is _WriterState.FAILED


# --- Error: None argument is rejected ---------------------------------------


async def test_stream_writer_error_none_argument_rejected() -> None:
    w, _ = _make_writer()
    with pytest.raises(ValueError):
        await w.error(None)  # type: ignore[arg-type]
    # Rejected inputs must not perturb the state machine.
    assert w.state is _WriterState.OPEN


# --- Post-terminal rejection ------------------------------------------------


async def test_stream_writer_after_final_rejects_further_frames() -> None:
    w, _ = _make_writer()
    await w.final(None)
    with pytest.raises(RuntimeError, match="already closed"):
        await w.delta("nope")
    with pytest.raises(RuntimeError, match="already closed"):
        await w.final(None)
    with pytest.raises(RuntimeError, match="already closed"):
        await w.error(RuntimeError("x"))
    assert w.state is _WriterState.CLOSED


# --- bus._run_stream_handler: FAILED → synthesized Error frame -------------


async def test_bus_run_stream_handler_direct_failed_emits_fallback_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Direct exercise of _run_stream_handler with a scripted transport
    that fails on the first publish. Verifies the fallback path lands an
    error frame.
    """
    tr = _ScriptedTransport()
    call_count = {"n": 0}

    def fail_first(_msg: RawMessage) -> None:
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise ConnectionError("transport: first publish failure")

    tr.publish_fn = fail_first  # type: ignore[assignment]

    caplog.set_level(logging.ERROR, logger="openagentio")

    b = Bus(agent_id="test-agent", transport=tr)
    await b.connect()
    try:
        async def handler(_env: Envelope, w: StreamWriter) -> None:
            await w.final(None)

        # Skip subscribe (scripted transport doesn't implement it) — call
        # _run_stream_handler directly with a synthetic request.
        req = Envelope.new(MessageReceived)
        req.event_id = "corr-1"
        req.from_ = "caller"
        req.to = "target"
        req.reply_to = "reply.inbox.1"

        await b._run_stream_handler(req, handler)

        # One fallback error frame lands (first publish failed, second one
        # — the fallback — succeeds).
        assert len(tr.published) == 1
        assert ResponseError.encode() in tr.published[0].data
    finally:
        # Direct close bypasses subscribe teardown paths.
        await tr.close()


async def test_bus_run_stream_handler_fallback_error_also_fails_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Pathological case: handler final() fails AND the fallback error() also
    fails. _run_stream_handler must emit a structured log line with the
    request identifiers instead of silently swallowing.
    """
    tr = _ScriptedTransport()

    def always_fail(_msg: RawMessage) -> None:
        raise ConnectionError("transport: total outage")

    tr.publish_fn = always_fail  # type: ignore[assignment]

    caplog.set_level(logging.ERROR, logger="openagentio")

    b = Bus(agent_id="test-agent", transport=tr)
    await b.connect()
    try:
        async def handler(_env: Envelope, w: StreamWriter) -> None:
            await w.final(None)

        req = Envelope.new(MessageReceived)
        req.event_id = "corr-9"
        req.from_ = "caller"
        req.to = "target"
        req.reply_to = "reply.inbox.9"

        await b._run_stream_handler(req, handler)

        # No frame was published (both attempts failed).
        assert tr.published == []
        # Log must carry structured identifiers so operators can correlate.
        text = "\n".join(r.getMessage() for r in caplog.records)
        assert "correlation_id=corr-9" in text
        assert "reply_to=reply.inbox.9" in text
        assert "publish_err" in text
    finally:
        await tr.close()


# --- Seq reuse: fallback error after failed final must not open a hole ------


async def test_stream_writer_fallback_error_reuses_reserved_seq() -> None:
    """Regression: the fallback ``error(...)`` from FAILED must reuse the
    seq that the failed ``final()`` reserved.

    Prior to this fix, the failed final consumed (say) seq=2 and the fallback
    error consumed seq=3 — the client's reorder buffer accepted seq=3 into
    pending and idle-timed out waiting for seq=2 (which was reserved but
    never landed on the wire).
    """
    tr = _ScriptedTransport()
    attempts: list[int] = []
    codec = JSONCodec()

    def script(msg: RawMessage) -> None:
        # Decode the envelope to record which seq each attempt carried.
        env = codec.decode_envelope(msg.data)
        attempts.append(env.seq)
        # Fail only on ResponseFinal; let Started/Delta/fallback-Error through.
        if env.event_type == ResponseFinal:
            raise ConnectionError("transport: intentional Final publish failure")

    tr.publish_fn = script  # type: ignore[assignment]
    w, _ = _make_writer(transport=tr)

    # Consume a couple of Deltas so the reserved seq is not 0 — that would
    # mask a bug where reserved_valid was ignored and the code silently
    # defaulted to 0.
    await w.delta("a")
    await w.delta("b")

    with pytest.raises(ConnectionError):
        await w.final(None)
    from openagentio.bus.stream import _WriterState as _WS

    assert w.state is _WS.FAILED

    await w.error(RuntimeError("fallback"))
    assert w.state is _WS.CLOSED

    # attempts: Delta seq=0, Delta seq=1, failed Final seq=2, fallback Error seq=2.
    assert attempts == [0, 1, 2, 2], attempts

    # Only successful publishes land in tr.published: 2 deltas + 1 fallback error.
    assert len(tr.published) == 3
    # And the terminal frame that actually reached the wire carries seq=2.
    last_env = codec.decode_envelope(tr.published[-1].data)
    assert last_env.seq == 2
    assert last_env.event_type == ResponseError
    assert last_env.is_final is True


async def test_stream_writer_failed_final_then_error_reuses_first_reserved_seq() -> None:
    """Even when the caller invokes ``final()`` twice (the second is rejected
    by the state machine), a later ``error(...)`` from FAILED must still use
    the first Final's reserved seq — the second Final rejection MUST NOT
    overwrite the reservation.
    """
    tr = _ScriptedTransport()

    def always_fail(_msg: RawMessage) -> None:
        raise ConnectionError("transport: total outage")

    tr.publish_fn = always_fail  # type: ignore[assignment]
    w, _ = _make_writer(transport=tr)

    with pytest.raises(ConnectionError):
        await w.final(None)
    # Second Final rejected by state machine; must not perturb reservation.
    with pytest.raises(RuntimeError):
        await w.final(None)

    # Flip publish back on and try an Error(); it must land at seq=0.
    tr.publish_fn = None  # type: ignore[assignment]
    await w.error(RuntimeError("fallback"))

    assert len(tr.published) == 1
    env = JSONCodec().decode_envelope(tr.published[0].data)
    assert env.seq == 0
    assert env.event_type == ResponseError


# --- End-to-end: through Bus + inmem, failed Final -> fallback Error --------


class _FailFinalReplyTransport:
    """Wraps an InMemoryDriver, fails the first Publish whose envelope has
    ``event_type == ResponseFinal``. Delegates everything else so Started/
    Delta reply frames, the invoke request itself, and the fallback Error
    frame all travel normally.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self._codec = JSONCodec()
        self.tripped = False
        self.fail_count = 0
        self.ok_reply_count = 0
        import threading

        self._lock = threading.Lock()

    async def connect(self) -> None:
        await self._inner.connect()

    async def close(self) -> None:
        await self._inner.close()

    def capabilities(self) -> Capabilities:
        return self._inner.capabilities()

    async def publish(self, msg: RawMessage) -> None:
        if msg.subject.startswith("_INBOX."):
            env = self._codec.decode_envelope(msg.data)
            if env.event_type == ResponseFinal:
                with self._lock:
                    if not self.tripped:
                        self.tripped = True
                        self.fail_count += 1
                        raise ConnectionError(
                            "transport: intentional Final publish failure"
                        )
            with self._lock:
                self.ok_reply_count += 1
        await self._inner.publish(msg)

    async def subscribe(self, *a: Any, **kw: Any) -> Any:
        return await self._inner.subscribe(*a, **kw)

    async def request(self, *a: Any, **kw: Any) -> Any:
        return await self._inner.request(*a, **kw)

    async def open_inbox(self, *a: Any, **kw: Any) -> Any:
        return await self._inner.open_inbox(*a, **kw)


async def test_stream_invoke_failed_final_fallback_error_reaches_client() -> None:
    """End-to-end regression: handler's Final publish fails, client must
    receive the fallback Error at the reserved seq (no idle timeout).
    """
    from openagentio import InMemoryDriver
    from openagentio.bus.options import WithIdleTimeout

    tr = _FailFinalReplyTransport(InMemoryDriver())
    b = Bus(agent_id="test-agent", transport=tr, default_timeout=2.0)
    await b.connect()
    try:
        async def handler(_env: Envelope, w: StreamWriter) -> None:
            await w.delta("a")
            await w.delta("b")
            # This Final publish will trip the transport; the runtime's
            # auto-terminal path observes FAILED and emits the fallback
            # Error at seq=2 (the reserved seq).
            await w.final(None)

        await b.handle_stream("failed-final", handler)

        stream = await b.stream_invoke(
            "failed-final",
            None,
            WithIdleTimeout(0.5),  # tight — regression would idle-timeout
        )
        frames: list[Envelope] = []
        async for env in stream:
            frames.append(env)

        # Must terminate cleanly.
        assert len(frames) >= 3, frames
        assert frames[-1].is_final
        assert frames[-1].event_type == ResponseError
        # Seq-reservation assertion: fallback landed at the reserved seq (2),
        # not at seq=3 (the next-fresh value).
        assert frames[-1].seq == 2, [f.seq for f in frames]
        # Deltas must have arrived contiguously first.
        for i, env in enumerate(frames[:-1]):
            assert env.event_type == ResponseDelta
            assert env.seq == i, [f.seq for f in frames]

        assert tr.tripped is True
        assert tr.fail_count == 1
        assert tr.ok_reply_count >= 3, tr.ok_reply_count
    finally:
        await b.close()
