"""Application-facing bus over a Transport.

Async-first: every IO operation is a coroutine. Mirrors pkg/bus.

A Bus instance owns the subscriptions registered via :meth:`Bus.handle_invoke`
and :meth:`Bus.handle_stream` **and** :meth:`Bus.subscribe`; closing the bus
unsubscribes them all and closes the underlying transport.

Middleware chain (registered via ``WithMiddleware``) is applied in
subscribe/handle_invoke/handle_stream dispatch paths, mirroring the Go SDK.
"""
from __future__ import annotations

import asyncio
import contextvars
import logging
from typing import Any, Awaitable, Callable

from openagentio.bus.errors import BusError, error_code_for, is_retryable_for
from openagentio.bus.options import (
    Options,
    Option,
    _HandleOpts,
    _InvokeOpts,
    _SubOpts,
    collect_handle_opts,
    collect_invoke_opts,
    collect_sub_opts,
    HandleOption,
    InvokeOption,
    SubOption,
)
from openagentio.bus.stream import Stream, StreamWriter, new_reply_shell
from openagentio.bus.subjects import (
    DEFAULT_SUBJECT_PREFIX,
    event_subject,
    invoke_subject,
)
from openagentio.codec.json_codec import Codec, JSONCodec
from openagentio.event.envelope import Envelope
from openagentio.event.payload import ErrorPayload
from openagentio.event.types import (
    FrameTypeRequest,
    MessageReceived,
    ResponseError,
    ResponseFinal,
    frame_type_for_event_type,
    is_terminal,
)
from openagentio.middleware import Chain, Handler as MiddlewareHandler, Middleware
import openagentio.session as _session
from openagentio.transport.base import (
    RawMessage,
    Subscription as TransportSubscription,
    Transport,
)

# Handler signatures.
Handler = Callable[[Envelope], Awaitable[None]]
InvokeHandler = Callable[[Envelope], Awaitable[Any]]
StreamHandler = Callable[[Envelope, StreamWriter], Awaitable[None]]


class Bus:
    """Application-facing bus. Construct, ``await bus.connect()``, then publish/subscribe."""

    def __init__(
        self,
        *,
        agent_id: str,
        transport: Transport,
        tenant: str = "",
        subject_prefix: str = DEFAULT_SUBJECT_PREFIX,
        codec: Codec | None = None,
        logger: logging.Logger | None = None,
        default_timeout: float = 30.0,
    ) -> None:
        opts = Options(
            agent_id=agent_id,
            transport=transport,
            tenant=tenant,
            subject_prefix=subject_prefix,
            codec=codec,
            logger=logger,
            default_timeout=default_timeout,
        )
        self._init_from_opts(opts)

    @classmethod
    def new(cls, *options: Option) -> Bus:
        """Factory aligned with Go SDK's ``bus.New(WithAgentID(...), WithTransport(...))``."""
        opts = Options()
        for o in options:
            o(opts)
        bus = cls.__new__(cls)
        bus._init_from_opts(opts)
        return bus

    def _init_from_opts(self, opts: Options) -> None:
        if not opts.agent_id:
            raise ValueError("bus: agent_id is required")
        if opts.transport is None:
            raise ValueError("bus: transport is required")
        self._opts = opts
        self._agent_id = opts.agent_id
        self._tenant = opts.tenant
        self._prefix = opts.subject_prefix
        self._codec = opts.codec or JSONCodec()
        self._transport = opts.transport
        self._logger = opts.logger or logging.getLogger("openagentio")
        self._default_timeout = opts.default_timeout
        self._propagate_session_context = opts.propagate_session_context
        self._envelope_preparers = opts.envelope_preparers

        # Session context propagation is opt-in via WithSessionPropagation(True).
        # When enabled, the bus injects the inbound envelope before dispatching
        # handlers and copies session/conversation/trace fields into nested
        # invoke/stream requests. WithMiddleware(Trace()) remains supported as the
        # older explicit injection mechanism; the two can be combined safely.
        self._middleware: list[Middleware] = list(opts.middleware)

        self._owned: list[TransportSubscription] = []
        self._tasks: set[asyncio.Task] = set()
        self._closed = False
        self._lock = asyncio.Lock()

    # --- lifecycle -------------------------------------------------------

    async def connect(self) -> None:
        await self._transport.connect()

    async def close(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            owned = list(self._owned)
            self._owned.clear()
            tasks = list(self._tasks)

        for t in tasks:
            t.cancel()
        for s in owned:
            try:
                await s.unsubscribe()
            except Exception as e:  # noqa: BLE001
                self._logger.warning("bus: unsubscribe error: %s", e)
        await self._transport.close()

    @property
    def agent_id(self) -> str:
        return self._agent_id

    @property
    def transport(self) -> Transport:
        return self._transport

    # --- pub / sub -------------------------------------------------------

    async def publish(self, env: Envelope) -> None:
        if env is None:
            raise ValueError("bus: nil envelope")
        if not env.event_type:
            raise ValueError("bus: envelope missing event_type")
        self._prepare_envelope(env)
        subject = event_subject(self._prefix, env.event_type, self._resolve_tenant(env.tenant_id))
        data = self._codec.encode_envelope(env)
        await self._transport.publish(
            RawMessage(subject=subject, data=data, reply_to=env.reply_to)
        )

    async def subscribe(
        self,
        event_type: str,
        handler: Handler,
        *options: SubOption,
    ) -> TransportSubscription:
        if handler is None:
            raise ValueError("bus: nil handler")
        if not event_type:
            raise ValueError("bus: empty event_type")
        sub_opts = collect_sub_opts(list(options))
        subject = event_subject(self._prefix, event_type, self._tenant)

        # Wrap handler with middleware chain.
        wrapped = Chain(handler, *self._middleware)

        async def dispatch(msg: RawMessage) -> None:
            try:
                env = self._codec.decode_envelope(msg.data)
            except Exception as e:  # noqa: BLE001
                self._logger.warning("bus: decode error: %s", e)
                return
            token: contextvars.Token | None = None
            if self._propagate_session_context:
                token = _session.inject(env)
            try:
                await wrapped(env)
            except Exception as e:  # noqa: BLE001
                self._logger.warning("bus: handler error after middleware: %s", e)
            finally:
                if token is not None:
                    _session.reset(token)

        sub = await self._transport.subscribe(subject, sub_opts.queue, dispatch)
        self._track_owned(sub)
        return sub

    # --- invoke / reply --------------------------------------------------

    async def invoke(
        self,
        target: str,
        payload: Any = None,
        *options: InvokeOption,
    ) -> Envelope:
        if not target:
            raise ValueError("bus: empty invoke target")

        invoke_opts = collect_invoke_opts(list(options))
        eff_timeout = invoke_opts.timeout if invoke_opts.timeout is not None else self._default_timeout
        env = self._build_request_envelope(target, payload)
        self._prepare_envelope(env)

        inbox = await self._transport.open_inbox()
        try:
            env.reply_to = inbox.subject
            data = self._codec.encode_envelope(env)
            await self._transport.publish(
                RawMessage(
                    subject=invoke_subject(
                        self._prefix, target, self._resolve_tenant(env.tenant_id)
                    ),
                    data=data,
                )
            )
            recv_timeout = eff_timeout if eff_timeout > 0 else None
            msg = await inbox.recv(timeout=recv_timeout)
            return self._codec.decode_envelope(msg.data)
        finally:
            await inbox.close()

    async def handle_invoke(
        self,
        target: str,
        handler: InvokeHandler,
        *options: HandleOption,
    ) -> TransportSubscription:
        """Register an invoke handler. Returns the Subscription so callers
        that own a finer-grained lifecycle than the Bus (e.g. bridges that
        start/stop independently of the Bus) can ``unsubscribe()`` it
        early. Callers that ignore the return value are unaffected — the
        Bus still tracks the subscription and tears it down on ``close()``.
        """
        if not target:
            raise ValueError("bus: empty invoke target")
        if handler is None:
            raise ValueError("bus: nil invoke handler")
        handle_opts = collect_handle_opts(list(options))
        queue = handle_opts.queue if handle_opts.queue_set else target
        subject = invoke_subject(self._prefix, target, self._tenant)

        async def invoke_dispatch(msg: RawMessage) -> None:
            try:
                req = self._codec.decode_envelope(msg.data)
            except Exception as e:  # noqa: BLE001
                self._logger.warning("bus: decode error: %s", e)
                return
            await self._handle_one(req, handler)

        sub = await self._transport.subscribe(subject, queue, invoke_dispatch)
        self._track_owned(sub)
        return sub

    async def _handle_one(self, req: Envelope, handler: InvokeHandler) -> None:
        result: Any = None

        # Adapter calls the InvokeHandler and captures result.
        # Exceptions propagate through the middleware chain naturally so
        # middleware like Retry can intercept and retry them.
        async def invoke_handler_adapter(env: Envelope) -> None:
            nonlocal result
            result = await handler(env)

        wrapped = Chain(invoke_handler_adapter, *self._middleware)
        user_err: BaseException | None = None
        token: contextvars.Token | None = None
        if self._propagate_session_context:
            token = _session.inject(req)
        try:
            await wrapped(req)
        except BaseException as e:  # noqa: BLE001
            user_err = e
        finally:
            if token is not None:
                _session.reset(token)

        if not req.reply_to:
            if user_err is not None:
                self._logger.warning(
                    "bus: invoke handler error (no reply_to): %s", user_err
                )
            return

        if user_err is not None:
            resp = self._error_response(req, user_err)
        elif isinstance(result, Envelope):
            resp = self._adopt_response(req, result)
        else:
            resp = self._final_response(req, result)

        try:
            data = self._codec.encode_envelope(resp)
            await self._transport.publish(
                RawMessage(subject=req.reply_to, data=data)
            )
        except Exception as e:  # noqa: BLE001
            self._logger.warning("bus: reply publish failed: %s", e)

    # --- stream invoke ---------------------------------------------------

    async def stream_invoke(
        self,
        target: str,
        payload: Any = None,
        *options: InvokeOption,
    ) -> Stream:
        if not target:
            raise ValueError("bus: empty invoke target")

        invoke_opts = collect_invoke_opts(list(options))
        eff_timeout = invoke_opts.timeout if invoke_opts.timeout is not None else self._default_timeout
        idle_timeout = invoke_opts.idle_timeout

        deadline: float | None
        if eff_timeout > 0:
            deadline = asyncio.get_running_loop().time() + eff_timeout
        else:
            deadline = None

        env = self._build_request_envelope(target, payload)
        self._prepare_envelope(env)
        inbox = await self._transport.open_inbox()
        env.reply_to = inbox.subject
        try:
            data = self._codec.encode_envelope(env)
            await self._transport.publish(
                RawMessage(
                    subject=invoke_subject(
                        self._prefix, target, self._resolve_tenant(env.tenant_id)
                    ),
                    data=data,
                )
            )
        except Exception:
            await inbox.close()
            raise

        return Stream(
            inbox=inbox,
            codec=self._codec,
            idle_timeout=idle_timeout,
            deadline=deadline,
            max_pending_frames=invoke_opts.max_pending_frames,
            max_sequence_gap=invoke_opts.max_sequence_gap,
        )

    async def handle_stream(
        self,
        target: str,
        handler: StreamHandler,
        *options: HandleOption,
    ) -> TransportSubscription:
        """Register a stream handler. Returns the Subscription so callers
        that own a finer-grained lifecycle than the Bus (e.g. bridges that
        start/stop independently of the Bus) can ``unsubscribe()`` it
        early. Callers that ignore the return value are unaffected — the
        Bus still tracks the subscription and tears it down on ``close()``.
        """
        if not target:
            raise ValueError("bus: empty invoke target")
        if handler is None:
            raise ValueError("bus: nil stream handler")
        handle_opts = collect_handle_opts(list(options))
        queue = handle_opts.queue if handle_opts.queue_set else target
        subject = invoke_subject(self._prefix, target, self._tenant)

        async def stream_dispatch(msg: RawMessage) -> None:
            try:
                req = self._codec.decode_envelope(msg.data)
            except Exception as e:  # noqa: BLE001
                self._logger.warning("bus: decode error: %s", e)
                return
            if not req.reply_to:
                self._logger.warning("bus: stream request missing reply_to")
                return
            task = asyncio.create_task(self._run_stream_handler(req, handler))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

        sub = await self._transport.subscribe(subject, queue, stream_dispatch)
        self._track_owned(sub)
        return sub

    async def _run_stream_handler(
        self, req: Envelope, handler: StreamHandler
    ) -> None:
        writer = StreamWriter(self._transport, self._codec, self._agent_id, req)

        # Adapter calls the StreamHandler. Exceptions propagate through
        # middleware chain so Retry etc. can intercept them.
        async def stream_handler_adapter(env: Envelope) -> None:
            await handler(env, writer)

        wrapped = Chain(stream_handler_adapter, *self._middleware)
        herr: BaseException | None = None
        token: contextvars.Token | None = None
        if self._propagate_session_context:
            token = _session.inject(req)
        try:
            await wrapped(req)
        except BaseException as e:  # noqa: BLE001
            herr = e
        finally:
            if token is not None:
                _session.reset(token)

        # Auto-terminal logic. Three cases:
        #   * OPEN: handler returned without terminating — synth Final(None)
        #     (or Error(herr) if the handler raised).
        #   * FAILED: handler attempted Final/Error but publish or codec
        #     failed. The peer is still waiting on a terminal frame; try one
        #     more error frame (may or may not succeed).
        #   * CLOSED / CLOSING: nothing to do (CLOSING should never persist
        #     past a completed terminal call, but treat it defensively).
        from openagentio.bus.stream import _WriterState

        state = writer.state
        if state is _WriterState.CLOSED:
            return

        correlation_id = req.event_id
        target = req.to or req.event_type

        if state is _WriterState.FAILED:
            fallback = herr or writer.last_error or RuntimeError(
                "stream: terminal publish failed"
            )
            try:
                await writer.error(fallback)
            except BaseException as e:  # noqa: BLE001
                self._logger.error(
                    "bus: stream fallback error publish failed "
                    "target=%s correlation_id=%s reply_to=%s handler_err=%s "
                    "publish_err=%s",
                    target,
                    correlation_id,
                    req.reply_to,
                    herr,
                    e,
                )
            return

        # state is OPEN (or defensively CLOSING/unknown — treat as needing a
        # terminal frame). Any exception from writer.final/error here is real:
        # log with request identifiers instead of swallowing it silently.
        try:
            if herr is not None:
                await writer.error(herr)
            else:
                await writer.final(None)
        except BaseException as e:  # noqa: BLE001
            self._logger.error(
                "bus: stream auto-terminal failed "
                "target=%s correlation_id=%s reply_to=%s handler_err=%s "
                "publish_err=%s",
                target,
                correlation_id,
                req.reply_to,
                herr,
                e,
            )

    # --- helpers ---------------------------------------------------------

    def _track_owned(self, sub: TransportSubscription) -> None:
        self._owned.append(sub)

    def _resolve_tenant(self, envelope_tenant: str) -> str:
        return envelope_tenant or self._tenant

    def _prepare_envelope(self, env: Envelope) -> None:
        """Run all registered EnvelopePreparers on an outbound envelope."""
        for preparer in self._envelope_preparers:
            preparer(env)

    def _build_request_envelope(self, target: str, payload: Any) -> Envelope:
        if isinstance(payload, Envelope):
            env = payload.clone()
            if not env.from_:
                env.from_ = self._agent_id
            if not env.to:
                env.to = target
            if not env.tenant_id:
                env.tenant_id = self._tenant
            if not env.frame_type and env.event_type == MessageReceived:
                env.frame_type = FrameTypeRequest
            if self._propagate_session_context:
                self._inherit_session_context(env)
            return env

        env = Envelope.new(MessageReceived)
        env.frame_type = FrameTypeRequest
        env.from_ = self._agent_id
        env.to = target
        env.tenant_id = self._tenant
        if payload is not None:
            env.payload = self._codec.encode_payload(payload)
        if self._propagate_session_context:
            self._inherit_session_context(env)
        return env

    def _inherit_session_context(self, env: Envelope) -> None:
        """Copy trace/session fields from the currently dispatched envelope.

        When a handler triggered by an inbound event calls ``invoke`` or
        ``stream_invoke``, the new request should continue the same session,
        conversation, and trace context unless the caller explicitly supplied
        them. We intentionally do NOT inherit ``correlation_id`` or ``user_id``
        here because those carry request-specific or business semantics that may
        not belong on a nested invocation.
        """
        current = _session.current()
        if current is None:
            return
        if not env.session_id:
            env.session_id = current.session_id
        if not env.conversation_id:
            env.conversation_id = current.conversation_id
        if not env.trace_id:
            env.trace_id = current.trace_id
        if not env.span_id:
            env.span_id = current.span_id
        if not env.traceparent:
            env.traceparent = current.traceparent

    def _final_response(self, req: Envelope, payload: Any) -> Envelope:
        resp = new_reply_shell(self._agent_id, req, ResponseFinal)
        resp.is_final = True
        if payload is not None:
            resp.payload = self._codec.encode_payload(payload)
        return resp

    def _error_response(self, req: Envelope, exc: BaseException) -> Envelope:
        resp = new_reply_shell(self._agent_id, req, ResponseError)
        resp.is_final = True
        code = error_code_for(exc)
        retryable = is_retryable_for(exc)
        payload = ErrorPayload(code=code, message=str(exc), retryable=retryable)
        resp.payload = self._codec.encode_payload(payload)
        return resp

    def _adopt_response(self, req: Envelope, user: Envelope) -> Envelope:
        from openagentio.bus.stream import _inherit_metadata

        resp = user.clone()
        if not resp.from_:
            resp.from_ = self._agent_id
        if not resp.to:
            resp.to = req.from_
        if not resp.correlation_id:
            resp.correlation_id = req.event_id
        if not resp.session_id:
            resp.session_id = req.session_id
        if not resp.conversation_id:
            resp.conversation_id = req.conversation_id
        if not resp.tenant_id:
            resp.tenant_id = req.tenant_id
        if not resp.trace_id:
            resp.trace_id = req.trace_id
        if not resp.traceparent:
            resp.traceparent = req.traceparent
        if not resp.is_final and is_terminal(resp.event_type):
            resp.is_final = True
        # For known protocol event types, frame_type is canonical and must not
        # contradict event_type. Derive it from event_type whenever a mapping exists.
        ft = frame_type_for_event_type(resp.event_type)
        if ft:
            resp.frame_type = ft
        if resp.metadata is None:
            resp.metadata = _inherit_metadata(req.metadata)
        return resp