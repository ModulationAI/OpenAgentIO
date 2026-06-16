"""Session/trace context propagation through the bus."""
from __future__ import annotations

import asyncio

from openagentio import (
    Bus,
    Envelope,
    InMemoryDriver,
    ResponseFinal,
    Trace,
    WithAgentID,
    WithMiddleware,
    WithSessionPropagation,
    WithTransport,
    session,
)


def test_inject_returns_envelope_via_current() -> None:
    env = Envelope.new("test.smoke")
    env.trace_id = "trace-smoke"
    env.session_id = "sess-smoke"
    env.conversation_id = "conv-smoke"
    env.tenant_id = "tenant-smoke"

    assert session.current() is None

    token = session.inject(env)
    try:
        assert session.current() is env
        assert session.trace_id() == "trace-smoke"
        assert session.session_id() == "sess-smoke"
        assert session.conversation_id() == "conv-smoke"
        assert session.tenant_id() == "tenant-smoke"
    finally:
        session.reset(token)

    assert session.current() is None


def test_helpers_return_none_when_fields_blank() -> None:
    env = Envelope.new("test.blank")
    token = session.inject(env)
    try:
        assert session.trace_id() is None
        assert session.session_id() is None
        assert session.conversation_id() is None
        assert session.tenant_id() is None
    finally:
        session.reset(token)


async def _bus_with_trace() -> Bus:
    """Bus with Trace middleware so session helpers work inside handlers."""
    b = Bus.new(WithAgentID("test-agent"), WithTransport(InMemoryDriver()), WithMiddleware(Trace()))
    await b.connect()
    return b


async def test_handler_can_read_session_helpers() -> None:
    """Identity fields set on the request envelope should be readable via the
    session helpers from inside the handler."""
    bus = await _bus_with_trace()
    try:
        async def handler(_: Envelope) -> dict:
            return {
                "trace_id": session.trace_id(),
                "session_id": session.session_id(),
                "conversation_id": session.conversation_id(),
            }

        await bus.handle_invoke("ctx-echo", handler)

        req = Envelope.new("test.ctx")
        req.trace_id = "trace-A"
        req.session_id = "sess-A"
        req.conversation_id = "conv-A"

        resp = await bus.invoke("ctx-echo", req)
        assert resp.event_type == ResponseFinal
        assert resp.payload_json() == {
            "trace_id": "trace-A",
            "session_id": "sess-A",
            "conversation_id": "conv-A",
        }
    finally:
        await bus.close()


async def test_concurrent_invokes_dont_bleed_session() -> None:
    """Two concurrent invokes with distinct trace ids must each see only
    their own context, even when handlers interleave via ``asyncio.sleep(0)``."""
    bus = await _bus_with_trace()
    try:
        async def handler(_: Envelope) -> dict:
            before = session.trace_id()
            # Force interleaving with the other handler's coroutine.
            await asyncio.sleep(0)
            after = session.trace_id()
            return {"before": before, "after": after}

        await bus.handle_invoke("isolated", handler)

        req_a = Envelope.new("test.iso")
        req_a.trace_id = "trace-A"
        req_b = Envelope.new("test.iso")
        req_b.trace_id = "trace-B"

        resp_a, resp_b = await asyncio.gather(
            bus.invoke("isolated", req_a),
            bus.invoke("isolated", req_b),
        )

        body_a = resp_a.payload_json()
        body_b = resp_b.payload_json()
        assert body_a == {"before": "trace-A", "after": "trace-A"}
        assert body_b == {"before": "trace-B", "after": "trace-B"}
    finally:
        await bus.close()


async def test_session_clears_after_handler_returns() -> None:
    """Once the handler returns, a fresh task must observe an empty session."""
    bus = await _bus_with_trace()
    try:
        async def handler(_: Envelope) -> dict:
            return {"saw": session.trace_id()}

        await bus.handle_invoke("clear", handler)

        req = Envelope.new("test.clear")
        req.trace_id = "trace-C"
        resp = await bus.invoke("clear", req)
        assert resp.payload_json() == {"saw": "trace-C"}

        # A fresh asyncio.Task starts with no inherited binding from this scope.
        async def probe() -> object:
            return session.current()

        assert await asyncio.create_task(probe()) is None
        assert session.current() is None
    finally:
        await bus.close()


async def test_invoke_inherits_session_from_subscribe_handler() -> None:
    """A handler triggered by bus.publish() can call bus.invoke() and the
    request envelope continues the published event's session context when
    session propagation is enabled.
    """
    bus = Bus.new(
        WithAgentID("test-agent"),
        WithTransport(InMemoryDriver()),
        WithSessionPropagation(True),
    )
    await bus.connect()
    try:
        async def inner_handler(_: Envelope) -> dict:
            return {
                "trace_id": session.trace_id(),
                "session_id": session.session_id(),
                "conversation_id": session.conversation_id(),
                "traceparent": session.current().traceparent if session.current() else None,
            }

        await bus.handle_invoke("svc", inner_handler)

        seen: list[dict] = []

        async def subscriber(_: Envelope) -> None:
            resp = await bus.invoke("svc", {"text": "hello"})
            seen.append(resp.payload_json())

        sub = await bus.subscribe("matrix.message.received", subscriber)
        try:
            event = Envelope.new("matrix.message.received")
            event.trace_id = "trace-matrix"
            event.session_id = "sess-matrix"
            event.conversation_id = "conv-matrix"
            event.traceparent = "00-abc-def-01"
            await bus.publish(event)
            await asyncio.sleep(0.05)

            assert len(seen) == 1
            assert seen[0] == {
                "trace_id": "trace-matrix",
                "session_id": "sess-matrix",
                "conversation_id": "conv-matrix",
                "traceparent": "00-abc-def-01",
            }
        finally:
            await sub.unsubscribe()
    finally:
        await bus.close()


async def test_nested_invoke_inherits_session() -> None:
    """An invoke handler that calls bus.invoke() propagates session context
    when session propagation is enabled.
    """
    bus = Bus.new(
        WithAgentID("test-agent"),
        WithTransport(InMemoryDriver()),
        WithMiddleware(Trace()),
        WithSessionPropagation(True),
    )
    await bus.connect()
    try:
        async def inner_handler(_: Envelope) -> dict:
            env = session.current()
            return {
                "trace_id": env.trace_id if env else None,
                "session_id": env.session_id if env else None,
                "conversation_id": env.conversation_id if env else None,
            }

        await bus.handle_invoke("inner", inner_handler)

        async def outer_handler(_: Envelope) -> dict:
            return (await bus.invoke("inner", {"text": "hello"})).payload_json()

        await bus.handle_invoke("outer", outer_handler)

        req = Envelope.new("test.outer")
        req.trace_id = "trace-outer"
        req.session_id = "sess-outer"
        req.conversation_id = "conv-outer"
        resp = await bus.invoke("outer", req)
        assert resp.payload_json() == {
            "trace_id": "trace-outer",
            "session_id": "sess-outer",
            "conversation_id": "conv-outer",
        }
    finally:
        await bus.close()


async def test_session_propagation_disabled_by_default() -> None:
    """By default, the bus does not inject session context or propagate it to
    nested invoke calls. Trace() middleware remains the explicit opt-in way to
    get session.current() inside handlers.
    """
    bus = Bus.new(
        WithAgentID("test-agent"),
        WithTransport(InMemoryDriver()),
    )
    await bus.connect()
    try:
        outer_context: list[Envelope | None] = []

        async def inner_handler(_: Envelope) -> dict:
            env = session.current()
            return {
                "trace_id": env.trace_id if env else None,
                "session_id": env.session_id if env else None,
            }

        await bus.handle_invoke("inner", inner_handler)

        async def outer_handler(_: Envelope) -> dict:
            outer_context.append(session.current())
            return (await bus.invoke("inner", {"text": "hello"})).payload_json()

        await bus.handle_invoke("outer", outer_handler)

        req = Envelope.new("test.outer")
        req.trace_id = "trace-outer"
        req.session_id = "sess-outer"
        resp = await bus.invoke("outer", req)
        assert resp.payload_json() == {
            "trace_id": None,
            "session_id": None,
        }
        assert outer_context == [None]
    finally:
        await bus.close()
