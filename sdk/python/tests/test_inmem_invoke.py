"""Invoke / reply round-trips over the in-memory driver."""
from __future__ import annotations

import asyncio

import pytest

from openagentio import (
    Bus,
    CodeAgentTimeout,
    CodeAgentUnavailable,
    Envelope,
    ResponseError,
    ResponseFinal,
    WithTimeout,
)


async def test_invoke_round_trip(bus: Bus) -> None:
    async def handler(req: Envelope) -> dict:
        body = req.payload_json() or {}
        return {"echo": body}

    await bus.handle_invoke("echo", handler)
    resp = await bus.invoke("echo", {"msg": "ping"})
    assert resp.event_type == ResponseFinal
    assert resp.is_final is True
    assert resp.correlation_id  # set by the runtime
    assert resp.payload_json() == {"echo": {"msg": "ping"}}


async def test_invoke_handler_returning_envelope_is_adopted(bus: Bus) -> None:
    async def handler(req: Envelope) -> Envelope:
        out = Envelope.new("custom.reply")
        out.payload = b'{"ok":true}'
        return out

    await bus.handle_invoke("custom", handler)
    resp = await bus.invoke("custom", None)
    assert resp.event_type == "custom.reply"
    assert resp.correlation_id  # adopted from request
    assert resp.payload_json() == {"ok": True}


async def test_invoke_handler_error_maps_to_error_envelope(bus: Bus) -> None:
    async def handler(_: Envelope) -> None:
        raise RuntimeError("kaboom")

    await bus.handle_invoke("boom", handler)
    resp = await bus.invoke("boom", None)
    assert resp.event_type == ResponseError
    assert resp.is_final is True
    err = resp.payload_json()
    assert err["code"] == CodeAgentUnavailable
    assert err["message"] == "kaboom"


async def test_invoke_passes_through_envelope_payload(bus: Bus) -> None:
    """When payload is itself an Envelope, the bus uses it as the request."""
    seen_event_id: dict[str, str] = {}

    async def handler(req: Envelope) -> dict:
        seen_event_id["id"] = req.event_id
        return {"got": req.event_type}

    await bus.handle_invoke("passthru", handler)

    custom_req = Envelope.new("user.custom")
    custom_req.payload = b'{"raw":1}'
    resp = await bus.invoke("passthru", custom_req)

    assert seen_event_id["id"] == custom_req.event_id
    assert resp.correlation_id == custom_req.event_id
    assert resp.payload_json() == {"got": "user.custom"}


async def test_invoke_timeout_error_maps_to_agent_timeout(bus: Bus) -> None:
    """Handler raising AgentTimeoutError → ResponseError with code=AGENT_TIMEOUT, retryable=True."""
    from openagentio import AgentTimeoutError, CodeAgentTimeout

    async def handler(_: Envelope) -> None:
        raise AgentTimeoutError("deadline exceeded")

    await bus.handle_invoke("timeout-err", handler)
    resp = await bus.invoke("timeout-err", None)
    assert resp.event_type == ResponseError
    err = resp.payload_json()
    assert err["code"] == CodeAgentTimeout
    assert err["retryable"] is True


async def test_invoke_handler_metadata_merges_with_request(bus: Bus) -> None:
    async def handler(req: Envelope) -> Envelope:
        out = Envelope.new(ResponseFinal)
        out.metadata = {"handler_key": "handler_value"}
        return out

    await bus.handle_invoke("merge", handler)

    req = Envelope.new_request()
    req.metadata = {
        "request_key": "request_value",
        "shared_key": "request_value",
        "acp.internal": "must_be_filtered",
    }
    resp = await bus.invoke("merge", req)

    assert resp.metadata["request_key"] == "request_value"
    assert resp.metadata["handler_key"] == "handler_value"
    assert resp.metadata["shared_key"] == "request_value"
    assert "acp.internal" not in resp.metadata


async def test_invoke_handler_metadata_overrides_request(bus: Bus) -> None:
    async def handler(req: Envelope) -> Envelope:
        out = Envelope.new(ResponseFinal)
        out.metadata = {"shared_key": "handler_value", "acp.handler": "filtered"}
        return out

    await bus.handle_invoke("override", handler)

    req = Envelope.new_request()
    req.metadata = {"shared_key": "request_value"}
    resp = await bus.invoke("override", req)

    assert resp.metadata["shared_key"] == "handler_value"
    assert "acp.handler" not in resp.metadata


async def test_invoke_handler_empty_metadata_inherits_request(bus: Bus) -> None:
    async def handler(req: Envelope) -> Envelope:
        out = Envelope.new(ResponseFinal)
        out.metadata = {}
        return out

    await bus.handle_invoke("empty", handler)

    req = Envelope.new_request()
    req.metadata = {"request_key": "request_value"}
    resp = await bus.invoke("empty", req)

    assert resp.metadata is not None
    assert resp.metadata["request_key"] == "request_value"


async def test_invoke_handler_metadata_does_not_mutate_inputs(bus: Bus) -> None:
    handler_out: Envelope | None = None

    async def handler(req: Envelope) -> Envelope:
        nonlocal handler_out
        out = Envelope.new(ResponseFinal)
        out.metadata = {"handler_key": "handler_value"}
        handler_out = out
        return out

    await bus.handle_invoke("no-mutate", handler)

    req = Envelope.new_request()
    req.metadata = {"request_key": "request_value"}
    resp = await bus.invoke("no-mutate", req)

    # Mutate the merged response metadata and ensure the original input maps are untouched.
    resp.metadata["new_key"] = "new_value"
    resp.metadata.pop("request_key", None)
    resp.metadata["handler_key"] = "mutated"

    assert "new_key" not in req.metadata
    assert req.metadata.get("request_key") == "request_value"
    assert handler_out is not None
    assert "new_key" not in handler_out.metadata
    assert handler_out.metadata.get("handler_key") == "handler_value"
