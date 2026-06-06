"""HTTP/SSE adapter tests mirroring Go's adapter/http test suite."""
from __future__ import annotations

import asyncio
import json
import logging

import pytest

from openagentio import (
    Bus,
    InMemoryDriver,
    WithAgentID,
    WithTransport,
    WithTenant,
    WithMiddleware,
)
from openagentio.adapter.http.adapter import New
from openagentio.adapter.http.auth import AuthContext, AuthFunc, BearerAuth, ErrUnauthorized
from openagentio.adapter.http.errors import status_for_code, status_for_bus_error
from openagentio.adapter.http.middleware import Recover, Logging
from openagentio.adapter.http.options import WithAuth, WithTimeout, WithIdleTimeout, WithLogger, WithSSERetry
from openagentio.bus.stream import ErrIdleTimeout
from openagentio.event.envelope import Envelope
from openagentio.event.payload import (
    CodeAgentTimeout,
    CodeAgentUnavailable,
    CodeAuthFailure,
    CodeBackpressureDrop,
    CodeCodecFailure,
    CodeInvalidRequest,
    CodeNoHandler,
    CodeTransportFailure,
    ErrorPayload,
)
from openagentio.event.types import ResponseError, ResponseFinal

from starlette.requests import Request
from starlette.testclient import TestClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _make_bus(**bus_opts) -> Bus:
    opts = [WithAgentID("test-agent"), WithTransport(InMemoryDriver())]
    for k, v in bus_opts.items():
        if k == "tenant":
            opts.append(WithTenant(v))
    bus = Bus.new(*opts)
    await bus.connect()
    return bus


def _client(bus: Bus, *adapter_opts) -> TestClient:
    adapter = New(bus, *adapter_opts)
    return TestClient(adapter.app, raise_server_exceptions=False)


def _parse_sse(text: str) -> list[dict]:
    """Parse SSE text into list of {event, id, retry, data} dicts."""
    frames: list[dict] = []
    cur: dict = {}
    for line in text.split("\n"):
        if line == "":
            if cur:
                frames.append(cur)
            cur = {}
            continue
        if line.startswith("event: "):
            cur["event"] = line[7:]
        elif line.startswith("id: "):
            cur["id"] = line[4:]
        elif line.startswith("retry: "):
            cur["retry"] = line[7:]
        elif line.startswith("data: "):
            cur["data"] = line[6:]
    if cur:
        frames.append(cur)
    return frames


# ---------------------------------------------------------------------------
# Tests — status_for_code (Go parity: errors.go mapping)
# ---------------------------------------------------------------------------


class TestStatusForCode:
    def test_all_known_codes(self):
        assert status_for_code(CodeAuthFailure) == 401
        assert status_for_code(CodeInvalidRequest) == 400
        assert status_for_code(CodeNoHandler) == 404
        assert status_for_code(CodeAgentTimeout) == 504
        assert status_for_code(CodeAgentUnavailable) == 502
        assert status_for_code(CodeTransportFailure) == 502
        assert status_for_code(CodeBackpressureDrop) == 429
        assert status_for_code(CodeCodecFailure) == 500

    def test_unknown_code_defaults_500(self):
        assert status_for_code("SOMETHING_UNKNOWN") == 500

    def test_status_for_bus_error_timeout(self):
        status, code = status_for_bus_error(asyncio.TimeoutError())
        assert status == 504
        assert code == CodeAgentTimeout

    def test_status_for_bus_error_idle_timeout(self):
        status, code = status_for_bus_error(ErrIdleTimeout("idle"))
        assert status == 504
        assert code == CodeAgentTimeout

    def test_status_for_bus_error_cancelled(self):
        status, code = status_for_bus_error(asyncio.CancelledError())
        assert status == 499
        assert code == CodeInvalidRequest

    def test_status_for_bus_error_generic(self):
        status, code = status_for_bus_error(RuntimeError("boom"))
        assert status == 502
        assert code == CodeAgentUnavailable


# ---------------------------------------------------------------------------
# Tests — Invoke (Go parity: adapter_test.go)
# ---------------------------------------------------------------------------


class TestInvoke:
    @pytest.mark.asyncio
    async def test_invoke_happy_path(self):
        bus = await _make_bus()

        async def echo(env: Envelope):
            return env.payload_json()

        await bus.handle_invoke("echo", echo)
        with _client(bus) as c:
            resp = c.post("/v1/agents/echo/invoke", json={"msg": "hello"})
        await bus.close()
        assert resp.status_code == 200
        assert resp.json() == {"msg": "hello"}

    @pytest.mark.asyncio
    async def test_invoke_handler_error_maps_to_502(self):
        bus = await _make_bus()

        async def fail(env: Envelope):
            raise RuntimeError("kaboom")

        await bus.handle_invoke("fail", fail)
        with _client(bus) as c:
            resp = c.post("/v1/agents/fail/invoke", json={})
        await bus.close()
        assert resp.status_code == 502
        body = resp.json()
        assert body["code"] == CodeAgentUnavailable
        assert "kaboom" in body["message"]

    @pytest.mark.asyncio
    async def test_invoke_maps_headers_to_envelope(self):
        """X-* headers flow into envelope. Bus created with WithTenant
        matching X-Tenant-Id since tenant drives subject routing."""
        bus = await _make_bus(tenant="t1")
        seen: dict = {}

        async def capture(env: Envelope):
            seen["tenant_id"] = env.tenant_id
            seen["session_id"] = env.session_id
            seen["conversation_id"] = env.conversation_id
            seen["user_id"] = env.user_id
            seen["channel"] = env.channel
            seen["trace_id"] = env.trace_id
            return {}

        await bus.handle_invoke("capture", capture)
        with _client(bus) as c:
            resp = c.post(
                "/v1/agents/capture/invoke",
                json={},
                headers={
                    "X-Tenant-Id": "t1",
                    "X-Session-Id": "s1",
                    "X-Conversation-Id": "c1",
                    "X-User-Id": "u1",
                    "X-Channel": "ch1",
                    "X-Trace-Id": "tr1",
                },
            )
        await bus.close()
        assert resp.status_code == 200
        assert seen["tenant_id"] == "t1"
        assert seen["session_id"] == "s1"
        assert seen["conversation_id"] == "c1"
        assert seen["user_id"] == "u1"
        assert seen["channel"] == "ch1"
        assert seen["trace_id"] == "tr1"

    @pytest.mark.asyncio
    async def test_invoke_empty_payload_204(self):
        bus = await _make_bus()

        async def noop(env: Envelope):
            return None

        await bus.handle_invoke("noop", noop)
        with _client(bus) as c:
            resp = c.post("/v1/agents/noop/invoke", json={"x": 1})
        await bus.close()
        assert resp.status_code == 204


# ---------------------------------------------------------------------------
# Tests — Stream (Go parity: adapter_test.go)
# ---------------------------------------------------------------------------


class TestStream:
    @pytest.mark.asyncio
    async def test_stream_happy_path(self):
        """SSE frames: verify event types, id lines, seq, is_final."""
        bus = await _make_bus()

        async def stream_echo(env: Envelope, writer):
            await writer.started()
            await writer.delta({"chunk": "hi"})
            await writer.final({"done": True})

        await bus.handle_stream("echo", stream_echo)
        with _client(bus) as c:
            resp = c.post("/v1/agents/echo/stream", json={})
        await bus.close()
        assert resp.status_code == 200
        assert "text/event-stream" in resp.headers.get("content-type", "")

        frames = _parse_sse(resp.text)
        assert len(frames) == 3

        # Frame 0: started — has event, id, valid envelope JSON.
        assert frames[0]["event"] == "agent.response.started"
        assert "id" in frames[0]
        assert frames[0]["retry"] == "3000"
        env0 = json.loads(frames[0]["data"])
        assert env0["event_type"] == "agent.response.started"
        # seq omitted when 0 per Envelope.to_dict() omitempty semantics.
        assert env0.get("seq", 0) == 0

        # Frame 1: delta — seq=1.
        assert frames[1]["event"] == "agent.response.delta"
        assert frames[1]["retry"] == "3000"
        env1 = json.loads(frames[1]["data"])
        assert env1["seq"] == 1

        # Frame 2: final — seq=2, is_final=True.
        assert frames[2]["event"] == "agent.response.final"
        assert frames[2]["retry"] == "3000"
        env2 = json.loads(frames[2]["data"])
        assert env2["seq"] == 2
        assert env2["is_final"] == True

    @pytest.mark.asyncio
    async def test_stream_sse_retry_option(self):
        """WithSSERetry controls retry: milliseconds; zero disables it."""
        bus = await _make_bus()

        async def stream_once(env: Envelope, writer):
            await writer.final({"done": True})

        await bus.handle_stream("once", stream_once)
        with _client(bus, WithSSERetry(1.25)) as c:
            resp = c.post("/v1/agents/once/stream", json={})
        frames = _parse_sse(resp.text)
        assert resp.status_code == 200
        assert frames[0]["retry"] == "1250"

        with _client(bus, WithSSERetry(0)) as c:
            resp = c.post("/v1/agents/once/stream", json={})
        await bus.close()
        frames = _parse_sse(resp.text)
        assert resp.status_code == 200
        assert "retry" not in frames[0]

    @pytest.mark.asyncio
    async def test_stream_idle_timeout_emits_error_frame(self):
        """Idle timeout → SSE error frame with AGENT_TIMEOUT code."""
        bus = await _make_bus()

        async def slow(env: Envelope, writer):
            await writer.started()
            await asyncio.sleep(10)
            await writer.final({})

        await bus.handle_stream("slow", slow)
        with _client(bus, WithIdleTimeout(0.1)) as c:
            resp = c.post("/v1/agents/slow/stream", json={})
        await bus.close()
        assert resp.status_code == 200

        frames = _parse_sse(resp.text)
        assert len(frames) >= 2
        last = frames[-1]
        assert last["event"] == "agent.response.error"
        assert last["retry"] == "3000"
        env = json.loads(last["data"])
        assert env["is_final"] == True
        # Payload is embedded as structured value per Envelope.to_dict().
        payload = env["payload"]
        assert payload["code"] == CodeAgentTimeout


# ---------------------------------------------------------------------------
# Tests — Publish (Go parity: adapter_test.go)
# ---------------------------------------------------------------------------


class TestPublish:
    @pytest.mark.asyncio
    async def test_publish_returns_202(self):
        bus = await _make_bus()
        with _client(bus) as c:
            resp = c.post("/v1/events/order.created", json={"order": 1})
        await bus.close()
        assert resp.status_code == 202


# ---------------------------------------------------------------------------
# Tests — Auth (Go parity: adapter_test.go)
# ---------------------------------------------------------------------------


class TestAuth:
    @pytest.mark.asyncio
    async def test_auth_rejects_401(self):
        bus = await _make_bus()

        async def reject(request):
            raise ErrUnauthorized("bad token")

        with _client(bus, WithAuth(reject)) as c:
            resp = c.post("/v1/agents/x/invoke", json={})
        await bus.close()
        assert resp.status_code == 401
        assert resp.json()["code"] == CodeAuthFailure

    @pytest.mark.asyncio
    async def test_auth_overrides_headers(self):
        """AuthContext overrides non-routing fields (UserID, SessionID, Channel).
        Avoids tenant subject routing mismatch — matching Go test."""
        bus = await _make_bus()
        seen: dict = {}

        async def capture(env: Envelope):
            seen["user_id"] = env.user_id
            seen["session_id"] = env.session_id
            seen["channel"] = env.channel
            return {}

        await bus.handle_invoke("capture", capture)

        async def auth_fn(request):
            return AuthContext(user_id="auth-user", session_id="auth-sess", channel="auth-chan")

        with _client(bus, WithAuth(auth_fn)) as c:
            resp = c.post(
                "/v1/agents/capture/invoke",
                json={},
                headers={
                    "X-User-Id": "header-user",
                    "X-Session-Id": "header-sess",
                    "X-Channel": "header-chan",
                },
            )
        await bus.close()
        assert resp.status_code == 200
        assert seen["user_id"] == "auth-user"
        assert seen["session_id"] == "auth-sess"
        assert seen["channel"] == "auth-chan"

    @pytest.mark.asyncio
    async def test_auth_tenant_drives_subject_routing(self):
        """AuthContext.TenantID overrides X-Tenant-Id header and drives
        subject routing. Bus must be configured with the same tenant."""
        bus = await _make_bus(tenant="auth-tenant")
        seen: dict = {}

        async def capture(env: Envelope):
            seen["tenant_id"] = env.tenant_id
            return {}

        await bus.handle_invoke("scoped", capture)

        async def auth_fn(request):
            return AuthContext(tenant_id="auth-tenant")

        with _client(bus, WithAuth(auth_fn)) as c:
            # X-Tenant-Id is spoofed — auth override wins.
            resp = c.post(
                "/v1/agents/scoped/invoke",
                json={},
                headers={"X-Tenant-Id": "spoof-tenant"},
            )
        await bus.close()
        assert resp.status_code == 200
        assert seen["tenant_id"] == "auth-tenant"


# ---------------------------------------------------------------------------
# Tests — BearerAuth edge cases (Go parity: TestBearerAuthHelper)
# ---------------------------------------------------------------------------


class TestBearerAuth:
    @pytest.mark.asyncio
    async def test_bearer_auth_helper(self):
        """Good token, bad token, missing header."""
        bus = await _make_bus()

        async def noop(env: Envelope):
            return {}

        await bus.handle_invoke("x", noop)

        async def validator(token: str):
            if token == "secret":
                return AuthContext(user_id="u1")
            raise ErrUnauthorized("invalid token")

        auth = BearerAuth(validator)
        with _client(bus, WithAuth(auth)) as c:
            # Good token
            assert c.post("/v1/agents/x/invoke", json={}, headers={"Authorization": "Bearer secret"}).status_code == 200
            # Bad token
            assert c.post("/v1/agents/x/invoke", json={}, headers={"Authorization": "Bearer wrong"}).status_code == 401
            # Missing header
            assert c.post("/v1/agents/x/invoke", json={}).status_code == 401
        await bus.close()

    def test_bearer_auth_lowercase_scheme(self):
        """'bearer secret' (lowercase scheme) is accepted."""
        async def validator(token: str):
            if token == "secret":
                return AuthContext(tenant_id="ok")
            raise ErrUnauthorized("bad token")

        auth = BearerAuth(validator)
        from starlette.testclient import TestClient
        from starlette.applications import Starlette
        from starlette.routing import Route
        from starlette.responses import JSONResponse

        async def handler(request: Request):
            try:
                ac = await auth(request)
            except Exception as e:
                return JSONResponse(status_code=401, content={"error": str(e)})
            return JSONResponse(content={"tenant": ac.tenant_id})

        app = Starlette(routes=[Route("/test", handler, methods=["POST"])])
        with TestClient(app) as c:
            resp = c.post("/test", json={}, headers={"Authorization": "bearer secret"})
            assert resp.status_code == 200
            assert resp.json()["tenant"] == "ok"

    def test_bearer_auth_wrong_scheme(self):
        """'Basic ...' scheme → ErrUnauthorized."""
        async def validator(token: str):
            return AuthContext()

        auth = BearerAuth(validator)
        from starlette.responses import JSONResponse
        from starlette.applications import Starlette
        from starlette.routing import Route

        async def handler(request: Request):
            try:
                await auth(request)
            except Exception as e:
                return JSONResponse(status_code=401, content={"error": str(e)})
            return JSONResponse(content={"ok": True})

        app = Starlette(routes=[Route("/test", handler, methods=["POST"])])
        with TestClient(app) as c:
            resp = c.post("/test", json={}, headers={"Authorization": "Basic c2VjcmV0"})
            assert resp.status_code == 401

    def test_bearer_auth_empty_token(self):
        """'Bearer ' (empty token) → ErrUnauthorized."""
        async def validator(token: str):
            return AuthContext()

        auth = BearerAuth(validator)
        from starlette.responses import JSONResponse
        from starlette.applications import Starlette
        from starlette.routing import Route

        async def handler(request: Request):
            try:
                await auth(request)
            except Exception as e:
                return JSONResponse(status_code=401, content={"error": str(e)})
            return JSONResponse(content={"ok": True})

        from starlette.applications import Starlette
        from starlette.routing import Route
        app = Starlette(routes=[Route("/test", handler, methods=["POST"])])
        with TestClient(app) as c:
            resp = c.post("/test", json={}, headers={"Authorization": "Bearer "})
            assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Tests — Misc (Go parity: adapter_test.go)
# ---------------------------------------------------------------------------


class TestMisc:
    @pytest.mark.asyncio
    async def test_unknown_route_is_404(self):
        bus = await _make_bus()
        with _client(bus) as c:
            resp = c.post("/v1/nope", json={})
        await bus.close()
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_invalid_json_body_is_400(self):
        bus = await _make_bus()

        async def noop(env: Envelope):
            return {}

        await bus.handle_invoke("x", noop)
        with _client(bus) as c:
            resp = c.post(
                "/v1/agents/x/invoke",
                content=b"not json",
                headers={"Content-Type": "application/json"},
            )
        await bus.close()
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_body_over_4mb_is_400(self):
        """Body exceeding 4 MiB → 400 INVALID_REQUEST."""
        bus = await _make_bus()

        async def noop(env: Envelope):
            return {}

        await bus.handle_invoke("x", noop)
        big_body = b"x" * (5 * 1024 * 1024)
        with _client(bus) as c:
            resp = c.post(
                "/v1/agents/x/invoke",
                content=big_body,
                headers={"Content-Type": "application/json"},
            )
        await bus.close()
        assert resp.status_code == 400
        assert "4 MiB" in resp.json()["message"]
