"""Tests for openagentio.bridge.matrix_event — Matrix Event bridge."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest

from openagentio import Bus, Envelope, InMemoryDriver
from openagentio.bridge import (
    BUILTIN_FACTORIES,
    BridgeRunner,
    MatrixEventBridge,
    matrix_event_factory,
)
from openagentio.bridge.config import BridgeConfig, BridgeConfigError, BridgeDefinition, BridgeMappings
from openagentio.bus.errors import (
    AgentTimeoutError,
    AgentUnavailableError,
    AuthFailureError,
    InvalidRequestError,
    TransportFailureError,
)
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route


def _bridge_def(
    *,
    name: str = "matrix-main",
    homeserver_url: str = "https://matrix.example.com",
    access_token: str = "test-token",
    user_id: str = "@agent:example.com",
    room_ids: list[str] | None = None,
    mappings: BridgeMappings | None = None,
    **kwargs: object,
) -> BridgeDefinition:
    """Build a BridgeDefinition with sensible defaults."""
    config: dict[str, object] = {
        "homeserver_url": homeserver_url,
        "access_token": access_token,
        "user_id": user_id,
        "room_ids": room_ids if room_ids is not None else ["!room:example.com"],
    }
    config.update(kwargs)
    return BridgeDefinition(
        name=name, type="matrix_event", config=config, mappings=mappings or BridgeMappings()
    )


def _make_envelope(
    bridge: MatrixEventBridge,
    payload: dict[str, Any],
    *,
    session_id: str = "",
    metadata: dict[str, Any] | None = None,
) -> Envelope:
    """Build an outbound envelope for the configured bridge."""
    env = Envelope.new(bridge._outbound_message_event)
    env.session_id = session_id
    env.metadata = dict(metadata) if metadata else None
    env.payload = json.dumps(payload).encode("utf-8")
    return env


class MockMatrixHomeserver:
    """Minimal mock Matrix Client-Server API for outbound send and inbound sync tests."""

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.sync_requests: list[dict[str, Any]] = []
        self.status_code = 200
        self.error_body: dict[str, Any] | str = ""
        self.sync_status_code = 200
        self.sync_error_body: dict[str, Any] | str = ""
        self._sync_responses: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._sync_request_event = asyncio.Event()
        self.app = Starlette(
            routes=[
                Route(
                    "/_matrix/client/v3/rooms/{room_id}/send/m.room.message/{txn_id}",
                    self._send,
                    methods=["PUT"],
                ),
                Route(
                    "/_matrix/client/v3/sync",
                    self._sync,
                    methods=["GET"],
                ),
            ]
        )

    def set_error(self, status_code: int, body: dict[str, Any] | str = "") -> None:
        self.status_code = status_code
        self.error_body = body

    def set_sync_error(self, status_code: int, body: dict[str, Any] | str = "") -> None:
        self.sync_status_code = status_code
        self.sync_error_body = body

    def clear_sync_error(self) -> None:
        self.sync_status_code = 200
        self.sync_error_body = ""

    def push_sync(self, response: dict[str, Any]) -> None:
        """Queue a response for the next ``/sync`` request."""
        self._sync_responses.put_nowait(response)

    async def wait_for_sync_request(self) -> None:
        """Wait until a ``/sync`` request has been received, then reset."""
        await self._sync_request_event.wait()
        self._sync_request_event.clear()

    async def _send(self, request: Request) -> Response:
        body = await request.body()
        self.requests.append(
            {
                "method": request.method,
                "path": request.url.path,
                "room_id": request.path_params["room_id"],
                "txn_id": request.path_params["txn_id"],
                "headers": dict(request.headers),
                "body": json.loads(body) if body else None,
            }
        )

        auth = request.headers.get("authorization", "")
        if not auth.startswith("Bearer "):
            return JSONResponse(
                {"errcode": "M_MISSING_TOKEN", "error": "missing token"},
                status_code=401,
            )

        if self.status_code != 200:
            if isinstance(self.error_body, dict):
                return JSONResponse(self.error_body, status_code=self.status_code)
            return Response(self.error_body, status_code=self.status_code)

        return JSONResponse({"event_id": "$mock-event-id"})

    async def _sync(self, request: Request) -> Response:
        self.sync_requests.append({"params": dict(request.query_params)})
        self._sync_request_event.set()
        if self.sync_status_code != 200:
            if isinstance(self.sync_error_body, dict):
                return JSONResponse(self.sync_error_body, status_code=self.sync_status_code)
            return Response(self.sync_error_body, status_code=self.sync_status_code)
        response = await self._sync_responses.get()
        return JSONResponse(response)


async def _start_bridge(
    bus: Bus, defn: BridgeDefinition, mock_server: MockMatrixHomeserver
) -> MatrixEventBridge:
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=mock_server.app))
    bridge = MatrixEventBridge(bus, defn, client=client)
    await bridge.start()
    return bridge


class TestConfigValidation:
    def test_valid_minimal_config(self) -> None:
        defn = _bridge_def()
        bridge = MatrixEventBridge(Bus(agent_id="a", transport=InMemoryDriver()), defn)
        assert bridge._homeserver_url == "https://matrix.example.com"
        assert bridge._access_token == "test-token"
        assert bridge._user_id == "@agent:example.com"
        assert bridge._room_ids == {"!room:example.com"}
        assert bridge._sync_timeout == 30.0
        assert bridge._reconnect_delay == 2.0
        assert bridge._initial_sync_behavior == "skip"
        assert bridge._outbound_msgtype == "m.text"
        assert bridge._event_prefix == "matrix"
        assert bridge._inbound_message_event == "matrix.message.received"
        assert bridge._outbound_message_event == "matrix.message.send"
        assert bridge._session_strategy == "room"

    def test_homeserver_url_stripped_and_normalised(self) -> None:
        defn = _bridge_def(homeserver_url="  https://matrix.example.com/  ")
        bridge = MatrixEventBridge(Bus(agent_id="a", transport=InMemoryDriver()), defn)
        assert bridge._homeserver_url == "https://matrix.example.com"

    def test_invalid_homeserver_url(self) -> None:
        for value in ["ftp://matrix.example.com", "not-a-url", "   ", "http://"]:
            defn = _bridge_def(homeserver_url=value)
            with pytest.raises(BridgeConfigError, match="config 'homeserver_url'"):
                MatrixEventBridge(Bus(agent_id="a", transport=InMemoryDriver()), defn)

    def test_user_id_is_validated(self) -> None:
        for value in ["agent:example.com", "@agent", "   ", "not-a-user"]:
            defn = _bridge_def(user_id=value)
            with pytest.raises(BridgeConfigError, match="config 'user_id'"):
                MatrixEventBridge(Bus(agent_id="a", transport=InMemoryDriver()), defn)

    def test_room_ids_are_validated(self) -> None:
        for value in [["room:example.com"], ["!room"], ["   "], ["!room:example.com", "bad"]]:
            defn = _bridge_def(room_ids=value)
            with pytest.raises(BridgeConfigError, match="config 'room_ids"):
                MatrixEventBridge(Bus(agent_id="a", transport=InMemoryDriver()), defn)

    def test_whitespace_only_values_are_rejected(self) -> None:
        for key in ("homeserver_url", "access_token", "user_id"):
            defn = _bridge_def(**{key: "   "})
            with pytest.raises(BridgeConfigError, match=f"config '{key}'"):
                MatrixEventBridge(Bus(agent_id="a", transport=InMemoryDriver()), defn)

    def test_custom_config_and_mappings(self) -> None:
        defn = BridgeDefinition(
            name="mx",
            type="matrix_event",
            config={
                "homeserver_url": "https://hs.test",
                "access_token": "tok",
                "user_id": "@bot:hs.test",
                "room_ids": ["!a:hs.test", "!b:hs.test"],
                "sync_timeout": 60,
                "reconnect_delay": 5,
                "initial_sync_behavior": "replay",
                "outbound_msgtype": "m.notice",
            },
            mappings=BridgeMappings(
                extra={
                    "event_prefix": "mx",
                    "inbound_message_event": "mx.in",
                    "outbound_message_event": "mx.out",
                    "session_strategy": "room_sender",
                }
            ),
        )
        bridge = MatrixEventBridge(Bus(agent_id="a", transport=InMemoryDriver()), defn)
        assert bridge._sync_timeout == 60.0
        assert bridge._reconnect_delay == 5.0
        assert bridge._initial_sync_behavior == "replay"
        assert bridge._outbound_msgtype == "m.notice"
        assert bridge._event_prefix == "mx"
        assert bridge._inbound_message_event == "mx.in"
        assert bridge._outbound_message_event == "mx.out"
        assert bridge._session_strategy == "room_sender"

    def test_missing_homeserver_url(self) -> None:
        defn = _bridge_def(homeserver_url="")
        with pytest.raises(BridgeConfigError, match="config 'homeserver_url' is required"):
            MatrixEventBridge(Bus(agent_id="a", transport=InMemoryDriver()), defn)

    def test_missing_access_token(self) -> None:
        defn = _bridge_def(access_token="")
        with pytest.raises(BridgeConfigError, match="config 'access_token' is required"):
            MatrixEventBridge(Bus(agent_id="a", transport=InMemoryDriver()), defn)

    def test_missing_user_id(self) -> None:
        defn = _bridge_def(user_id="")
        with pytest.raises(BridgeConfigError, match="config 'user_id' is required"):
            MatrixEventBridge(Bus(agent_id="a", transport=InMemoryDriver()), defn)

    @pytest.mark.parametrize("room_ids", [[], {}, [""], ["!ok:example.com", ""]])
    def test_invalid_room_ids(self, room_ids: object) -> None:
        defn = _bridge_def(room_ids=room_ids)  # type: ignore[arg-type]
        with pytest.raises(BridgeConfigError, match="config 'room_ids"):
            MatrixEventBridge(Bus(agent_id="a", transport=InMemoryDriver()), defn)

    def test_missing_room_ids(self) -> None:
        defn = BridgeDefinition(
            name="mx",
            type="matrix_event",
            config={
                "homeserver_url": "https://hs.test",
                "access_token": "tok",
                "user_id": "@bot:hs.test",
            },
        )
        with pytest.raises(BridgeConfigError, match="config 'room_ids"):
            MatrixEventBridge(Bus(agent_id="a", transport=InMemoryDriver()), defn)

    @pytest.mark.parametrize("value", ["fast", -1, 0])
    def test_invalid_sync_timeout(self, value: object) -> None:
        defn = _bridge_def(sync_timeout=value)
        with pytest.raises(BridgeConfigError, match="config 'sync_timeout'"):
            MatrixEventBridge(Bus(agent_id="a", transport=InMemoryDriver()), defn)

    @pytest.mark.parametrize("value", ["soon", -1, 0])
    def test_invalid_reconnect_delay(self, value: object) -> None:
        defn = _bridge_def(reconnect_delay=value)
        with pytest.raises(BridgeConfigError, match="config 'reconnect_delay'"):
            MatrixEventBridge(Bus(agent_id="a", transport=InMemoryDriver()), defn)

    @pytest.mark.parametrize("value", ["ask", "", None])
    def test_invalid_initial_sync_behavior(self, value: object) -> None:
        defn = _bridge_def(initial_sync_behavior=value)
        with pytest.raises(BridgeConfigError, match="config 'initial_sync_behavior'"):
            MatrixEventBridge(Bus(agent_id="a", transport=InMemoryDriver()), defn)

    @pytest.mark.parametrize("value", ["m.image", "", None])
    def test_invalid_outbound_msgtype(self, value: object) -> None:
        defn = _bridge_def(outbound_msgtype=value)
        with pytest.raises(BridgeConfigError, match="config 'outbound_msgtype'"):
            MatrixEventBridge(Bus(agent_id="a", transport=InMemoryDriver()), defn)

    @pytest.mark.parametrize("value", ["user", "", None])
    def test_invalid_session_strategy(self, value: object) -> None:
        defn = BridgeDefinition(
            name="mx",
            type="matrix_event",
            config={
                "homeserver_url": "https://hs.test",
                "access_token": "tok",
                "user_id": "@bot:hs.test",
                "room_ids": ["!room:hs.test"],
            },
            mappings=BridgeMappings(extra={"session_strategy": value}),
        )
        with pytest.raises(BridgeConfigError, match="mapping 'session_strategy'"):
            MatrixEventBridge(Bus(agent_id="a", transport=InMemoryDriver()), defn)


class TestFactoryAndLifecycle:
    async def test_factory_is_registered(self, bus: Bus) -> None:
        assert "matrix_event" in BUILTIN_FACTORIES
        defn = _bridge_def()
        bridge = BUILTIN_FACTORIES["matrix_event"](bus, defn)
        assert isinstance(bridge, MatrixEventBridge)

    async def test_start_stop_is_idempotent(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = MatrixEventBridge(
            bus, _bridge_def(), client=httpx.AsyncClient(transport=httpx.ASGITransport(app=mock.app))
        )
        await bridge.start()
        assert bridge._client is not None
        assert len(bridge._subscriptions) == 1
        # Second start should be a no-op.
        await bridge.start()
        assert len(bridge._subscriptions) == 1
        await bridge.stop()
        assert bridge._client is None
        assert len(bridge._subscriptions) == 0
        # Second stop should be a no-op.
        await bridge.stop()

    async def test_start_stop_restarts_cleanly(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        client = httpx.AsyncClient(transport=httpx.ASGITransport(app=mock.app))
        bridge = MatrixEventBridge(bus, _bridge_def(), client=client)
        await bridge.start()
        first_sub = bridge._subscriptions[0]
        await bridge.stop()
        await bridge.start()
        assert len(bridge._subscriptions) == 1
        assert bridge._subscriptions[0] is not first_sub
        await bridge.stop()

    async def test_bridge_runner_lifecycle(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()

        def factory(b: Bus, defn: BridgeDefinition) -> MatrixEventBridge:
            return MatrixEventBridge(
                b, defn, client=httpx.AsyncClient(transport=httpx.ASGITransport(app=mock.app))
            )

        config = BridgeConfig.from_dict(
            {
                "version": "openagentio.bridge/v1",
                "bridges": [
                    {
                        "name": "matrix-main",
                        "type": "matrix_event",
                        "config": {
                            "homeserver_url": "https://matrix.example.com",
                            "access_token": "secret",
                            "user_id": "@agent:example.com",
                            "room_ids": ["!room:example.com"],
                        },
                    }
                ],
            }
        )
        factories = dict(BUILTIN_FACTORIES)
        factories["matrix_event"] = factory
        runner = BridgeRunner(bus, config, factories)
        await runner.start()
        assert len(runner.bridges) == 1
        name, bridge = runner.bridges[0]
        assert name == "matrix-main"
        assert isinstance(bridge, MatrixEventBridge)
        await runner.stop()


class TestOutboundSend:
    async def test_send_basic_text_message(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(), mock)
        try:
            env = _make_envelope(bridge, {"room_id": "!room:example.com", "text": "hello"})
            await bridge._on_outbound_event(env)

            assert len(mock.requests) == 1
            req = mock.requests[0]
            assert req["method"] == "PUT"
            assert req["room_id"] == "!room:example.com"
            assert req["body"]["msgtype"] == "m.text"
            assert req["body"]["body"] == "hello"
            assert req["headers"]["authorization"] == "Bearer test-token"
            assert req["txn_id"].startswith("openagentio-")
            assert req["txn_id"] in bridge._recent_txn_ids
        finally:
            await bridge.stop()

    async def test_send_m_notice(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(outbound_msgtype="m.notice"), mock)
        try:
            env = _make_envelope(bridge, {"room_id": "!room:example.com", "text": "notice"})
            await bridge._on_outbound_event(env)
            assert mock.requests[0]["body"]["msgtype"] == "m.notice"
        finally:
            await bridge.stop()

    async def test_send_with_html(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(), mock)
        try:
            env = _make_envelope(
                bridge,
                {
                    "room_id": "!room:example.com",
                    "text": "hello",
                    "html": "<p>hello</p>",
                    "reply_to_event_id": "$orig",
                },
            )
            await bridge._on_outbound_event(env)
            body = mock.requests[0]["body"]
            assert body["format"] == "org.matrix.custom.html"
            assert body["formatted_body"] == "<p>hello</p>"
            assert body["m.relates_to"]["m.in_reply_to"]["event_id"] == "$orig"
        finally:
            await bridge.stop()

    async def test_room_id_from_metadata(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(), mock)
        try:
            env = _make_envelope(
                bridge,
                {"text": "hello"},
                metadata={"matrix.room_id": "!meta:example.com"},
            )
            await bridge._on_outbound_event(env)
            assert mock.requests[0]["room_id"] == "!meta:example.com"
        finally:
            await bridge.stop()

    async def test_room_id_from_session_room_strategy(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(), mock)
        try:
            env = _make_envelope(
                bridge, {"text": "hello"}, session_id="!session:example.com"
            )
            await bridge._on_outbound_event(env)
            assert mock.requests[0]["room_id"] == "!session:example.com"
        finally:
            await bridge.stop()

    async def test_room_id_from_session_room_sender_strategy(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(
            bus, _bridge_def(mappings=BridgeMappings(extra={"session_strategy": "room_sender"})), mock
        )
        try:
            env = _make_envelope(
                bridge,
                {"text": "hello"},
                session_id="!room:example.com:@alice:example.com",
            )
            await bridge._on_outbound_event(env)
            assert mock.requests[0]["room_id"] == "!room:example.com"
        finally:
            await bridge.stop()

    async def test_room_sender_unparseable_session(self, bus: Bus) -> None:
        bridge = MatrixEventBridge(
            bus,
            _bridge_def(mappings=BridgeMappings(extra={"session_strategy": "room_sender"})),
        )
        env = _make_envelope(bridge, {"text": "hello"}, session_id="!room:example.com")
        with pytest.raises(InvalidRequestError, match="cannot derive room_id"):
            await bridge._on_outbound_event(env)

    async def test_payload_room_id_has_priority(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(), mock)
        try:
            env = _make_envelope(
                bridge,
                {"room_id": "!payload:example.com", "text": "hello"},
                session_id="!session:example.com",
                metadata={"matrix.room_id": "!meta:example.com"},
            )
            await bridge._on_outbound_event(env)
            assert mock.requests[0]["room_id"] == "!payload:example.com"
        finally:
            await bridge.stop()

    async def test_missing_room_id_and_session(self, bus: Bus) -> None:
        bridge = MatrixEventBridge(bus, _bridge_def())
        env = _make_envelope(bridge, {"text": "hello"})
        with pytest.raises(InvalidRequestError, match="missing 'room_id'"):
            await bridge._on_outbound_event(env)

    async def test_invalid_room_id_in_payload(self, bus: Bus) -> None:
        bridge = MatrixEventBridge(bus, _bridge_def())
        env = _make_envelope(bridge, {"room_id": "not-a-room", "text": "hello"})
        with pytest.raises(InvalidRequestError, match="not a valid Matrix room ID"):
            await bridge._on_outbound_event(env)

    async def test_invalid_room_id_in_metadata(self, bus: Bus) -> None:
        bridge = MatrixEventBridge(bus, _bridge_def())
        env = _make_envelope(
            bridge,
            {"text": "hello"},
            metadata={"matrix.room_id": "bad"},
        )
        with pytest.raises(InvalidRequestError, match="not a valid Matrix room ID"):
            await bridge._on_outbound_event(env)

    async def test_invalid_room_id_in_session(self, bus: Bus) -> None:
        bridge = MatrixEventBridge(bus, _bridge_def())
        env = _make_envelope(bridge, {"text": "hello"}, session_id="bad-session")
        with pytest.raises(InvalidRequestError, match="not a valid Matrix room ID"):
            await bridge._on_outbound_event(env)

    @pytest.mark.parametrize("payload", [None, "text", [], {"room_id": "!r:example.com"}])
    async def test_invalid_payload(self, bus: Bus, payload: Any) -> None:
        bridge = MatrixEventBridge(bus, _bridge_def())
        env = _make_envelope(bridge, payload) if payload is not None else Envelope.new(bridge._outbound_message_event)
        if payload is None:
            env.payload = None
        with pytest.raises(InvalidRequestError):
            await bridge._on_outbound_event(env)

    async def test_401_maps_to_auth_failure(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        mock.set_error(401, {"errcode": "M_UNKNOWN_TOKEN", "error": "bad token"})
        bridge = await _start_bridge(bus, _bridge_def(), mock)
        try:
            env = _make_envelope(bridge, {"room_id": "!room:example.com", "text": "hello"})
            with pytest.raises(AuthFailureError):
                await bridge._on_outbound_event(env)
        finally:
            await bridge.stop()

    async def test_403_maps_to_auth_failure(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        mock.set_error(403, {"errcode": "M_FORBIDDEN", "error": "no permission"})
        bridge = await _start_bridge(bus, _bridge_def(), mock)
        try:
            env = _make_envelope(bridge, {"room_id": "!room:example.com", "text": "hello"})
            with pytest.raises(AuthFailureError):
                await bridge._on_outbound_event(env)
        finally:
            await bridge.stop()

    async def test_404_maps_to_invalid_request(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        mock.set_error(404, {"errcode": "M_NOT_FOUND", "error": "room not found"})
        bridge = await _start_bridge(bus, _bridge_def(), mock)
        try:
            env = _make_envelope(bridge, {"room_id": "!room:example.com", "text": "hello"})
            with pytest.raises(InvalidRequestError):
                await bridge._on_outbound_event(env)
        finally:
            await bridge.stop()

    async def test_429_maps_to_transport_failure_with_retry_after(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        mock.set_error(
            429,
            {"errcode": "M_LIMIT_EXCEEDED", "error": "too fast", "retry_after_ms": 2500},
        )
        bridge = await _start_bridge(bus, _bridge_def(), mock)
        try:
            env = _make_envelope(bridge, {"room_id": "!room:example.com", "text": "hello"})
            with pytest.raises(TransportFailureError, match="retry_after_ms=2500"):
                await bridge._on_outbound_event(env)
        finally:
            await bridge.stop()

    async def test_5xx_maps_to_transport_failure(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        mock.set_error(500, {"errcode": "M_UNKNOWN", "error": "server error"})
        bridge = await _start_bridge(bus, _bridge_def(), mock)
        try:
            env = _make_envelope(bridge, {"room_id": "!room:example.com", "text": "hello"})
            with pytest.raises(TransportFailureError):
                await bridge._on_outbound_event(env)
        finally:
            await bridge.stop()

    async def test_timeout_maps_to_agent_timeout(self, bus: Bus) -> None:
        class FailingTransport(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
                raise httpx.TimeoutException("too slow")

        client = httpx.AsyncClient(transport=FailingTransport())
        bridge = MatrixEventBridge(bus, _bridge_def(), client=client)
        await bridge.start()
        try:
            env = _make_envelope(bridge, {"room_id": "!room:example.com", "text": "hello"})
            with pytest.raises(AgentTimeoutError):
                await bridge._on_outbound_event(env)
        finally:
            await bridge.stop()

    async def test_network_error_maps_to_agent_unavailable(self, bus: Bus) -> None:
        class FailingTransport(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
                raise httpx.ConnectError("connection refused")

        client = httpx.AsyncClient(transport=FailingTransport())
        bridge = MatrixEventBridge(bus, _bridge_def(), client=client)
        await bridge.start()
        try:
            env = _make_envelope(bridge, {"room_id": "!room:example.com", "text": "hello"})
            with pytest.raises(AgentUnavailableError):
                await bridge._on_outbound_event(env)
        finally:
            await bridge.stop()

    async def test_bus_publish_triggers_send(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(), mock)
        try:
            await bus.publish(
                _make_envelope(bridge, {"room_id": "!room:example.com", "text": "via bus"})
            )
            assert len(mock.requests) == 1
            assert mock.requests[0]["body"]["body"] == "via bus"
        finally:
            await bridge.stop()


def _text_event(
    event_id: str = "$evt",
    sender: str = "@alice:example.com",
    body: str = "hello",
    txn_id: str | None = None,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "type": "m.room.message",
        "sender": sender,
        "event_id": event_id,
        "origin_server_ts": 1730000000000,
        "content": {"msgtype": "m.text", "body": body},
    }
    if txn_id is not None:
        event["unsigned"] = {"transaction_id": txn_id}
    return event


def _sync_response(
    events: list[dict[str, Any]],
    *,
    room_id: str = "!room:example.com",
    next_batch: str = "batch1",
) -> dict[str, Any]:
    return {
        "next_batch": next_batch,
        "rooms": {"join": {room_id: {"timeline": {"events": events}}}},
    }


class _Collector:
    """Helper to collect inbound envelopes in tests."""

    def __init__(self) -> None:
        self.events: list[Envelope] = []
        self._event = asyncio.Event()

    async def handler(self, env: Envelope) -> None:
        self.events.append(env)
        self._event.set()

    async def wait(self, timeout: float = 2.0) -> None:
        await asyncio.wait_for(self._event.wait(), timeout=timeout)

    def clear(self) -> None:
        self._event.clear()


async def _expect_inbound(
    bus: Bus,
    event_type: str,
    mock: MockMatrixHomeserver,
    response: dict[str, Any],
    timeout: float = 2.0,
) -> Envelope:
    """Subscribe, push a sync response, and return the first inbound envelope."""
    collector = _Collector()
    sub = await bus.subscribe(event_type, collector.handler)
    try:
        mock.push_sync(response)
        await collector.wait(timeout=timeout)
        return collector.events[0]
    finally:
        await sub.unsubscribe()


class TestInboundSync:
    async def test_inbound_text_message_publishes_bus_event(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(initial_sync_behavior="replay"), mock)
        try:
            await mock.wait_for_sync_request()
            env = await _expect_inbound(
                bus,
                bridge._inbound_message_event,
                mock,
                _sync_response([_text_event("$evt1", "@alice:example.com", "hello")]),
            )
            payload = env.payload_json()
            assert payload["text"] == "hello"
            assert payload["room_id"] == "!room:example.com"
            assert payload["sender"] == "@alice:example.com"
            assert payload["msgtype"] == "m.text"
            assert env.event_type == "matrix.message.received"
            assert env.session_id == "!room:example.com"
            assert env.conversation_id == "!room:example.com"
            assert env.correlation_id == "$evt1"
            assert env.metadata["matrix.event_id"] == "$evt1"
            assert env.metadata["matrix.room_id"] == "!room:example.com"
            assert env.metadata["matrix.sender"] == "@alice:example.com"
        finally:
            await bridge.stop()

    async def test_next_batch_used_on_second_sync(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(), mock)
        try:
            await mock.wait_for_sync_request()
            mock.push_sync(_sync_response([], next_batch="batch1"))
            await mock.wait_for_sync_request()
            assert len(mock.sync_requests) == 2
            assert mock.sync_requests[1]["params"].get("since") == "batch1"
            assert mock.sync_requests[1]["params"].get("timeout") == "30000"
            assert mock.sync_requests[1]["params"].get("set_presence") == "offline"
        finally:
            await bridge.stop()

    async def test_initial_sync_skip_drops_events(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(), mock)
        try:
            await mock.wait_for_sync_request()
            collector = _Collector()
            sub = await bus.subscribe(bridge._inbound_message_event, collector.handler)
            try:
                mock.push_sync(
                    _sync_response([_text_event("$skip", body="skip me")], next_batch="batch1")
                )
                await asyncio.sleep(0.05)
                assert len(collector.events) == 0

                await mock.wait_for_sync_request()
                mock.push_sync(
                    _sync_response([_text_event("$real", body="real event")], next_batch="batch2")
                )
                await collector.wait()
                assert len(collector.events) == 1
                assert collector.events[0].payload_json()["event_id"] == "$real"
                assert mock.sync_requests[1]["params"].get("since") == "batch1"
            finally:
                await sub.unsubscribe()
        finally:
            await bridge.stop()

    async def test_initial_sync_replay_publishes_events(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(initial_sync_behavior="replay"), mock)
        try:
            await mock.wait_for_sync_request()
            env = await _expect_inbound(
                bus,
                bridge._inbound_message_event,
                mock,
                _sync_response([_text_event("$replay", body="replay me")], next_batch="batch1"),
            )
            assert env.payload_json()["event_id"] == "$replay"
        finally:
            await bridge.stop()

    async def test_inbound_ignores_own_sender(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(initial_sync_behavior="replay"), mock)
        try:
            await mock.wait_for_sync_request()
            collector = _Collector()
            sub = await bus.subscribe(bridge._inbound_message_event, collector.handler)
            try:
                mock.push_sync(
                    _sync_response(
                        [_text_event("$own", sender="@agent:example.com", body="from me")],
                        next_batch="batch1",
                    )
                )
                await asyncio.sleep(0.05)
                assert len(collector.events) == 0
            finally:
                await sub.unsubscribe()
        finally:
            await bridge.stop()

    async def test_inbound_ignores_non_text_msgtype(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(initial_sync_behavior="replay"), mock)
        try:
            await mock.wait_for_sync_request()
            collector = _Collector()
            sub = await bus.subscribe(bridge._inbound_message_event, collector.handler)
            try:
                mock.push_sync(
                    _sync_response(
                        [
                            {
                                "type": "m.room.message",
                                "sender": "@alice:example.com",
                                "event_id": "$img",
                                "content": {"msgtype": "m.image", "body": "pic.png"},
                            }
                        ],
                        next_batch="batch1",
                    )
                )
                await asyncio.sleep(0.05)
                assert len(collector.events) == 0
            finally:
                await sub.unsubscribe()
        finally:
            await bridge.stop()

    async def test_inbound_ignores_unconfigured_room(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(initial_sync_behavior="replay"), mock)
        try:
            await mock.wait_for_sync_request()
            collector = _Collector()
            sub = await bus.subscribe(bridge._inbound_message_event, collector.handler)
            try:
                mock.push_sync(
                    _sync_response(
                        [_text_event("$other")],
                        room_id="!other:example.com",
                        next_batch="batch1",
                    )
                )
                await asyncio.sleep(0.05)
                assert len(collector.events) == 0
            finally:
                await sub.unsubscribe()
        finally:
            await bridge.stop()

    async def test_inbound_dedups_event_id(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(initial_sync_behavior="replay"), mock)
        try:
            await mock.wait_for_sync_request()
            collector = _Collector()
            sub = await bus.subscribe(bridge._inbound_message_event, collector.handler)
            try:
                mock.push_sync(
                    _sync_response([_text_event("$dup", body="first")], next_batch="batch1")
                )
                await collector.wait()
                assert len(collector.events) == 1

                collector.clear()
                await mock.wait_for_sync_request()
                mock.push_sync(
                    _sync_response([_text_event("$dup", body="duplicate")], next_batch="batch2")
                )
                await asyncio.sleep(0.05)
                assert len(collector.events) == 1
            finally:
                await sub.unsubscribe()
        finally:
            await bridge.stop()

    async def test_inbound_ignores_recent_txn_id(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(initial_sync_behavior="replay"), mock)
        try:
            await mock.wait_for_sync_request()
            # Trigger outbound send so the bridge records a transaction id.
            env = _make_envelope(bridge, {"room_id": "!room:example.com", "text": "hi"})
            await bridge._on_outbound_event(env)
            txn_id = mock.requests[0]["txn_id"]

            collector = _Collector()
            sub = await bus.subscribe(bridge._inbound_message_event, collector.handler)
            try:
                mock.push_sync(
                    _sync_response(
                        [_text_event("$echo", body="echo", txn_id=txn_id)],
                        next_batch="batch1",
                    )
                )
                await asyncio.sleep(0.05)
                assert len(collector.events) == 0
                assert txn_id in bridge._recent_txn_ids
            finally:
                await sub.unsubscribe()
        finally:
            await bridge.stop()

    async def test_inbound_room_sender_session_strategy(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(
            bus,
            _bridge_def(
                initial_sync_behavior="replay",
                mappings=BridgeMappings(extra={"session_strategy": "room_sender"}),
            ),
            mock,
        )
        try:
            await mock.wait_for_sync_request()
            env = await _expect_inbound(
                bus,
                bridge._inbound_message_event,
                mock,
                _sync_response([_text_event("$rs", sender="@alice:example.com")]),
            )
            assert env.session_id == "!room:example.com:@alice:example.com"
            assert env.conversation_id == "!room:example.com"
        finally:
            await bridge.stop()

    async def test_inbound_traceparent_from_content(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(initial_sync_behavior="replay"), mock)
        try:
            await mock.wait_for_sync_request()
            event = _text_event("$tp", body="traced")
            event["content"]["traceparent"] = "abc123-def456-01"
            env = await _expect_inbound(
                bus,
                bridge._inbound_message_event,
                mock,
                _sync_response([event]),
            )
            assert env.traceparent == "abc123-def456-01"
            assert env.trace_id == ""
            assert env.span_id == ""
            assert env.metadata["matrix.traceparent"] == "abc123-def456-01"
        finally:
            await bridge.stop()

    async def test_inbound_standard_traceparent_parses_trace_id_and_span_id(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(initial_sync_behavior="replay"), mock)
        try:
            await mock.wait_for_sync_request()
            event = _text_event("$tp", body="traced")
            event["content"]["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
            env = await _expect_inbound(
                bus,
                bridge._inbound_message_event,
                mock,
                _sync_response([event]),
            )
            assert env.traceparent == "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
            assert env.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
            assert env.span_id == "00f067aa0ba902b7"
            assert env.metadata["matrix.traceparent"] == "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        finally:
            await bridge.stop()

    async def test_inbound_traceparent_from_unsigned(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(initial_sync_behavior="replay"), mock)
        try:
            await mock.wait_for_sync_request()
            event = _text_event("$tp", body="traced")
            event["unsigned"] = {"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
            env = await _expect_inbound(
                bus,
                bridge._inbound_message_event,
                mock,
                _sync_response([event]),
            )
            assert env.traceparent == "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
            assert env.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
            assert env.span_id == "00f067aa0ba902b7"
            assert env.metadata["matrix.traceparent"] == "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        finally:
            await bridge.stop()

    async def test_inbound_traceparent_content_takes_priority(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(initial_sync_behavior="replay"), mock)
        try:
            await mock.wait_for_sync_request()
            event = _text_event("$tp", body="traced")
            event["content"]["traceparent"] = "content-tp"
            event["unsigned"] = {"traceparent": "unsigned-tp"}
            env = await _expect_inbound(
                bus,
                bridge._inbound_message_event,
                mock,
                _sync_response([event]),
            )
            assert env.traceparent == "content-tp"
        finally:
            await bridge.stop()

    async def test_sync_loop_survives_malformed_event(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(initial_sync_behavior="replay"), mock)
        try:
            await mock.wait_for_sync_request()
            collector = _Collector()
            sub = await bus.subscribe(bridge._inbound_message_event, collector.handler)
            try:
                # First response has one malformed event and one good event.
                mock.push_sync(
                    _sync_response(
                        [
                            {"type": "m.room.message", "content": {"msgtype": "m.text"}},
                            _text_event("$good", body="good"),
                        ],
                        next_batch="batch1",
                    )
                )
                await collector.wait()
                assert len(collector.events) == 1
                assert collector.events[0].payload_json()["event_id"] == "$good"
            finally:
                await sub.unsubscribe()
        finally:
            await bridge.stop()

    async def test_next_batch_not_advanced_when_publish_fails(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(
            bus,
            _bridge_def(initial_sync_behavior="replay", reconnect_delay=0.001),
            mock,
        )
        try:
            await mock.wait_for_sync_request()
            original_publish = bridge._bus.publish

            async def failing_publish(env: Envelope) -> None:
                raise RuntimeError("boom")

            bridge._bus.publish = failing_publish
            mock.push_sync(
                _sync_response([_text_event("$fail", body="fail")], next_batch="batch1")
            )
            await asyncio.sleep(0.05)
            # Cursor must stay unset so the same batch is re-fetched after retry.
            assert bridge._next_batch is None
        finally:
            bridge._bus.publish = original_publish
            await bridge.stop()


class TestLifecycleAndHealth:
    async def test_stop_cancels_sync_task(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(bus, _bridge_def(), mock)
        task = bridge._sync_task
        assert task is not None and not task.done()
        await bridge.stop()
        assert task.done()
        assert task.cancelled()
        assert bridge._sync_task is None
        assert bridge._client is None
        assert len(bridge._subscriptions) == 0

    async def test_sync_loop_survives_sync_error(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(
            bus, _bridge_def(initial_sync_behavior="replay", reconnect_delay=0.001), mock
        )
        try:
            await mock.wait_for_sync_request()
            mock.set_sync_error(500, {"errcode": "M_UNKNOWN", "error": "server error"})
            mock.push_sync({})
            await asyncio.sleep(0.05)

            await mock.wait_for_sync_request()
            mock.clear_sync_error()
            env = await _expect_inbound(
                bus,
                bridge._inbound_message_event,
                mock,
                _sync_response([_text_event("$recovered", body="recovered")], next_batch="batch1"),
            )
            assert env.payload_json()["event_id"] == "$recovered"
        finally:
            await bridge.stop()

    async def test_reconnect_preserves_next_batch(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(
            bus, _bridge_def(initial_sync_behavior="skip", reconnect_delay=0.001), mock
        )
        try:
            await mock.wait_for_sync_request()
            mock.push_sync(_sync_response([], next_batch="batch1"))
            await mock.wait_for_sync_request()
            assert bridge._next_batch == "batch1"

            mock.set_sync_error(500, "boom")
            mock.push_sync({})
            await asyncio.sleep(0.05)
            await mock.wait_for_sync_request()
            assert mock.sync_requests[-1]["params"].get("since") == "batch1"
            assert bridge._next_batch == "batch1"

            mock.clear_sync_error()
            await mock.wait_for_sync_request()
            pending_idx = len(mock.sync_requests) - 1
            mock.push_sync(_sync_response([], next_batch="batch2"))
            await asyncio.sleep(0.05)
            assert mock.sync_requests[pending_idx]["params"].get("since") == "batch1"
            assert bridge._next_batch == "batch2"
        finally:
            await bridge.stop()

    async def test_healthy_to_unhealthy_to_healthy(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        bridge = await _start_bridge(
            bus, _bridge_def(reconnect_delay=0.001), mock
        )
        try:
            assert bridge.is_healthy is True
            await mock.wait_for_sync_request()

            mock.set_sync_error(500, "boom")
            for _ in range(3):
                mock.push_sync({})
                await asyncio.sleep(0.02)

            assert bridge.is_healthy is False
            assert bridge.last_error is not None

            mock.clear_sync_error()
            mock.push_sync(_sync_response([], next_batch="batch1"))
            await asyncio.sleep(0.05)
            assert bridge.is_healthy is True
            assert bridge.last_error is None
        finally:
            await bridge.stop()

    async def test_rate_limit_uses_retry_after(self, bus: Bus) -> None:
        mock = MockMatrixHomeserver()
        # reconnect_delay is deliberately large to prove retry_after_ms wins.
        bridge = await _start_bridge(
            bus, _bridge_def(reconnect_delay=60.0), mock
        )
        try:
            await mock.wait_for_sync_request()
            mock.set_sync_error(
                429,
                {"errcode": "M_LIMIT_EXCEEDED", "error": "too fast", "retry_after_ms": 50},
            )
            mock.push_sync({})
            # Wait long enough for the 50ms retry_after but far less than 60s.
            await asyncio.sleep(0.15)
            assert len(mock.sync_requests) >= 2

            mock.clear_sync_error()
            mock.push_sync(_sync_response([], next_batch="batch1"))
            await asyncio.sleep(0.05)
            assert bridge.is_healthy is True
        finally:
            await bridge.stop()

    async def test_partial_start_cleanup(self, bus: Bus) -> None:
        original_subscribe = bus.subscribe

        async def failing_subscribe(*args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("subscribe failed")

        bus.subscribe = failing_subscribe  # type: ignore[method-assign]
        try:
            mock = MockMatrixHomeserver()
            bridge = MatrixEventBridge(bus, _bridge_def())
            with pytest.raises(RuntimeError, match="subscribe failed"):
                await bridge.start()
            assert bridge._client is None
            assert len(bridge._subscriptions) == 0
            assert bridge._sync_task is None
            assert bridge.is_healthy is False
        finally:
            bus.subscribe = original_subscribe  # type: ignore[method-assign]
