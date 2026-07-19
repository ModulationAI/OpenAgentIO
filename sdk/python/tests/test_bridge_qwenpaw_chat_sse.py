"""Tests for openagentio.bridge.qwenpaw_chat_sse - QwenPaw Chat SSE bridge.

Covers the request mapping (plan §3), cumulative-text SSE dedup (plan §4),
HTTP/SSE error mapping, lifecycle, BridgeRunner registration, and the
``QwenPawChatBridge`` convenience wrapper. No real QwenPaw service is
required: a :class:`MockQwenPawServer` is mounted via
:class:`httpx.ASGITransport`.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from openagentio import Envelope, MessageReceived
from openagentio.bridge import (
    BUILTIN_FACTORIES,
    QwenPawChatBridge,
    QwenPawChatSSEBridge,
    qwenpaw_chat_sse_factory,
)
from openagentio.bridge.config import BridgeConfig, BridgeDefinition, BridgeMappings
from openagentio.bridge.runner import BridgeRunner
from openagentio.event.types import ResponseDelta, ResponseError, ResponseFinal
from mock_qwenpaw_http import MockQwenPawServer, assistant_event, text_delta_event


def _bridge_def(
    *,
    name: str = "qwenpaw.chat",
    base_url: str = "http://qwenpaw.test",
    token: str = "",
    agent_id: str = "default",
    user_id: str = "openagentio-user",
    channel: str = "console",
    request_timeout: float = 5.0,
    metadata_prefix: str = "qwenpaw.",
) -> BridgeDefinition:
    return BridgeDefinition(
        name=name,
        type="qwenpaw_chat_sse",
        config={
            "base_url": base_url,
            "token": token,
            "agent_id": agent_id,
            "user_id": user_id,
            "channel": channel,
            "request_timeout": request_timeout,
        },
        mappings=BridgeMappings(
            text_field="text",
            session_field="session_id",
            metadata_prefix=metadata_prefix,
        ),
    )


@pytest.fixture
def mock_server() -> MockQwenPawServer:
    return MockQwenPawServer()


async def _client_for(mock_server: MockQwenPawServer) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=mock_server.app))


async def _collect_stream(stream) -> list:
    frames = []
    async for env in stream:
        frames.append(env)
    return frames


async def _start_bridge(
    bus, definition: BridgeDefinition, mock_server: MockQwenPawServer
) -> QwenPawChatSSEBridge:
    client = await _client_for(mock_server)
    bridge = QwenPawChatSSEBridge(bus, definition, client=client)
    await bridge.start()
    return bridge


def _request_envelope(bus, target: str, *, payload: dict, session_id: str | None = None,
                     metadata: dict | None = None) -> Envelope:
    """Build a request Envelope carrying payload/session/metadata for stream_invoke."""
    req = Envelope.new(MessageReceived)
    req.from_ = bus.agent_id
    req.to = target
    if session_id is not None:
        req.session_id = session_id
    req.payload = bus._codec.encode_payload(payload)
    if metadata is not None:
        req.metadata = metadata
    return req


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    async def test_stream_invoke_sse_deltas_and_final(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        """Incremental multi-chunk SSE produces one delta per chunk + one final."""
        mock_server.set_stream(
            [
                assistant_event("你好"),
                assistant_event("，"),
                assistant_event("世界", "completed"),
            ]
        )
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hello"})
            frames = await _collect_stream(stream)

            deltas = [f for f in frames if f.event_type == ResponseDelta]
            finals = [f for f in frames if f.event_type == ResponseFinal]

            assert [d.payload_json()["delta"] for d in deltas] == [
                "你好",
                "，",
                "世界",
            ]
            assert len(finals) == 1
            assert finals[0].payload_json()["text"] == "你好，世界"
            assert finals[0].payload_json()["raw"]["status"] == "completed"
        finally:
            await bridge.stop()

    async def test_cumulative_text_dedup(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        """QwenPaw cumulative events: only the new suffix is forwarded per delta."""
        mock_server.set_stream(
            [
                assistant_event("你"),
                assistant_event("你好"),
                assistant_event("你好，"),
                assistant_event("你好，世界", "completed"),
            ]
        )
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hello"})
            frames = await _collect_stream(stream)

            deltas = [f for f in frames if f.event_type == ResponseDelta]
            finals = [f for f in frames if f.event_type == ResponseFinal]

            assert [d.payload_json()["delta"] for d in deltas] == [
                "你",
                "好",
                "，",
                "世界",
            ]
            assert len(finals) == 1
            assert finals[0].payload_json()["text"] == "你好，世界"
        finally:
            await bridge.stop()

    async def test_top_level_text_delta_events(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        """Current QwenPaw streams text as top-level content delta events."""
        mock_server.set_stream(
            [
                {"status": "created", "output": []},
                {"status": "in_progress", "output": []},
                {
                    "type": "reasoning",
                    "role": "assistant",
                    "content": [],
                    "status": "in_progress",
                },
                text_delta_event("你"),
                text_delta_event("好"),
                {"status": "completed", "output": []},
            ]
        )
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hello"})
            frames = await _collect_stream(stream)

            deltas = [f for f in frames if f.event_type == ResponseDelta]
            finals = [f for f in frames if f.event_type == ResponseFinal]

            assert [d.payload_json()["delta"] for d in deltas] == ["你", "好"]
            assert len(finals) == 1
            assert finals[0].payload_json()["text"] == "你好"
        finally:
            await bridge.stop()

    async def test_stream_end_without_completed_emits_final(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        """A stream that simply ends (no completed/[DONE]) still yields a final."""
        mock_server.set_stream(
            [assistant_event("a"), assistant_event("b")]
        )
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hello"})
            frames = await _collect_stream(stream)

            deltas = [f for f in frames if f.event_type == ResponseDelta]
            finals = [f for f in frames if f.event_type == ResponseFinal]
            assert [d.payload_json()["delta"] for d in deltas] == ["a", "b"]
            assert len(finals) == 1
            assert finals[0].payload_json()["text"] == "ab"
        finally:
            await bridge.stop()

    async def test_done_terminator_tolerated(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        """The OpenAI ``[DONE]`` terminator is tolerated when no completed status."""
        mock_server.set_stream([assistant_event("hi")], append_done=True)
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hello"})
            frames = await _collect_stream(stream)

            deltas = [f for f in frames if f.event_type == ResponseDelta]
            finals = [f for f in frames if f.event_type == ResponseFinal]
            assert [d.payload_json()["delta"] for d in deltas] == ["hi"]
            assert len(finals) == 1
            assert finals[0].payload_json()["text"] == "hi"
        finally:
            await bridge.stop()

    async def test_empty_output_filtered(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        """Events with no assistant text emit no delta; later text still streams."""
        mock_server.set_stream(
            [
                {"status": "in_progress", "output": []},
                assistant_event("ok", "completed"),
            ]
        )
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hello"})
            frames = await _collect_stream(stream)

            deltas = [f for f in frames if f.event_type == ResponseDelta]
            assert [d.payload_json()["delta"] for d in deltas] == ["ok"]
        finally:
            await bridge.stop()

    async def test_session_id_mapped_to_body(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        mock_server.set_stream([assistant_event("ok", "completed")])
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            req = _request_envelope(
                bus, "qwenpaw.chat", payload={"text": "hi"}, session_id="thread_abc"
            )
            stream = await bus.stream_invoke("qwenpaw.chat", req)
            await _collect_stream(stream)

            assert len(mock_server.requests) == 1
            assert mock_server.requests[0]["body"]["session_id"] == "thread_abc"
        finally:
            await bridge.stop()

    async def test_default_user_id_and_channel(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        mock_server.set_stream([assistant_event("ok", "completed")])
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hi"})
            await _collect_stream(stream)

            body = mock_server.requests[0]["body"]
            assert body["user_id"] == "openagentio-user"
            assert body["channel"] == "console"
        finally:
            await bridge.stop()

    async def test_input_shape(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        """payload text is wrapped as input[0].role=user / content[0].type=text."""
        mock_server.set_stream([assistant_event("ok", "completed")])
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hi there"})
            await _collect_stream(stream)

            body = mock_server.requests[0]["body"]
            assert body["input"] == [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": "hi there"}],
                }
            ]
        finally:
            await bridge.stop()


# ---------------------------------------------------------------------------
# Mapping
# ---------------------------------------------------------------------------


class TestMapping:
    async def test_qwenpaw_user_id_override(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        mock_server.set_stream([assistant_event("ok", "completed")])
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            req = _request_envelope(
                bus, "qwenpaw.chat",
                payload={"text": "hi"},
                metadata={"qwenpaw.user_id": "user-001"},
            )
            stream = await bus.stream_invoke("qwenpaw.chat", req)
            await _collect_stream(stream)

            assert mock_server.requests[0]["body"]["user_id"] == "user-001"
        finally:
            await bridge.stop()

    async def test_qwenpaw_channel_override(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        mock_server.set_stream([assistant_event("ok", "completed")])
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            req = _request_envelope(
                bus, "qwenpaw.chat",
                payload={"text": "hi"},
                metadata={"qwenpaw.channel": "web"},
            )
            stream = await bus.stream_invoke("qwenpaw.chat", req)
            await _collect_stream(stream)

            assert mock_server.requests[0]["body"]["channel"] == "web"
        finally:
            await bridge.stop()

    async def test_qwenpaw_extension_passthrough(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        mock_server.set_stream([assistant_event("ok", "completed")])
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            req = _request_envelope(
                bus, "qwenpaw.chat",
                payload={"text": "hi"},
                metadata={"qwenpaw.model": "qwen-max", "qwenpaw.temperature": 0.7},
            )
            stream = await bus.stream_invoke("qwenpaw.chat", req)
            await _collect_stream(stream)

            body = mock_server.requests[0]["body"]
            assert body["model"] == "qwen-max"
            assert body["temperature"] == 0.7
        finally:
            await bridge.stop()

    async def test_acp_metadata_not_passed(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        """acp.* protocol metadata never reaches the QwenPaw body."""
        mock_server.set_stream([assistant_event("ok", "completed")])
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            req = _request_envelope(
                bus, "qwenpaw.chat",
                payload={"text": "hi"},
                metadata={
                    "acp.trace_id": "must-not-pass",
                    "acp.user_id": "evil-user",
                    "acp.retry.attempt": "1",
                },
            )
            stream = await bus.stream_invoke("qwenpaw.chat", req)
            await _collect_stream(stream)

            body = mock_server.requests[0]["body"]
            assert "trace_id" not in body
            assert "retry.attempt" not in body
            # acp.user_id must not hijack the controlled user_id override.
            assert body["user_id"] == "openagentio-user"
        finally:
            await bridge.stop()

    async def test_acp_metadata_not_passed_under_custom_prefix(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        """With metadata_prefix='acp.', acp.* is still skipped (prefix-independent guard)."""
        mock_server.set_stream([assistant_event("ok", "completed")])
        bridge = await _start_bridge(
            bus, _bridge_def(metadata_prefix="acp."), mock_server
        )
        try:
            req = _request_envelope(
                bus, "qwenpaw.chat",
                payload={"text": "hi"},
                metadata={
                    "acp.trace_id": "must-not-pass",
                    "acp.user_id": "evil-user",
                },
            )
            stream = await bus.stream_invoke("qwenpaw.chat", req)
            await _collect_stream(stream)

            body = mock_server.requests[0]["body"]
            assert "trace_id" not in body
            # acp.user_id does not become a controlled override under the
            # 'acp.' prefix: it is protocol metadata, skipped unconditionally.
            assert body["user_id"] == "openagentio-user"
        finally:
            await bridge.stop()

    async def test_payload_extra_overrides_metadata(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        mock_server.set_stream([assistant_event("ok", "completed")])
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            req = _request_envelope(
                bus, "qwenpaw.chat",
                payload={"text": "hi", "model": "from_payload"},
                metadata={"qwenpaw.model": "from_metadata"},
            )
            stream = await bus.stream_invoke("qwenpaw.chat", req)
            await _collect_stream(stream)

            assert mock_server.requests[0]["body"]["model"] == "from_payload"
        finally:
            await bridge.stop()

    async def test_payload_cannot_override_reserved(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        """Payload cannot override input/session_id/user_id/channel."""
        mock_server.set_stream([assistant_event("ok", "completed")])
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            req = _request_envelope(
                bus, "qwenpaw.chat",
                payload={
                    "text": "hi",
                    "input": "evil-input",
                    "session_id": "evil-session",
                    "user_id": "evil-user",
                    "channel": "evil-channel",
                },
                session_id="real-session",
            )
            stream = await bus.stream_invoke("qwenpaw.chat", req)
            await _collect_stream(stream)

            body = mock_server.requests[0]["body"]
            assert body["input"] == [
                {"role": "user", "content": [{"type": "text", "text": "hi"}]}
            ]
            assert body["session_id"] == "real-session"
            assert body["user_id"] == "openagentio-user"
            assert body["channel"] == "console"
        finally:
            await bridge.stop()

    async def test_metadata_cannot_override_reserved_except_controlled(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        """Only qwenpaw.user_id/qwenpaw.channel may touch reserved fields."""
        mock_server.set_stream([assistant_event("ok", "completed")])
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            req = _request_envelope(
                bus, "qwenpaw.chat",
                payload={"text": "hi"},
                metadata={
                    "qwenpaw.input": "evil-input",
                    "qwenpaw.session_id": "evil-session",
                    "qwenpaw.user_id": "ok-user",
                    "qwenpaw.channel": "ok-channel",
                },
                session_id="real-session",
            )
            stream = await bus.stream_invoke("qwenpaw.chat", req)
            await _collect_stream(stream)

            body = mock_server.requests[0]["body"]
            assert body["input"] == [
                {"role": "user", "content": [{"type": "text", "text": "hi"}]}
            ]
            assert body["session_id"] == "real-session"
            assert body["user_id"] == "ok-user"
            assert body["channel"] == "ok-channel"
        finally:
            await bridge.stop()


# ---------------------------------------------------------------------------
# Headers
# ---------------------------------------------------------------------------


class TestHeaders:
    async def test_agent_id_header_always_injected(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        mock_server.set_stream([assistant_event("ok", "completed")])
        bridge = await _start_bridge(
            bus, _bridge_def(agent_id="my-agent"), mock_server
        )
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hi"})
            await _collect_stream(stream)

            headers = mock_server.requests[0]["headers"]
            assert headers["x-agent-id"] == "my-agent"
            assert "authorization" not in headers
        finally:
            await bridge.stop()

    async def test_token_empty_omits_authorization(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        mock_server.set_stream([assistant_event("ok", "completed")])
        bridge = await _start_bridge(bus, _bridge_def(token=""), mock_server)
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hi"})
            await _collect_stream(stream)

            assert "authorization" not in mock_server.requests[0]["headers"]
        finally:
            await bridge.stop()

    async def test_token_injected_as_bearer(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        mock_server.set_stream([assistant_event("ok", "completed")])
        bridge = await _start_bridge(
            bus, _bridge_def(token="secret-token"), mock_server
        )
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hi"})
            await _collect_stream(stream)

            assert (
                mock_server.requests[0]["headers"]["authorization"]
                == "Bearer secret-token"
            )
        finally:
            await bridge.stop()


# ---------------------------------------------------------------------------
# Error scenarios
# ---------------------------------------------------------------------------


class TestErrors:
    async def test_401_auth_failure(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        mock_server.set_error(401, "unauthorized")
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hi"})
            frames = await _collect_stream(stream)
            errors = [f for f in frames if f.event_type == ResponseError]
            assert len(errors) == 1
            payload = errors[0].payload_json()
            assert payload["code"] == "AUTH_FAILURE"
            assert payload["retryable"] is False
        finally:
            await bridge.stop()

    async def test_403_auth_failure(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        mock_server.set_error(403, "forbidden")
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hi"})
            frames = await _collect_stream(stream)
            errors = [f for f in frames if f.event_type == ResponseError]
            assert errors[0].payload_json()["code"] == "AUTH_FAILURE"
        finally:
            await bridge.stop()

    async def test_400_invalid_request(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        mock_server.set_error(400, "bad request")
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hi"})
            frames = await _collect_stream(stream)
            errors = [f for f in frames if f.event_type == ResponseError]
            assert errors[0].payload_json()["code"] == "INVALID_REQUEST"
        finally:
            await bridge.stop()

    async def test_404_invalid_request(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        mock_server.set_error(404, "not found")
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hi"})
            frames = await _collect_stream(stream)
            errors = [f for f in frames if f.event_type == ResponseError]
            assert errors[0].payload_json()["code"] == "INVALID_REQUEST"
        finally:
            await bridge.stop()

    async def test_500_transport_failure(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        mock_server.set_error(500, "internal error")
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hi"})
            frames = await _collect_stream(stream)
            errors = [f for f in frames if f.event_type == ResponseError]
            assert len(errors) == 1
            payload = errors[0].payload_json()
            assert payload["code"] == "TRANSPORT_FAILURE"
            assert payload["retryable"] is True
        finally:
            await bridge.stop()

    async def test_request_timeout(self, bus) -> None:
        """httpx.TimeoutException is mapped to AGENT_TIMEOUT (retryable).

        ASGITransport does not enforce read timeouts, so a MagicMock client
        whose stream() context raises on enter is used to exercise the
        ``except httpx.TimeoutException`` branch directly.
        """
        stream_ctx = AsyncMock()
        stream_ctx.__aenter__ = AsyncMock(
            side_effect=httpx.TimeoutException("timed out")
        )
        stream_ctx.__aexit__ = AsyncMock(return_value=None)

        fake_client = MagicMock()
        fake_client.stream = MagicMock(return_value=stream_ctx)

        bridge = QwenPawChatSSEBridge(
            bus, _bridge_def(request_timeout=0.1), client=fake_client
        )
        await bridge.start()
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hi"})
            frames = await _collect_stream(stream)
            errors = [f for f in frames if f.event_type == ResponseError]
            assert len(errors) == 1
            payload = errors[0].payload_json()
            assert payload["code"] == "AGENT_TIMEOUT"
            assert payload["retryable"] is True
        finally:
            await bridge.stop()

    async def test_network_error(self, bus) -> None:
        """httpx.ConnectError is mapped to TRANSPORT_FAILURE."""
        stream_ctx = AsyncMock()
        stream_ctx.__aenter__ = AsyncMock(
            side_effect=httpx.ConnectError("connection refused")
        )
        stream_ctx.__aexit__ = AsyncMock(return_value=None)

        fake_client = MagicMock()
        fake_client.stream = MagicMock(return_value=stream_ctx)

        bridge = QwenPawChatSSEBridge(bus, _bridge_def(), client=fake_client)
        await bridge.start()
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hi"})
            frames = await _collect_stream(stream)
            errors = [f for f in frames if f.event_type == ResponseError]
            assert len(errors) == 1
            assert errors[0].payload_json()["code"] == "TRANSPORT_FAILURE"
        finally:
            await bridge.stop()

    async def test_sse_failed_event_agent_unavailable(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        """A status:failed SSE event after a partial delta -> AGENT_UNAVAILABLE."""
        mock_server.set_stream(
            [
                assistant_event("partial"),
                {
                    "status": "failed",
                    "error": {
                        "message": "model crashed",
                        "code": "MODEL_EXECUTION_FAILED",
                    },
                },
            ]
        )
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hi"})
            frames = await _collect_stream(stream)
            deltas = [f for f in frames if f.event_type == ResponseDelta]
            errors = [f for f in frames if f.event_type == ResponseError]
            finals = [f for f in frames if f.event_type == ResponseFinal]

            assert [d.payload_json()["delta"] for d in deltas] == ["partial"]
            assert len(errors) == 1
            assert len(finals) == 0
            payload = errors[0].payload_json()
            assert payload["code"] == "AGENT_UNAVAILABLE"
            assert "model crashed" in payload["message"]
        finally:
            await bridge.stop()

    async def test_sse_error_auth_failure(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        """SSE error with auth semantics maps to AUTH_FAILURE."""
        mock_server.set_stream(
            [
                {
                    "status": "in_progress",
                    "error": {"message": "token revoked", "code": "AUTH_EXPIRED"},
                }
            ]
        )
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hi"})
            frames = await _collect_stream(stream)
            errors = [f for f in frames if f.event_type == ResponseError]
            assert len(errors) == 1
            payload = errors[0].payload_json()
            assert payload["code"] == "AUTH_FAILURE"
            assert "token revoked" in payload["message"]
        finally:
            await bridge.stop()

    async def test_sse_error_invalid_request(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        """SSE error with validation semantics maps to INVALID_REQUEST."""
        mock_server.set_stream(
            [
                {
                    "status": "failed",
                    "error": {
                        "message": "context length exceeded",
                        "code": "INVALID_INPUT",
                    },
                }
            ]
        )
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hi"})
            frames = await _collect_stream(stream)
            errors = [f for f in frames if f.event_type == ResponseError]
            assert len(errors) == 1
            payload = errors[0].payload_json()
            assert payload["code"] == "INVALID_REQUEST"
            assert "context length exceeded" in payload["message"]
        finally:
            await bridge.stop()

    async def test_missing_text_field(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        mock_server.set_stream([assistant_event("ok", "completed")])
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        try:
            stream = await bus.stream_invoke(
                "qwenpaw.chat", {"content": "no text field"}
            )
            frames = await _collect_stream(stream)
            errors = [f for f in frames if f.event_type == ResponseError]
            assert len(errors) == 1
            payload = errors[0].payload_json()
            assert payload["code"] == "INVALID_REQUEST"
            assert "missing 'text'" in payload["message"]
        finally:
            await bridge.stop()


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


class TestLifecycle:
    async def test_stop_unsubscribes_handler(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        mock_server.set_stream([assistant_event("ok", "completed")])
        bridge = await _start_bridge(bus, _bridge_def(), mock_server)
        stream = await bus.stream_invoke("qwenpaw.chat", {"text": "hi"})
        await _collect_stream(stream)
        await bridge.stop()
        assert bridge._stream_sub is None


# ---------------------------------------------------------------------------
# BridgeRunner integration
# ---------------------------------------------------------------------------


class TestBridgeRunner:
    async def test_builtin_factory_registered(self, bus) -> None:
        assert "qwenpaw_chat_sse" in BUILTIN_FACTORIES
        bridge = BUILTIN_FACTORIES["qwenpaw_chat_sse"](bus, _bridge_def())
        assert isinstance(bridge, QwenPawChatSSEBridge)

    async def test_runner_starts_and_stops_bridge(
        self, bus, mock_server: MockQwenPawServer
    ) -> None:
        mock_server.set_stream([assistant_event("via", "completed")])

        def factory(bus_, definition):
            client = httpx.AsyncClient(
                transport=httpx.ASGITransport(app=mock_server.app),
            )
            return QwenPawChatSSEBridge(bus_, definition, client=client)

        cfg = BridgeConfig(
            version="openagentio.bridge/v1",
            bridges=(_bridge_def(),),
        )
        runner = BridgeRunner(bus, cfg, {"qwenpaw_chat_sse": factory})
        await runner.start()
        try:
            stream = await bus.stream_invoke("qwenpaw.chat", {"text": "via runner"})
            frames = await _collect_stream(stream)
            finals = [f for f in frames if f.event_type == ResponseFinal]
            assert len(finals) == 1
            assert finals[0].payload_json()["text"] == "via"
        finally:
            await runner.stop()

    async def test_factory_helper_returns_bridge(self, bus) -> None:
        bridge = qwenpaw_chat_sse_factory(bus, _bridge_def())
        assert isinstance(bridge, QwenPawChatSSEBridge)


# ---------------------------------------------------------------------------
# QwenPawChatBridge convenience wrapper
# ---------------------------------------------------------------------------


class TestQwenPawChatBridgeConvenience:
    def test_from_env_defaults(
        self, bus, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        for var in (
            "QWENPAW_BASE_URL",
            "QWENPAW_AUTH_TOKEN",
            "QWENPAW_AGENT_ID",
            "QWENPAW_USER_ID",
            "QWENPAW_CHANNEL",
            "QWENPAW_REQUEST_TIMEOUT",
        ):
            monkeypatch.delenv(var, raising=False)

        bridge = QwenPawChatBridge.from_env(bus)

        assert bridge.target == "qwenpaw.chat"
        inner = bridge._bridge
        assert inner._base_url == "http://127.0.0.1:8088"
        assert inner._token == ""
        assert inner._agent_id == "default"
        assert inner._user_id == "openagentio-user"
        assert inner._channel == "console"
        assert inner._request_timeout == 120.0

    def test_from_env_custom_values(
        self, bus, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("QWENPAW_BASE_URL", "http://qwenpaw.local:8088")
        monkeypatch.setenv("QWENPAW_AUTH_TOKEN", "env-token")
        monkeypatch.setenv("QWENPAW_AGENT_ID", "env-agent")
        monkeypatch.setenv("QWENPAW_USER_ID", "env-user")
        monkeypatch.setenv("QWENPAW_CHANNEL", "env-channel")
        monkeypatch.setenv("QWENPAW_REQUEST_TIMEOUT", "30")

        bridge = QwenPawChatBridge.from_env(bus, target="custom.target")

        assert bridge.target == "custom.target"
        inner = bridge._bridge
        assert inner._base_url == "http://qwenpaw.local:8088"
        assert inner._token == "env-token"
        assert inner._agent_id == "env-agent"
        assert inner._user_id == "env-user"
        assert inner._channel == "env-channel"
        assert inner._request_timeout == 30.0

    def test_from_env_custom_env_names(
        self, bus, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("MY_URL", "http://custom.env:8088")
        monkeypatch.setenv("MY_TOKEN", "custom-token")

        bridge = QwenPawChatBridge.from_env(
            bus,
            base_url_env="MY_URL",
            token_env="MY_TOKEN",
        )

        inner = bridge._bridge
        assert inner._base_url == "http://custom.env:8088"
        assert inner._token == "custom-token"


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestConfigValidation:
    async def test_missing_base_url_raises_value_error(self, bus) -> None:
        defn = _bridge_def(base_url="")
        with pytest.raises(ValueError, match="base_url"):
            QwenPawChatSSEBridge(bus, defn)

    async def test_zero_request_timeout_raises_value_error(self, bus) -> None:
        defn = _bridge_def(request_timeout=0)
        with pytest.raises(ValueError, match="request_timeout"):
            QwenPawChatSSEBridge(bus, defn)

    async def test_token_empty_allowed(self, bus) -> None:
        """Unlike OpenClaw, an empty QwenPaw token is a valid local config."""
        defn = _bridge_def(token="")
        bridge = QwenPawChatSSEBridge(bus, defn)
        assert bridge._token == ""
