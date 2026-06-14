"""Tests for openagentio.bridge.mcp_tool — MCP Tool bridge."""

from __future__ import annotations

import asyncio
import json
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any, AsyncGenerator

import pytest

from openagentio import Bus, InMemoryDriver, WithTimeout
from openagentio.bridge import BUILTIN_FACTORIES, McpToolBridge, mcp_tool_factory
from openagentio.bridge.config import (
    BridgeConfig,
    BridgeConfigError,
    BridgeDefinition,
    BridgeMappings,
)
from openagentio.bridge.mcp_tool import _import_mcp, _map_mcp_error
from openagentio.bridge.runner import BridgeRunner
from openagentio.event.payload import (
    CodeAgentTimeout,
    CodeAgentUnavailable,
    CodeAuthFailure,
    CodeInvalidRequest,
    CodeTransportFailure,
)
from openagentio.event.types import ResponseError, ResponseFinal

# Imported before patch_mcp replaces the mcp package in sys.modules.
from mcp.types import CallToolResult as RealCallToolResult
from mcp.types import TextContent as RealTextContent


# ---------------------------------------------------------------------------
# Fake MCP SDK
# ---------------------------------------------------------------------------


@dataclass
class FakeTextContent:
    type: str = "text"
    text: str = ""

    def model_dump(self) -> dict[str, Any]:
        return {"type": self.type, "text": self.text}


@dataclass
class FakeImageContent:
    type: str = "image"
    mimeType: str = "image/png"
    data: str = ""

    def model_dump(self) -> dict[str, Any]:
        return {"type": self.type, "mimeType": self.mimeType, "data": self.data}


@dataclass
class FakeCallToolResult:
    content: list[Any] = field(default_factory=list)
    isError: bool = False
    structuredContent: dict[str, Any] | None = None
    meta: dict[str, Any] | None = None

    def model_dump(self) -> dict[str, Any]:
        return {
            "content": self.content,
            "isError": self.isError,
            "structuredContent": self.structuredContent,
            "meta": self.meta,
        }


@dataclass
class FakeErrorData:
    code: int
    message: str


class FakeMcpError(Exception):
    def __init__(self, error: FakeErrorData):
        super().__init__(error.message)
        self.error = error


@dataclass
class FakeTool:
    name: str
    description: str = ""
    inputSchema: dict[str, Any] = field(default_factory=dict)


@dataclass
class FakeListToolsResult:
    tools: list[FakeTool] = field(default_factory=list)


@dataclass
class FakeStdioServerParameters:
    command: str
    args: list[str] = field(default_factory=list)
    env: dict[str, str] | None = None


class FakeClientSession:
    def __init__(self, tools: list[FakeTool], call_results: dict[str, Any]):
        self._tools = tools
        self._call_results = call_results
        self.initialized = False
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def initialize(self) -> None:
        self.initialized = True

    async def list_tools(self) -> FakeListToolsResult:
        return FakeListToolsResult(tools=self._tools)

    async def call_tool(
        self,
        name: str,
        arguments: dict[str, Any] | None = None,
        *,
        meta: dict[str, Any] | None = None,
    ) -> Any:
        self.calls.append((name, arguments or {}, meta))
        if name in self._call_results:
            return self._call_results[name]
        return FakeCallToolResult(content=[FakeTextContent(text=f"result for {name}")])

    async def __aenter__(self) -> "FakeClientSession":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        pass


class FakeStdioClient:
    def __init__(self, session: FakeClientSession):
        self._session = session

    async def __aenter__(self) -> tuple[Any, Any]:
        return (object(), object())

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        pass


def _make_fake_mcp_module(
    *,
    tools: list[FakeTool] | None = None,
    call_results: dict[str, Any] | None = None,
) -> ModuleType:
    """Build a fake ``mcp`` package plus its ``mcp.client.stdio`` submodule."""
    tools = tools or []
    call_results = call_results or {}
    session = FakeClientSession(tools, call_results)

    mcp_mod = ModuleType("mcp")
    mcp_mod.StdioServerParameters = FakeStdioServerParameters
    mcp_mod.ClientSession = lambda _read, _write: session
    mcp_mod.types = ModuleType("mcp.types")
    mcp_mod.types.ErrorData = lambda *, code, message: FakeErrorData(code, message)
    mcp_mod.types.PARSE_ERROR = -32700
    mcp_mod.types.INVALID_REQUEST = -32600
    mcp_mod.types.METHOD_NOT_FOUND = -32601
    mcp_mod.types.INVALID_PARAMS = -32602
    mcp_mod.types.INTERNAL_ERROR = -32603
    mcp_mod.McpError = FakeMcpError

    client_mod = ModuleType("mcp.client")
    stdio_mod = ModuleType("mcp.client.stdio")

    @asynccontextmanager
    async def stdio_client(params: Any):
        yield (object(), object())

    stdio_mod.stdio_client = stdio_client
    client_mod.stdio = stdio_mod
    mcp_mod.client = client_mod

    return mcp_mod


@pytest.fixture
def patch_mcp(monkeypatch: pytest.MonkeyPatch):
    """Patch the ``mcp`` package with a configurable fake implementation."""
    injected: dict[str, Any] = {}

    def _patch(**kwargs: Any) -> FakeClientSession:
        mcp_mod = _make_fake_mcp_module(**kwargs)
        monkeypatch.setitem(sys.modules, "mcp", mcp_mod)
        monkeypatch.setitem(sys.modules, "mcp.client", mcp_mod.client)
        monkeypatch.setitem(sys.modules, "mcp.client.stdio", mcp_mod.client.stdio)
        # _import_mcp caches via sys.modules, so this returns the fake module.
        return mcp_mod.ClientSession(None, None)  # type: ignore[call-arg]

    injected["patch"] = _patch
    yield _patch


@pytest.fixture
def bus() -> Bus:
    b = Bus(agent_id="test-agent", transport=InMemoryDriver())
    return b


async def _connected_bus() -> Bus:
    b = Bus(agent_id="test-agent", transport=InMemoryDriver())
    await b.connect()
    return b


def _bridge_def(
    *,
    name: str = "mcp-fs",
    target_prefix: str | None = None,
    transport: str = "stdio",
    command: str = "npx",
    args: list[str] | None = None,
    env: dict[str, Any] | None = None,
    http_url: str = "",
    token: str = "",
    headers: dict[str, str] | None = None,
    timeout: float = 5.0,
) -> BridgeDefinition:
    extra: dict[str, Any] = {}
    if target_prefix is not None:
        extra["target_prefix"] = target_prefix
    config: dict[str, Any] = {
        "transport": transport,
        "timeout": timeout,
    }
    if transport == "stdio":
        config["command"] = command
        config["args"] = args or ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"]
        config["env"] = env or {}
    else:
        config["http_url"] = http_url
        if token:
            config["token"] = token
        if headers:
            config["headers"] = headers
    return BridgeDefinition(
        name=name,
        type="mcp_tool",
        config=config,
        mappings=BridgeMappings(extra=extra),
    )


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestConfigValidation:
    def test_args_must_be_a_list(self) -> None:
        with pytest.raises(BridgeConfigError, match="config 'args' must be a list of strings"):
            McpToolBridge(
                None,  # type: ignore[arg-type]
                _bridge_def(args="abc"),
            )

    def test_args_items_must_be_strings(self) -> None:
        with pytest.raises(BridgeConfigError, match="config 'args\\[1\\]' must be a string"):
            McpToolBridge(
                None,  # type: ignore[arg-type]
                _bridge_def(args=["-y", 123]),
            )

    def test_env_must_be_a_mapping(self) -> None:
        with pytest.raises(BridgeConfigError, match="config 'env' must be a mapping of strings"):
            McpToolBridge(
                None,  # type: ignore[arg-type]
                _bridge_def(env="production"),
            )

    def test_env_values_must_be_strings(self) -> None:
        with pytest.raises(BridgeConfigError, match="config 'env' value for key 'PORT'"):
            McpToolBridge(
                None,  # type: ignore[arg-type]
                _bridge_def(env={"PORT": 8080}),
            )

    def test_env_keys_must_be_strings(self) -> None:
        with pytest.raises(BridgeConfigError, match="config 'env' keys must be strings"):
            McpToolBridge(
                None,  # type: ignore[arg-type]
                _bridge_def(env={1: "one"}),
            )

    def test_streamable_http_requires_http_url(self) -> None:
        with pytest.raises(BridgeConfigError, match="config 'http_url' is required"):
            McpToolBridge(
                None,  # type: ignore[arg-type]
                _bridge_def(transport="streamable_http"),
            )

    def test_streamable_http_accepts_token_and_headers(self) -> None:
        bridge = McpToolBridge(
            None,  # type: ignore[arg-type]
            _bridge_def(
                transport="streamable_http",
                http_url="http://localhost:8000/mcp",
                token="secret",
                headers={"X-Custom": "value"},
            ),
        )
        assert bridge._http_url == "http://localhost:8000/mcp"
        assert bridge._http_headers["Authorization"] == "Bearer secret"
        assert bridge._http_headers["X-Custom"] == "value"

    def test_streamable_http_headers_must_be_strings(self) -> None:
        with pytest.raises(BridgeConfigError, match="config 'headers' value for key"):
            McpToolBridge(
                None,  # type: ignore[arg-type]
                _bridge_def(
                    transport="streamable_http",
                    http_url="http://localhost:8000/mcp",
                    headers={"X-Custom": 123},
                ),
            )


# ---------------------------------------------------------------------------
# Lifecycle and discovery
# ---------------------------------------------------------------------------


class TestLifecycle:
    async def test_discovers_tools_and_registers_targets(
        self, patch_mcp: Any, bus: Bus
    ) -> None:
        await bus.connect()
        patch_mcp(
            tools=[
                FakeTool(name="read_file"),
                FakeTool(name="write_file"),
            ]
        )
        defn = _bridge_def(name="mcp-fs")
        bridge = McpToolBridge(bus, defn)
        await bridge.start()
        try:
            resp = await bus.invoke("mcp-fs.read_file", {"path": "x.txt"})
            assert resp.event_type == ResponseFinal
            payload = resp.payload_json()
            assert payload["text"] == "result for read_file"
            assert payload["content"] == [{"type": "text", "text": "result for read_file"}]
        finally:
            await bridge.stop()

    async def test_target_prefix_derived_from_bridge_name(
        self, patch_mcp: Any, bus: Bus
    ) -> None:
        await bus.connect()
        patch_mcp(tools=[FakeTool(name="echo")])
        defn = _bridge_def(name="custom-mcp")
        bridge = McpToolBridge(bus, defn)
        await bridge.start()
        try:
            resp = await bus.invoke("custom-mcp.echo", {"msg": "hi"})
            assert resp.event_type == ResponseFinal
        finally:
            await bridge.stop()

    async def test_target_prefix_from_mappings_extra(
        self, patch_mcp: Any, bus: Bus
    ) -> None:
        await bus.connect()
        patch_mcp(tools=[FakeTool(name="ping")])
        defn = _bridge_def(name="my-bridge", target_prefix="tools")
        bridge = McpToolBridge(bus, defn)
        await bridge.start()
        try:
            resp = await bus.invoke("tools.ping", {})
            assert resp.event_type == ResponseFinal
        finally:
            await bridge.stop()

    async def test_stop_is_idempotent_and_safe_after_partial_start(
        self, patch_mcp: Any, bus: Bus
    ) -> None:
        await bus.connect()

        class BrokenSession(FakeClientSession):
            async def list_tools(self) -> FakeListToolsResult:
                raise RuntimeError("discovery failed")

        mcp_mod = _make_fake_mcp_module()
        mcp_mod.ClientSession = lambda _read, _write: BrokenSession([], {})

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setitem(sys.modules, "mcp", mcp_mod)
            monkeypatch.setitem(sys.modules, "mcp.client", mcp_mod.client)
            monkeypatch.setitem(sys.modules, "mcp.client.stdio", mcp_mod.client.stdio)

            bridge = McpToolBridge(bus, _bridge_def())
            with pytest.raises(Exception) as exc_info:
                await bridge.start()
            assert exc_info.value.code == CodeTransportFailure
            # Should not raise even though start() failed partway through.
            await bridge.stop()
            await bridge.stop()

    async def test_streamable_http_transport_discovers_and_invokes_tools(
        self, bus: Bus
    ) -> None:
        await bus.connect()

        mcp_mod = _make_fake_mcp_module(
            tools=[FakeTool(name="ping")],
            call_results={"ping": FakeCallToolResult(content=[FakeTextContent(text="pong")])},
        )

        captured: dict[str, Any] = {}

        @asynccontextmanager
        async def fake_streamable_http_client(
            url: str, *, http_client: Any = None
        ) -> AsyncGenerator[tuple[Any, Any, Any], None]:
            captured["url"] = url
            captured["client"] = http_client
            yield (object(), object(), lambda: None)

        mcp_mod.client.streamable_http_client = fake_streamable_http_client

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setitem(sys.modules, "mcp", mcp_mod)
            monkeypatch.setitem(sys.modules, "mcp.client", mcp_mod.client)
            monkeypatch.setitem(sys.modules, "mcp.client.stdio", mcp_mod.client.stdio)
            monkeypatch.setitem(
                sys.modules, "mcp.client.streamable_http", mcp_mod.client
            )

            bridge = McpToolBridge(
                bus,
                _bridge_def(
                    transport="streamable_http",
                    http_url="http://localhost:8000/mcp",
                    token="secret-token",
                ),
            )
            await bridge.start()
            try:
                resp = await bus.invoke("mcp-fs.ping", {})
                assert resp.event_type == ResponseFinal
                assert resp.payload_json()["text"] == "pong"
            finally:
                await bridge.stop()

        assert captured["url"] == "http://localhost:8000/mcp"
        assert captured["client"] is not None
        assert captured["client"].headers["Authorization"] == "Bearer secret-token"


# ---------------------------------------------------------------------------
# Invocation mapping
# ---------------------------------------------------------------------------


class TestInvocation:
    async def test_arguments_passed_to_call_tool(
        self, patch_mcp: Any, bus: Bus
    ) -> None:
        await bus.connect()
        session = patch_mcp(
            tools=[FakeTool(name="search")],
            call_results={
                "search": FakeCallToolResult(
                    content=[FakeTextContent(text="found")]
                )
            },
        )
        defn = _bridge_def(name="mcp-tools")
        bridge = McpToolBridge(bus, defn)
        await bridge.start()
        try:
            resp = await bus.invoke(
                "mcp-tools.search", {"query": "python", "limit": 10}
            )
            assert resp.event_type == ResponseFinal
            assert session.calls == [("search", {"query": "python", "limit": 10}, None)]
        finally:
            await bridge.stop()

    async def test_null_payload_treated_as_empty_arguments(
        self, patch_mcp: Any, bus: Bus
    ) -> None:
        await bus.connect()
        session = patch_mcp(
            tools=[FakeTool(name="ping")],
            call_results={"ping": FakeCallToolResult(content=[FakeTextContent(text="pong")])},
        )
        bridge = McpToolBridge(bus, _bridge_def())
        await bridge.start()
        try:
            resp = await bus.invoke("mcp-fs.ping")
            assert resp.event_type == ResponseFinal
            assert session.calls == [("ping", {}, None)]
        finally:
            await bridge.stop()

    async def test_non_object_payload_returns_invalid_request(
        self, patch_mcp: Any, bus: Bus
    ) -> None:
        await bus.connect()
        patch_mcp(tools=[FakeTool(name="read")])
        bridge = McpToolBridge(bus, _bridge_def())
        await bridge.start()
        try:
            resp = await bus.invoke("mcp-fs.read", "not-an-object")
            assert resp.event_type == ResponseError
            payload = resp.payload_json()
            assert payload["code"] == CodeInvalidRequest
        finally:
            await bridge.stop()

    async def test_multiple_content_items_mapped(
        self, patch_mcp: Any, bus: Bus
    ) -> None:
        await bus.connect()
        patch_mcp(
            tools=[FakeTool(name="render")],
            call_results={
                "render": FakeCallToolResult(
                    content=[
                        FakeTextContent(text="Here is the chart:"),
                        FakeImageContent(mimeType="image/png", data="base64data"),
                    ]
                )
            },
        )
        bridge = McpToolBridge(bus, _bridge_def())
        await bridge.start()
        try:
            resp = await bus.invoke("mcp-fs.render", {})
            assert resp.event_type == ResponseFinal
            payload = resp.payload_json()
            assert "text" not in payload  # only single text gets convenience field
            assert payload["content"] == [
                {"type": "text", "text": "Here is the chart:"},
                {"type": "image", "mimeType": "image/png", "data": "base64data"},
            ]
        finally:
            await bridge.stop()

    async def test_structured_content_and_meta_are_preserved(
        self, patch_mcp: Any, bus: Bus
    ) -> None:
        await bus.connect()
        patch_mcp(
            tools=[FakeTool(name="structured")],
            call_results={
                "structured": FakeCallToolResult(
                    content=[FakeTextContent(text="ok")],
                    structuredContent={"count": 42, "items": ["a", "b"]},
                    meta={"request_id": "abc-123"},
                )
            },
        )
        bridge = McpToolBridge(bus, _bridge_def())
        await bridge.start()
        try:
            resp = await bus.invoke("mcp-fs.structured", {})
            assert resp.event_type == ResponseFinal
            payload = resp.payload_json()
            assert payload["text"] == "ok"
            assert payload["structuredContent"] == {"count": 42, "items": ["a", "b"]}
            assert payload["meta"] == {"request_id": "abc-123"}
        finally:
            await bridge.stop()

    async def test_real_mcp_result_meta_alias_is_mapped(
        self, patch_mcp: Any, bus: Bus
    ) -> None:
        await bus.connect()

        patch_mcp(
            tools=[FakeTool(name="real")],
            call_results={
                "real": RealCallToolResult(
                    content=[RealTextContent(type="text", text="ok")],
                    structuredContent={"count": 1},
                    meta={"request_id": "xyz"},
                )
            },
        )
        bridge = McpToolBridge(bus, _bridge_def())
        await bridge.start()
        try:
            resp = await bus.invoke("mcp-fs.real", {})
            assert resp.event_type == ResponseFinal
            payload = resp.payload_json()
            assert payload["text"] == "ok"
            assert payload["structuredContent"] == {"count": 1}
            assert payload["meta"] == {"request_id": "xyz"}
        finally:
            await bridge.stop()

    async def test_tool_is_error_result_returns_error_response(
        self, patch_mcp: Any, bus: Bus
    ) -> None:
        await bus.connect()
        patch_mcp(
            tools=[FakeTool(name="risky")],
            call_results={
                "risky": FakeCallToolResult(
                    content=[FakeTextContent(text="exploded")],
                    isError=True,
                )
            },
        )
        bridge = McpToolBridge(bus, _bridge_def())
        await bridge.start()
        try:
            resp = await bus.invoke("mcp-fs.risky", {})
            assert resp.event_type == ResponseError
            payload = resp.payload_json()
            assert payload["code"] == CodeInvalidRequest
            assert "exploded" in payload["message"]
        finally:
            await bridge.stop()


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------


class TestErrorMapping:
    async def test_call_tool_timeout_maps_to_agent_timeout(
        self, patch_mcp: Any, bus: Bus
    ) -> None:
        await bus.connect()

        class FailingSession(FakeClientSession):
            async def call_tool(
                self,
                name: str,
                arguments: dict[str, Any] | None = None,
                *,
                meta: dict[str, Any] | None = None,
            ) -> Any:
                raise asyncio.TimeoutError()

        mcp_mod = _make_fake_mcp_module(tools=[FakeTool(name="slow")])
        mcp_mod.ClientSession = lambda _read, _write: FailingSession(
            [FakeTool(name="slow")], {}
        )

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setitem(sys.modules, "mcp", mcp_mod)
            monkeypatch.setitem(sys.modules, "mcp.client", mcp_mod.client)
            monkeypatch.setitem(sys.modules, "mcp.client.stdio", mcp_mod.client.stdio)

            bridge = McpToolBridge(bus, _bridge_def(timeout=5.0))
            await bridge.start()
            try:
                resp = await bus.invoke("mcp-fs.slow", {})
                assert resp.event_type == ResponseError
                assert resp.payload_json()["code"] == CodeAgentTimeout
            finally:
                await bridge.stop()

    async def test_mcp_invalid_params_maps_to_invalid_request(
        self, patch_mcp: Any, bus: Bus
    ) -> None:
        await bus.connect()

        class FailingSession(FakeClientSession):
            async def call_tool(
                self,
                name: str,
                arguments: dict[str, Any] | None = None,
                *,
                meta: dict[str, Any] | None = None,
            ) -> Any:
                raise FakeMcpError(
                    FakeErrorData(code=-32602, message="bad args")
                )

        mcp_mod = _make_fake_mcp_module(tools=[FakeTool(name="calc")])
        mcp_mod.ClientSession = lambda _read, _write: FailingSession(
            [FakeTool(name="calc")], {}
        )

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setitem(sys.modules, "mcp", mcp_mod)
            monkeypatch.setitem(sys.modules, "mcp.client", mcp_mod.client)
            monkeypatch.setitem(sys.modules, "mcp.client.stdio", mcp_mod.client.stdio)

            bridge = McpToolBridge(bus, _bridge_def())
            await bridge.start()
            try:
                resp = await bus.invoke("mcp-fs.calc", {})
                assert resp.event_type == ResponseError
                payload = resp.payload_json()
                assert payload["code"] == CodeInvalidRequest
                assert "bad args" in payload["message"]
            finally:
                await bridge.stop()

    async def test_mcp_internal_error_maps_to_transport_failure(
        self, patch_mcp: Any, bus: Bus
    ) -> None:
        await bus.connect()

        class FailingSession(FakeClientSession):
            async def call_tool(
                self,
                name: str,
                arguments: dict[str, Any] | None = None,
                *,
                meta: dict[str, Any] | None = None,
            ) -> Any:
                raise FakeMcpError(
                    FakeErrorData(code=-32603, message="server exploded")
                )

        mcp_mod = _make_fake_mcp_module(tools=[FakeTool(name="calc")])
        mcp_mod.ClientSession = lambda _read, _write: FailingSession(
            [FakeTool(name="calc")], {}
        )

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setitem(sys.modules, "mcp", mcp_mod)
            monkeypatch.setitem(sys.modules, "mcp.client", mcp_mod.client)
            monkeypatch.setitem(sys.modules, "mcp.client.stdio", mcp_mod.client.stdio)

            bridge = McpToolBridge(bus, _bridge_def())
            await bridge.start()
            try:
                resp = await bus.invoke("mcp-fs.calc", {})
                assert resp.event_type == ResponseError
                assert resp.payload_json()["code"] == CodeTransportFailure
            finally:
                await bridge.stop()

    async def test_start_failure_maps_to_agent_unavailable_on_connection_error(
        self, bus: Bus
    ) -> None:
        await bus.connect()

        mcp_mod = ModuleType("mcp")
        mcp_mod.StdioServerParameters = FakeStdioServerParameters
        mcp_mod.McpError = FakeMcpError
        mcp_mod.types = ModuleType("mcp.types")
        mcp_mod.types.INVALID_PARAMS = -32602
        client_mod = ModuleType("mcp.client")
        stdio_mod = ModuleType("mcp.client.stdio")

        @asynccontextmanager
        async def broken_stdio(params: Any):
            raise ConnectionError("subprocess failed to start")
            yield  # type: ignore[unreachable]

        stdio_mod.stdio_client = broken_stdio
        client_mod.stdio = stdio_mod
        mcp_mod.client = client_mod

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setitem(sys.modules, "mcp", mcp_mod)
            monkeypatch.setitem(sys.modules, "mcp.client", mcp_mod.client)
            monkeypatch.setitem(sys.modules, "mcp.client.stdio", mcp_mod.client.stdio)

            bridge = McpToolBridge(bus, _bridge_def())
            with pytest.raises(Exception) as exc_info:
                await bridge.start()
            assert exc_info.value.code == CodeAgentUnavailable
            await bridge.stop()


# ---------------------------------------------------------------------------
# Streamable HTTP error mapping
# ---------------------------------------------------------------------------


class TestStreamableHttpErrorMapping:
    def test_http_401_maps_to_auth_failure(self) -> None:
        import httpx

        request = httpx.Request("POST", "http://localhost/mcp")
        response = httpx.Response(401, request=request)
        exc = httpx.HTTPStatusError("Unauthorized", request=request, response=response)
        mapped = _map_mcp_error("mcp-http", exc)
        assert mapped.code == CodeAuthFailure

    def test_http_403_maps_to_auth_failure(self) -> None:
        import httpx

        request = httpx.Request("POST", "http://localhost/mcp")
        response = httpx.Response(403, request=request)
        exc = httpx.HTTPStatusError("Forbidden", request=request, response=response)
        mapped = _map_mcp_error("mcp-http", exc)
        assert mapped.code == CodeAuthFailure

    def test_http_500_maps_to_transport_failure(self) -> None:
        import httpx

        request = httpx.Request("POST", "http://localhost/mcp")
        response = httpx.Response(500, request=request)
        exc = httpx.HTTPStatusError("Server Error", request=request, response=response)
        mapped = _map_mcp_error("mcp-http", exc)
        assert mapped.code == CodeTransportFailure

    def test_http_400_maps_to_invalid_request(self) -> None:
        import httpx

        request = httpx.Request("POST", "http://localhost/mcp")
        response = httpx.Response(400, request=request)
        exc = httpx.HTTPStatusError("Bad Request", request=request, response=response)
        mapped = _map_mcp_error("mcp-http", exc)
        assert mapped.code == CodeInvalidRequest

    def test_http_connect_error_maps_to_agent_unavailable(self) -> None:
        import httpx

        exc = httpx.ConnectError("connection refused")
        mapped = _map_mcp_error("mcp-http", exc)
        assert mapped.code == CodeAgentUnavailable

    def test_http_timeout_maps_to_agent_timeout(self) -> None:
        import httpx

        exc = httpx.TimeoutException("request timed out")
        mapped = _map_mcp_error("mcp-http", exc)
        assert mapped.code == CodeAgentTimeout

# ---------------------------------------------------------------------------
# Streamable HTTP integration
# ---------------------------------------------------------------------------


class TestStreamableHttpIntegration:
    def _make_bridge(
        self,
        bus: Bus,
        status_code: int,
        *,
        http_url: str = "http://test/mcp",
    ) -> tuple[McpToolBridge, Any]:
        import httpx
        from starlette.applications import Starlette
        from starlette.responses import Response
        from starlette.routing import Route

        async def endpoint(request: Any) -> Response:
            return Response("error", status_code=status_code)

        app = Starlette(routes=[Route("/mcp", endpoint, methods=["GET", "POST", "DELETE"])])
        transport = httpx.ASGITransport(app=app)
        client = httpx.AsyncClient(transport=transport, base_url="http://test")

        defn = BridgeDefinition(
            name="mcp-http",
            type="mcp_tool",
            config={
                "transport": "streamable_http",
                "http_url": http_url,
                "timeout": 5,
            },
            mappings=BridgeMappings(extra={"target_prefix": "http"}),
        )
        bridge = McpToolBridge(bus, defn, http_client=client)
        return bridge, client

    async def test_401_auth_failure(self, bus: Bus) -> None:
        await bus.connect()
        bridge, client = self._make_bridge(bus, 401)
        try:
            with pytest.raises(Exception) as exc_info:
                await bridge.start()
            assert exc_info.value.code == CodeAuthFailure
        finally:
            await bridge.stop()
            await client.aclose()

    async def test_403_auth_failure(self, bus: Bus) -> None:
        await bus.connect()
        bridge, client = self._make_bridge(bus, 403)
        try:
            with pytest.raises(Exception) as exc_info:
                await bridge.start()
            assert exc_info.value.code == CodeAuthFailure
        finally:
            await bridge.stop()
            await client.aclose()

    async def test_500_transport_failure(self, bus: Bus) -> None:
        await bus.connect()
        bridge, client = self._make_bridge(bus, 500)
        try:
            with pytest.raises(Exception) as exc_info:
                await bridge.start()
            assert exc_info.value.code == CodeTransportFailure
        finally:
            await bridge.stop()
            await client.aclose()


# ---------------------------------------------------------------------------
# Session / trace propagation
# ---------------------------------------------------------------------------


class TestSessionTracePropagation:
    async def test_session_and_traceparent_passed_via_call_tool_meta(
        self, bus: Bus
    ) -> None:
        await bus.connect()

        captured_meta: dict[str, Any] | None = None

        class MetaCapturingSession(FakeClientSession):
            async def call_tool(
                self,
                name: str,
                arguments: dict[str, Any] | None = None,
                *,
                meta: dict[str, Any] | None = None,
            ) -> Any:
                nonlocal captured_meta
                captured_meta = meta
                return FakeCallToolResult(content=[FakeTextContent(text="pong")])

        mcp_mod = _make_fake_mcp_module(tools=[FakeTool(name="ping")])
        mcp_mod.ClientSession = lambda _read, _write: MetaCapturingSession(
            [FakeTool(name="ping")], {}
        )

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setitem(sys.modules, "mcp", mcp_mod)
            monkeypatch.setitem(sys.modules, "mcp.client", mcp_mod.client)
            monkeypatch.setitem(sys.modules, "mcp.client.stdio", mcp_mod.client.stdio)

            bridge = McpToolBridge(bus, _bridge_def())
            await bridge.start()
            try:
                from openagentio.event.envelope import Envelope
                from openagentio.event.types import MessageReceived

                env = Envelope.new(MessageReceived)
                env.session_id = "sess-1"
                env.traceparent = "tp-00-1234567890abcdef-1234567890abcdef-01"
                resp = await bus.invoke("mcp-fs.ping", env)
                assert resp.event_type == ResponseFinal
                assert captured_meta == {
                    "session_id": "sess-1",
                    "traceparent": "tp-00-1234567890abcdef-1234567890abcdef-01",
                }
            finally:
                await bridge.stop()

    async def test_missing_session_or_traceparent_omitted_from_meta(
        self, bus: Bus
    ) -> None:
        await bus.connect()

        captured_meta: dict[str, Any] | None = None

        class MetaCapturingSession(FakeClientSession):
            async def call_tool(
                self,
                name: str,
                arguments: dict[str, Any] | None = None,
                *,
                meta: dict[str, Any] | None = None,
            ) -> Any:
                nonlocal captured_meta
                captured_meta = meta
                return FakeCallToolResult(content=[FakeTextContent(text="pong")])

        mcp_mod = _make_fake_mcp_module(tools=[FakeTool(name="ping")])
        mcp_mod.ClientSession = lambda _read, _write: MetaCapturingSession(
            [FakeTool(name="ping")], {}
        )

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setitem(sys.modules, "mcp", mcp_mod)
            monkeypatch.setitem(sys.modules, "mcp.client", mcp_mod.client)
            monkeypatch.setitem(sys.modules, "mcp.client.stdio", mcp_mod.client.stdio)

            bridge = McpToolBridge(bus, _bridge_def())
            await bridge.start()
            try:
                resp = await bus.invoke("mcp-fs.ping", {})
                assert resp.event_type == ResponseFinal
                assert captured_meta is None
            finally:
                await bridge.stop()

    async def test_real_mcp_session_passes_meta(self, bus: Bus) -> None:
        """Verify the official ClientSession.call_tool accepts meta kwarg.

        This exercises the same signature used in production, ensuring the bridge
        passes session/trace in the JSON-RPC _meta field rather than relying on
        transport-level ContextVar propagation (which does not reach the MCP
        Streamable HTTP background task).
        """
        await bus.connect()

        from mcp import ClientSession
        from mcp.types import CallToolRequestParams

        bridge = McpToolBridge(bus, _bridge_def())
        # We do not start the bridge; just validate the signature.
        params = CallToolRequestParams(name="ping", arguments={}, _meta={})
        assert params.name == "ping"
        # meta kwarg is accepted by the real call_tool signature.
        sig = ClientSession.call_tool.__code__.co_varnames
        assert "meta" in sig


# ---------------------------------------------------------------------------
# Argument validation
# ---------------------------------------------------------------------------


class TestArgumentValidation:
    async def test_missing_required_argument_rejected(
        self, patch_mcp: Any, bus: Bus
    ) -> None:
        await bus.connect()
        patch_mcp(
            tools=[
                FakeTool(
                    name="read",
                    inputSchema={
                        "type": "object",
                        "required": ["path"],
                        "properties": {"path": {"type": "string"}},
                    },
                )
            ]
        )
        bridge = McpToolBridge(bus, _bridge_def())
        await bridge.start()
        try:
            resp = await bus.invoke("mcp-fs.read", {})
            assert resp.event_type == ResponseError
            payload = resp.payload_json()
            assert payload["code"] == CodeInvalidRequest
            assert "path" in payload["message"]
        finally:
            await bridge.stop()

    async def test_schema_type_violation_rejected(
        self, patch_mcp: Any, bus: Bus
    ) -> None:
        await bus.connect()
        patch_mcp(
            tools=[
                FakeTool(
                    name="add",
                    inputSchema={
                        "type": "object",
                        "required": ["a", "b"],
                        "properties": {
                            "a": {"type": "integer"},
                            "b": {"type": "integer"},
                        },
                    },
                )
            ]
        )
        bridge = McpToolBridge(bus, _bridge_def())
        await bridge.start()
        try:
            resp = await bus.invoke("mcp-fs.add", {"a": 1, "b": "two"})
            assert resp.event_type == ResponseError
            assert resp.payload_json()["code"] == CodeInvalidRequest
        finally:
            await bridge.stop()


# ---------------------------------------------------------------------------
# Log sanitization
# ---------------------------------------------------------------------------


class TestLogSanitization:
    def test_redact_sensitive_patterns(self) -> None:
        from openagentio.bridge.mcp_tool import _sanitize_for_log

        raw = (
            "Authorization: Bearer secret-token "
            "token=abc123 api_key=shh Authorization=Basic c2hh"
        )
        sanitized = _sanitize_for_log(raw)
        assert "secret-token" not in sanitized
        assert "abc123" not in sanitized
        assert "shh" not in sanitized
        assert "c2hh" not in sanitized
        assert "Basic" not in sanitized
        assert sanitized.count("[REDACTED]") == 4

    def test_truncate_long_strings(self) -> None:
        from openagentio.bridge.mcp_tool import _sanitize_for_log

        raw = "x" * 1000
        sanitized = _sanitize_for_log(raw, max_len=100)
        assert len(sanitized) == 100
        assert sanitized.endswith("...")


# ---------------------------------------------------------------------------
# Factory / runner integration
# ---------------------------------------------------------------------------


class TestFactoryAndRunner:
    async def test_factory_registered_in_builtin_factories(self) -> None:
        assert "mcp_tool" in BUILTIN_FACTORIES
        assert BUILTIN_FACTORIES["mcp_tool"] is mcp_tool_factory

    async def test_runner_starts_mcp_bridge_from_config(
        self, patch_mcp: Any, bus: Bus
    ) -> None:
        await bus.connect()
        patch_mcp(tools=[FakeTool(name="add")])
        config = BridgeConfig.from_dict(
            {
                "version": "openagentio.bridge/v1",
                "bridges": [
                    {
                        "name": "mcp-calc",
                        "type": "mcp_tool",
                        "config": {
                            "transport": "stdio",
                            "command": "python",
                            "args": ["-m", "mcp_server_calc"],
                            "timeout": 5,
                        },
                        "mappings": {"target_prefix": "calc"},
                    }
                ],
            }
        )
        runner = BridgeRunner(bus, config, BUILTIN_FACTORIES)
        await runner.start()
        try:
            resp = await bus.invoke("calc.add", {"a": 1, "b": 2})
            assert resp.event_type == ResponseFinal
        finally:
            await runner.stop()


# ---------------------------------------------------------------------------
# Stdio integration
# ---------------------------------------------------------------------------


class TestStdioIntegration:
    async def test_stdio_bridge_invokes_local_mcp_server(self, bus: Bus) -> None:
        await bus.connect()

        server_path = str(Path(__file__).with_name("mcp_test_server.py"))
        defn = BridgeDefinition(
            name="mcp-test",
            type="mcp_tool",
            config={
                "transport": "stdio",
                "command": sys.executable,
                "args": [server_path],
                "timeout": 10,
            },
            mappings=BridgeMappings(extra={"target_prefix": "test"}),
        )
        bridge = McpToolBridge(bus, defn)
        await bridge.start()
        try:
            resp = await bus.invoke("test.echo", {"message": "hello mcp"})
            assert resp.event_type == ResponseFinal
            payload = resp.payload_json()
            assert payload["text"] == "echo: hello mcp"

            resp2 = await bus.invoke("test.add", {"a": 2, "b": 3})
            assert resp2.payload_json()["text"] == "5"
        finally:
            await bridge.stop()

        # After stop(), the bridge must have unsubscribed its handlers.
        with pytest.raises(asyncio.TimeoutError):
            await bus.invoke("test.echo", {"message": "after stop"}, WithTimeout(0.5))

    async def test_stdio_bridge_propagates_meta_via_json_rpc(self, bus: Bus) -> None:
        """Verify that session_id/traceparent reach a real stdio MCP server in _meta."""
        await bus.connect()

        server_path = str(Path(__file__).with_name("mcp_test_server.py"))
        defn = BridgeDefinition(
            name="mcp-test",
            type="mcp_tool",
            config={
                "transport": "stdio",
                "command": sys.executable,
                "args": [server_path],
                "timeout": 10,
            },
            mappings=BridgeMappings(extra={"target_prefix": "test"}),
        )
        bridge = McpToolBridge(bus, defn)
        await bridge.start()
        try:
            from openagentio.event.envelope import Envelope
            from openagentio.event.types import MessageReceived

            env = Envelope.new(MessageReceived)
            env.session_id = "sess-stdio-1"
            env.traceparent = "tp-00-abcdefabcdefabcdef-abcdefabcdefabcdef-01"

            # Pass the envelope as the payload so session_id/traceparent are
            # preserved on the request. capture_meta does not need arguments.
            resp = await bus.invoke("test.capture_meta", env)
            assert resp.event_type == ResponseFinal
            payload = resp.payload_json()
            captured = json.loads(payload["text"])
            assert captured == {
                "session_id": "sess-stdio-1",
                "traceparent": "tp-00-abcdefabcdefabcdef-abcdefabcdefabcdef-01",
            }
        finally:
            await bridge.stop()


class TestLazyImport:
    async def test_import_error_when_mcp_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Remove any cached fake mcp module.
        for name in list(sys.modules):
            if name == "mcp" or name.startswith("mcp."):
                monkeypatch.delitem(sys.modules, name, raising=False)

        import builtins

        real_import = builtins.__import__

        def _guard_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "mcp":
                raise ImportError("No module named 'mcp'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr("builtins.__import__", _guard_import)
        with pytest.raises(ImportError, match=r"pip install 'openagentio\[mcp\]'"):
            _import_mcp()

    async def test_bridge_module_imports_without_mcp(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Importing the bridge subpackage should not require mcp.
        for name in list(sys.modules):
            if name == "mcp" or name.startswith("mcp."):
                monkeypatch.delitem(sys.modules, name, raising=False)

        import builtins

        real_import = builtins.__import__

        def _guard_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "mcp":
                raise ImportError("No module named 'mcp'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr("builtins.__import__", _guard_import)

        from openagentio.bridge import BUILTIN_FACTORIES

        assert "mcp_tool" in BUILTIN_FACTORIES
