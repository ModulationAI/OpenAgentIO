"""MCP tool bridge.

Exposes tools from an external Model Context Protocol (MCP) server as native
OpenAgentIO Bus invoke targets. Supports both stdio (local subprocess) and
Streamable HTTP (remote endpoint) transports.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Any

from openagentio.bridge.base import Bridge
from openagentio.bridge.config import BridgeConfigError, BridgeDefinition
from openagentio.bus.errors import (
    AgentTimeoutError,
    AgentUnavailableError,
    AuthFailureError,
    BusError,
    InvalidRequestError,
    TransportFailureError,
)

if TYPE_CHECKING:  # pragma: no cover
    from mcp import ClientSession, StdioServerParameters
    from mcp.types import CallToolResult

    from openagentio.bus import Bus
    from openagentio.event.envelope import Envelope


def _import_mcp() -> Any:
    """Lazily import the official MCP Python SDK.

    The ``mcp`` package is an optional dependency. Importing this module does
    not require it; the dependency is only needed when an ``McpToolBridge`` is
    actually constructed and started.
    """
    try:
        import mcp
    except ImportError as exc:
        raise ImportError(
            "McpToolBridge requires the 'mcp' package. "
            "Install the optional extra: pip install 'openagentio[mcp]'"
        ) from exc
    return mcp


def _require_string(config: dict[str, Any], key: str, bridge_name: str) -> str:
    """Extract a required string value from bridge config."""
    value = config.get(key)
    if not isinstance(value, str) or not value:
        raise BridgeConfigError(
            f"bridge '{bridge_name}': config '{key}' is required and must be a non-empty string"
        )
    return value


def _validate_args(value: Any, bridge_name: str) -> list[str]:
    """Validate that ``args`` is a list of strings."""
    if value is None:
        return []
    if not isinstance(value, list):
        raise BridgeConfigError(
            f"bridge '{bridge_name}': config 'args' must be a list of strings, "
            f"got {type(value).__name__}"
        )
    for i, item in enumerate(value):
        if not isinstance(item, str):
            raise BridgeConfigError(
                f"bridge '{bridge_name}': config 'args[{i}]' must be a string, "
                f"got {type(item).__name__}"
            )
    return list(value)


def _validate_env(value: Any, bridge_name: str) -> dict[str, str]:
    """Validate that ``env`` is a mapping of strings to strings."""
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise BridgeConfigError(
            f"bridge '{bridge_name}': config 'env' must be a mapping of strings, "
            f"got {type(value).__name__}"
        )
    result: dict[str, str] = {}
    for key, val in value.items():
        if not isinstance(key, str):
            raise BridgeConfigError(
                f"bridge '{bridge_name}': config 'env' keys must be strings, "
                f"got {type(key).__name__}"
            )
        if not isinstance(val, str):
            raise BridgeConfigError(
                f"bridge '{bridge_name}': config 'env' value for key {key!r} "
                f"must be a string, got {type(val).__name__}"
            )
        result[key] = val
    return result


def _validate_http_headers(value: Any, bridge_name: str) -> dict[str, str]:
    """Validate that ``headers`` is a mapping of strings to strings."""
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise BridgeConfigError(
            f"bridge '{bridge_name}': config 'headers' must be a mapping of strings, "
            f"got {type(value).__name__}"
        )
    result: dict[str, str] = {}
    for key, val in value.items():
        if not isinstance(key, str):
            raise BridgeConfigError(
                f"bridge '{bridge_name}': config 'headers' keys must be strings, "
                f"got {type(key).__name__}"
            )
        if not isinstance(val, str):
            raise BridgeConfigError(
                f"bridge '{bridge_name}': config 'headers' value for key {key!r} "
                f"must be a string, got {type(val).__name__}"
            )
        result[key] = val
    return result


def _map_mcp_error(bridge_name: str, exc: BaseException) -> BusError:
    """Map an MCP SDK or transport exception to a BusError subclass.

    The mapping prioritizes deterministic codes so callers receive consistent
    ACP error responses:

    * ``INVALID_PARAMS`` -> ``InvalidRequestError``
    * ``asyncio.TimeoutError`` -> ``AgentTimeoutError``
    * HTTP 401/403 -> ``AuthFailureError``
    * connection / init failures -> ``AgentUnavailableError``
    * other MCP protocol or HTTP errors -> ``TransportFailureError``

    ``ExceptionGroup`` raised by the MCP streamable HTTP task group is unpacked
    recursively so the original httpx/MCP error can be mapped.
    """
    mapped = _try_map_exception(bridge_name, exc)
    if mapped is not None:
        return mapped

    return TransportFailureError(
        f"mcp bridge '{bridge_name}': unexpected failure: {exc}"
    )


def _try_map_exception(bridge_name: str, exc: BaseException) -> BusError | None:
    """Best-effort mapping of a single exception, including ExceptionGroup."""
    if isinstance(exc, asyncio.TimeoutError):
        return AgentTimeoutError(
            f"mcp bridge '{bridge_name}': operation timed out"
        )

    # Handle httpx HTTP errors for streamable_http transport.
    try:
        import httpx
    except ImportError:
        httpx = None  # type: ignore[assignment]

    if httpx is not None:
        if isinstance(exc, httpx.HTTPStatusError):
            status = exc.response.status_code
            if status in (401, 403):
                return AuthFailureError(
                    f"mcp bridge '{bridge_name}': authentication failed: {status}"
                )
            if status >= 500:
                return TransportFailureError(
                    f"mcp bridge '{bridge_name}': server error: {status}"
                )
            return InvalidRequestError(
                f"mcp bridge '{bridge_name}': HTTP error: {status}"
            )
        if isinstance(exc, (httpx.ConnectError, httpx.NetworkError)):
            return AgentUnavailableError(
                f"mcp bridge '{bridge_name}': HTTP transport unavailable: {exc}"
            )
        if isinstance(exc, httpx.TimeoutException):
            return AgentTimeoutError(
                f"mcp bridge '{bridge_name}': HTTP request timed out"
            )

    mcp = _import_mcp()
    if isinstance(exc, mcp.McpError):
        code = exc.error.code
        message = exc.error.message
        if code == mcp.types.INVALID_PARAMS:
            return InvalidRequestError(
                f"mcp bridge '{bridge_name}': invalid tool arguments: {message}"
            )
        if code in (mcp.types.METHOD_NOT_FOUND, mcp.types.INVALID_REQUEST):
            return AgentUnavailableError(
                f"mcp bridge '{bridge_name}': server error: {message}"
            )
        return TransportFailureError(
            f"mcp bridge '{bridge_name}': protocol error: {message}"
        )

    if isinstance(exc, (OSError, ConnectionError)):
        return AgentUnavailableError(
            f"mcp bridge '{bridge_name}': transport unavailable: {exc}"
        )

    # Unpack ExceptionGroup from MCP streamable HTTP task group.
    if isinstance(exc, BaseExceptionGroup):
        for sub in exc.exceptions:
            mapped = _try_map_exception(bridge_name, sub)
            if mapped is not None:
                return mapped

    # When MCP streamable HTTP cancels the task group, the HTTPStatusError may
    # live in the cancel exception's context/cause rather than be raised directly.
    for related in (getattr(exc, "__cause__", None), getattr(exc, "__context__", None)):
        if related is not None and related is not exc:
            mapped = _try_map_exception(bridge_name, related)
            if mapped is not None:
                return mapped

    return None


def _truncate(value: str, max_len: int = 500) -> str:
    """Truncate a string to ``max_len`` characters with an ellipsis."""
    if len(value) <= max_len:
        return value
    return value[: max_len - 3] + "..."


# Patterns that commonly carry secrets in logs. Values are redacted to a
# fixed placeholder to avoid leaking credentials.
_SECRET_PATTERNS = (
    # "Authorization: Basic xxx", "Authorization=Bearer xxx", etc.
    # Matches the scheme + credential as a single secret.
    re.compile(
        r"(?i)(authorization\s*[:=]\s*)[^\s&]+(?:\s+[^\s&]+)?", re.IGNORECASE
    ),
    # Standalone "Bearer <token>"
    re.compile(r"(?i)(bearer\s+)[^\s&]+", re.IGNORECASE),
    re.compile(r"(?i)(token\s*[:=]\s*)[^\s&]+", re.IGNORECASE),
    re.compile(r"(?i)(api[_-]?key\s*[:=]\s*)[^\s&]+", re.IGNORECASE),
)


def _redact_sensitive(value: str) -> str:
    """Redact common secret-bearing patterns from log strings."""
    for pattern in _SECRET_PATTERNS:
        value = pattern.sub(r"\1[REDACTED]", value)
    return value


def _sanitize_for_log(value: str, max_len: int = 500) -> str:
    """Truncate and redact a string before writing it to logs."""
    return _redact_sensitive(_truncate(value, max_len))


class McpToolBridge(Bridge):
    """Bridge that exposes MCP server tools as Bus invoke targets.

    On ``start()`` the bridge connects to the configured MCP server, performs
    the initialization handshake, discovers the available tools, and registers
    one invoke handler per tool on the supplied Bus.

    Two transports are supported:

    * ``stdio``: spawn a local MCP server subprocess (the default).
    * ``streamable_http``: connect to a remote MCP endpoint over HTTP.

    The target namespace is controlled by ``target_prefix``. A tool named
    ``read_file`` becomes ``mcp-fs.read_file`` when ``target_prefix`` is
    ``mcp-fs``.
    """

    def __init__(
        self,
        bus: "Bus",
        definition: BridgeDefinition,
        *,
        http_client: Any | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._bus = bus
        self._definition = definition
        self._logger = logger or logging.getLogger(
            f"openagentio.bridge.mcp_tool.{definition.name}"
        )

        cfg = dict(definition.config)
        transport = str(cfg.get("transport", "stdio")).lower()
        if transport not in ("stdio", "streamable_http"):
            raise BridgeConfigError(
                f"bridge '{definition.name}': unsupported transport {transport!r}; "
                "supported transports are 'stdio' and 'streamable_http'"
            )
        self._transport_type = transport

        if transport == "stdio":
            self._command = _require_string(cfg, "command", definition.name)
            self._args = _validate_args(cfg.get("args"), definition.name)
            self._env = _validate_env(cfg.get("env"), definition.name)
            self._http_url = ""
            self._http_headers: dict[str, str] = {}
            self._http_client = None
        else:
            self._command = ""
            self._args: list[str] = []
            self._env: dict[str, str] = {}
            self._http_url = _require_string(cfg, "http_url", definition.name)
            self._http_headers = _validate_http_headers(
                cfg.get("headers", {}), definition.name
            )
            token = cfg.get("token")
            if token:
                if not isinstance(token, str) or not token:
                    raise BridgeConfigError(
                        f"bridge '{definition.name}': config 'token' must be a non-empty string"
                    )
                self._http_headers["Authorization"] = f"Bearer {token}"
            self._http_client = http_client

        timeout = cfg.get("timeout", 10)
        try:
            self._timeout = float(timeout)
        except (TypeError, ValueError) as exc:
            raise BridgeConfigError(
                f"bridge '{definition.name}': config 'timeout' must be a number"
            ) from exc
        if self._timeout <= 0:
            raise BridgeConfigError(
                f"bridge '{definition.name}': config 'timeout' must be positive"
            )

        # target_prefix: prefer mappings.extra, then definition.name.
        prefix = definition.mappings.extra.get("target_prefix")
        if not isinstance(prefix, str) or not prefix:
            prefix = definition.name
        self._target_prefix = prefix

        # Runtime state.
        self._exit_stack: AsyncExitStack | None = None
        self._session: "ClientSession" | None = None
        self._subscriptions: list[Any] = []

    async def start(self) -> None:
        """Connect to the MCP server, discover tools, and register bus targets."""
        mcp = _import_mcp()

        self._exit_stack = AsyncExitStack()
        try:
            if self._transport_type == "stdio":
                from mcp.client.stdio import stdio_client

                params = mcp.StdioServerParameters(
                    command=self._command,
                    args=self._args,
                    env=self._env if self._env else None,
                )
                read, write = await self._exit_stack.enter_async_context(
                    stdio_client(params)
                )
            else:
                from mcp.client.streamable_http import streamable_http_client
                import httpx

                if self._http_client is not None:
                    client = self._http_client
                    own_client = False
                else:
                    client = httpx.AsyncClient(
                        timeout=httpx.Timeout(self._timeout),
                        headers=self._http_headers,
                        trust_env=False,
                    )
                    own_client = True
                    await self._exit_stack.enter_async_context(client)
                read, write, _ = await self._exit_stack.enter_async_context(
                    streamable_http_client(self._http_url, http_client=client)
                )

            self._session = await self._exit_stack.enter_async_context(
                mcp.ClientSession(read, write)
            )
            await self._session.initialize()

            result = await self._session.list_tools()
            self._logger.info(
                "mcp bridge '%s' discovered %d tool(s)",
                self._definition.name,
                len(result.tools),
            )

            for tool in result.tools:
                target_name = f"{self._target_prefix}.{tool.name}"
                sub = await self._bus.handle_invoke(
                    target_name,
                    self._make_handler(tool),
                )
                self._subscriptions.append(sub)
                self._logger.debug(
                    "mcp bridge '%s' registered target %r",
                    self._definition.name,
                    target_name,
                )
        except BridgeConfigError:
            raise
        except asyncio.CancelledError as exc:
            # Streamable HTTP cancels the anyio task group on transport errors.
            # The original HTTPStatusError surfaces during cleanup, so prefer
            # the cleanup exception for mapping when available.
            cleanup_exc = await self._cleanup()
            mapped = _map_mcp_error(
                self._definition.name,
                cleanup_exc if cleanup_exc is not None else exc,
            )
            raise mapped from exc
        except Exception as exc:
            # Best-effort cleanup on startup failure so the runner can stop()
            # safely and BridgeRunner.start() can raise a clean exception.
            await self._cleanup()
            mapped = _map_mcp_error(self._definition.name, exc)
            raise mapped from exc

    async def stop(self) -> None:
        """Unregister bus handlers and release the MCP transport."""
        await self._cleanup()

    async def _cleanup(self) -> BaseException | None:
        """Idempotent cleanup of subscriptions and exit stack.

        Returns the last exception encountered during cleanup, if any, so callers
        can inspect it for richer error mapping (e.g. streamable HTTP task group
        failures wrapping an HTTPStatusError).
        """
        last_exc: BaseException | None = None
        while self._subscriptions:
            sub = self._subscriptions.pop()
            try:
                await sub.unsubscribe()
            except Exception as exc:
                self._logger.exception(
                    "mcp bridge '%s': failed to unsubscribe bus handler",
                    self._definition.name,
                )
                last_exc = exc

        self._session = None
        stack = self._exit_stack
        self._exit_stack = None
        if stack is not None:
            try:
                await stack.aclose()
            except BaseException as exc:
                self._logger.exception(
                    "mcp bridge '%s': error closing transport context",
                    self._definition.name,
                )
                last_exc = exc

        return last_exc

    def _make_handler(self, tool: Any):
        """Return an invoke handler for a specific MCP tool."""
        tool_name = tool.name
        input_schema = getattr(tool, "inputSchema", None) or {}

        async def handler(env: "Envelope") -> dict[str, Any]:
            session = self._session
            if session is None:
                raise AgentUnavailableError(
                    f"mcp bridge '{self._definition.name}': session not initialized"
                )

            payload = env.payload_json()
            if payload is None:
                arguments: dict[str, Any] = {}
            elif isinstance(payload, dict):
                arguments = payload
            else:
                raise InvalidRequestError(
                    f"mcp bridge '{self._definition.name}': "
                    "tool arguments must be a JSON object"
                )

            # Validate arguments against the tool's JSON schema when available.
            self._validate_arguments(tool_name, input_schema, arguments)

            # Propagate session/trace context into logs for observability.
            self._logger.debug(
                "mcp bridge '%s' calling tool %r for session=%s traceparent=%s",
                self._definition.name,
                tool_name,
                env.session_id,
                env.traceparent,
            )

            # Per-invoke session/trace context travels in the JSON-RPC _meta field.
            # This works for both stdio and streamable_http, because it is carried
            # by the MCP message itself rather than transport-specific headers.
            # The official MCP SDK does not expose per-request HTTP headers for
            # streamable_http; the HTTP request is issued by a background task
            # spawned during start(), so a ContextVar set in the handler would not
            # propagate to that task.
            meta: dict[str, Any] = {}
            if env.session_id:
                meta["session_id"] = env.session_id
            if env.traceparent:
                meta["traceparent"] = env.traceparent

            try:
                result: "CallToolResult" = await asyncio.wait_for(
                    session.call_tool(tool_name, arguments, meta=meta if meta else None),
                    timeout=self._timeout,
                )
            except asyncio.TimeoutError as exc:
                raise AgentTimeoutError(
                    f"mcp bridge '{self._definition.name}': tool call timed out"
                ) from exc
            except BusError:
                raise
            except Exception as exc:
                raise _map_mcp_error(self._definition.name, exc) from exc

            if result.isError:
                message = _sanitize_for_log(self._extract_error_message(result))
                raise InvalidRequestError(
                    f"mcp bridge '{self._definition.name}': tool error: {message}"
                )

            return self._map_result(result)

        return handler

    def _validate_arguments(
        self,
        tool_name: str,
        input_schema: dict[str, Any],
        arguments: dict[str, Any],
    ) -> None:
        """Validate tool arguments against the tool's inputSchema.

        If ``jsonschema`` is not installed, only a shallow required-fields check
        is performed. A missing required field or schema violation raises
        :class:`InvalidRequestError`.
        """
        if not isinstance(input_schema, dict) or not input_schema:
            return

        required = input_schema.get("required")
        if isinstance(required, list):
            missing = [name for name in required if name not in arguments]
            if missing:
                raise InvalidRequestError(
                    f"mcp bridge '{self._definition.name}': tool '{tool_name}' "
                    f"missing required argument(s): {', '.join(missing)}"
                )

        try:
            import jsonschema
        except ImportError:
            self._logger.debug(
                "mcp bridge '%s': jsonschema not installed; skipping full "
                "schema validation for tool '%s'",
                self._definition.name,
                tool_name,
            )
            return

        try:
            jsonschema.validate(instance=arguments, schema=input_schema)
        except jsonschema.ValidationError as exc:
            raise InvalidRequestError(
                f"mcp bridge '{self._definition.name}': tool '{tool_name}' "
                f"argument validation failed: {exc.message}"
            ) from exc

    def _map_result(self, result: "CallToolResult") -> dict[str, Any]:
        """Map a successful CallToolResult to an OpenAgentIO response payload."""
        content_items: list[dict[str, Any]] = []
        for item in result.content:
            data: dict[str, Any]
            if item.type == "text":
                data = {"type": "text", "text": item.text}
            elif item.type == "image":
                data = {
                    "type": "image",
                    "mimeType": getattr(item, "mimeType", ""),
                    "data": getattr(item, "data", ""),
                }
            elif item.type == "resource":
                resource = getattr(item, "resource", None)
                data = {
                    "type": "resource",
                    "resource": resource.model_dump() if resource is not None else None,
                }
            else:
                data = {"type": item.type, "data": item.model_dump()}
            content_items.append(data)

        response: dict[str, Any] = {"content": content_items}

        # Use model_dump() to read structuredContent and meta. meta has the alias
        # _meta in the MCP SDK, so direct attribute access can return None even
        # when the field was populated by name.
        raw = result.model_dump()
        if raw.get("structuredContent") is not None:
            response["structuredContent"] = raw["structuredContent"]
        if raw.get("meta") is not None:
            response["meta"] = raw["meta"]

        # Convenience: expose a single text result at the top level when the
        # mapping's text_field is the default "text".
        if (
            len(content_items) == 1
            and content_items[0]["type"] == "text"
            and self._definition.mappings.text_field == "text"
        ):
            response["text"] = content_items[0]["text"]

        return response

    def _extract_error_message(self, result: "CallToolResult") -> str:
        """Best-effort extraction of an error message from an isError result."""
        texts: list[str] = []
        for item in result.content:
            if item.type == "text":
                texts.append(str(item.text))
        message = " ".join(texts).strip()
        return message or "unknown tool error"


def mcp_tool_factory(bus: "Bus", definition: BridgeDefinition) -> Bridge:
    """Factory used by BridgeRunner to construct an McpToolBridge."""
    return McpToolBridge(bus, definition)
