"""OpenClaw Chat SSE bridge.

A :class:`Bridge` implementation that connects a single Bus target to
OpenClaw Gateway's OpenAI-compatible ``POST /v1/chat/completions``
endpoint with ``stream: true``.

On each incoming ``stream_invoke`` request:

1. The Envelope payload is mapped to a Chat Completions request:
   * ``payload[<text_field>]`` -> ``messages[0].content``
   * ``env.session_id``        -> ``x-openclaw-session-key`` header
   * ``metadata["openclaw.*"]`` -> top-level request body params

2. The response is consumed as SSE and translated to Bus stream frames:
   * ``choices[0].delta.content`` -> ``writer.delta({"delta": content})``
   * ``data: [DONE]`` or stream end -> ``writer.final({"text": accumulated})``

3. HTTP / transport / timeout errors are translated into the appropriate
   :class:`BusError` subclass.

This bridge intentionally supports only the streaming Chat Completions
path; non-streaming ``bus.invoke()`` and other endpoints are out of scope
for the first version.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import AsyncIterable
from typing import TYPE_CHECKING, Any

from openagentio.bridge.base import Bridge
from openagentio.bridge.config import BridgeDefinition, BridgeMappings
from openagentio.bridge.sse_parser import parse_sse
from openagentio.bus.errors import (
    AgentTimeoutError,
    AgentUnavailableError,
    AuthFailureError,
    InvalidRequestError,
    TransportFailureError,
)

if TYPE_CHECKING:  # pragma: no cover
    import httpx

    from openagentio.bus import Bus
    from openagentio.bus.stream import StreamWriter
    from openagentio.event.envelope import Envelope
    from openagentio.transport.base import Subscription


def _import_httpx() -> Any:
    """Lazily import httpx so the bridge extra remains optional.

    Importing ``openagentio.bridge`` does not require httpx; the dependency is
    only needed when an ``OpenClawChatSSEBridge`` is actually constructed.
    """
    try:
        import httpx
    except ImportError as exc:
        raise ImportError(
            "OpenClawChatSSEBridge requires httpx. "
            "Install the optional bridge extra: pip install 'openagentio[bridge]'"
        ) from exc
    return httpx


# Default mapping values, documented in prompts/openclaw_mapping.md.
_DEFAULT_TEXT_FIELD = "text"
_DEFAULT_SESSION_HEADER = "x-openclaw-session-key"
_DEFAULT_METADATA_PREFIX = "openclaw."

# Request body keys managed by the bridge itself. Payload passthrough and
# openclaw.* metadata mapping are not allowed to override these fields,
# preserving the SSE-only contract and message mapping.
_RESERVED_BODY_KEYS = frozenset({"model", "messages", "stream", "user"})

# Matches ``${VAR}`` or ``${VAR:-default}``.
_ENV_PLACEHOLDER_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def _resolve_config_value(value: Any, source_name: str) -> Any:
    """Replace ``${ENV}`` placeholders in config strings from ``os.environ``.

    A default value may be supplied with ``${ENV:-default}``. Non-string
    values are returned unchanged. Missing required variables raise
    :class:`ValueError`.
    """
    if not isinstance(value, str):
        return value

    def _repl(match: "re.Match[str]") -> str:
        var = match.group(1)
        default = match.group(2)
        resolved = os.environ.get(var)
        if resolved is None:
            if default is not None:
                return default
            raise ValueError(
                f"bridge '{source_name}': config references unset environment "
                f"variable {var!r}"
            )
        return resolved

    return _ENV_PLACEHOLDER_RE.sub(_repl, value)


class OpenClawChatSSEBridge(Bridge):
    """Bridges one Bus target to OpenClaw Gateway via Chat Completions SSE."""

    def __init__(
        self,
        bus: "Bus",
        definition: BridgeDefinition,
        *,
        client: httpx.AsyncClient | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._bus = bus
        self._definition = definition
        self._logger = logger or logging.getLogger(
            f"openagentio.bridge.openclaw_chat_sse.{definition.name}"
        )

        cfg = dict(definition.config)

        self._base_url = _resolve_config_value(
            self._require_string(cfg, "base_url"), definition.name
        )
        if not self._base_url:
            raise ValueError(
                f"bridge '{definition.name}': config 'base_url' is required"
            )
        # Ensure the base URL points at the /v1 chat completions endpoint.
        self._chat_url = self._base_url.rstrip("/") + "/chat/completions"

        token = _resolve_config_value(
            self._require_string(cfg, "token"), definition.name
        )
        if not token:
            raise ValueError(
                f"bridge '{definition.name}': config 'token' is required"
            )
        self._auth_header = f"Bearer {token}"

        self._model = _resolve_config_value(
            self._require_string(cfg, "model"), definition.name
        )
        if not self._model:
            raise ValueError(
                f"bridge '{definition.name}': config 'model' is required"
            )

        self._request_timeout = float(cfg.get("request_timeout", 60.0))
        if self._request_timeout <= 0:
            raise ValueError(
                f"bridge '{definition.name}': config 'request_timeout' must be "
                "positive"
            )

        mappings = definition.mappings
        self._text_field = mappings.text_field or _DEFAULT_TEXT_FIELD
        self._session_header = (
            mappings.session_field or _DEFAULT_SESSION_HEADER
        )
        self._metadata_prefix = (
            mappings.metadata_prefix or _DEFAULT_METADATA_PREFIX
        )

        # Created on start() so the bridge can be constructed without
        # opening network resources. Tests may inject a pre-configured client
        # (e.g. with ASGI transport) via the ``client`` argument.
        self._client: httpx.AsyncClient | None = client
        self._own_client = client is None
        self._stream_sub: "Subscription | None" = None
        # Populated in start() via lazy import of httpx.
        self._httpx: Any = None

    @staticmethod
    def _require_string(cfg: dict[str, Any], key: str) -> str:
        value = cfg.get(key, "")
        return str(value) if isinstance(value, str) else ""

    # -- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        httpx = _import_httpx()
        self._httpx = httpx
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self._request_timeout),
            )
            self._own_client = True
        try:
            self._stream_sub = await self._bus.handle_stream(
                self._definition.name, self._on_stream
            )
        except Exception:
            # Roll back the httpx client we just created — otherwise a failed
            # start() leaks it on non-BridgeRunner callers.
            await self.stop()
            raise
        self._logger.info(
            "openclaw chat sse bridge ready: target=%s url=%s model=%s",
            self._definition.name,
            self._chat_url,
            self._model,
        )

    async def stop(self) -> None:
        sub = self._stream_sub
        self._stream_sub = None
        if sub is not None:
            try:
                await sub.unsubscribe()
            except Exception:  # noqa: BLE001 - best-effort cleanup
                self._logger.exception(
                    "openclaw chat sse bridge: failed to unsubscribe bus handler"
                )
        client = self._client
        self._client = None
        if client is not None and self._own_client:
            try:
                await client.aclose()
            except Exception:  # noqa: BLE001 - best-effort cleanup
                self._logger.exception(
                    "openclaw chat sse bridge: failed to close HTTP client"
                )

    # -- request handler ----------------------------------------------------

    async def _on_stream(self, env: "Envelope", writer: "StreamWriter") -> None:
        """Bus stream handler: translate to OpenAI Chat Completions and
        stream the SSE response back as Bus delta/final frames."""
        request_body = self._build_request_body(env)
        headers = self._build_headers(env)

        client = self._client
        if client is None:
            raise TransportFailureError(
                f"bridge '{self._definition.name}': bridge is not started"
            )

        try:
            async with client.stream(
                "POST",
                self._chat_url,
                json=request_body,
                headers=headers,
            ) as response:
                if response.status_code >= 500:
                    body = await _safe_read_response_text(response)
                    raise TransportFailureError(
                        _format_http_error(
                            self._definition.name, response, body, "server error"
                        )
                    )
                if response.status_code in (401, 403):
                    body = await _safe_read_response_text(response)
                    raise AuthFailureError(
                        _format_http_error(
                            self._definition.name, response, body, "auth failure"
                        )
                    )
                if response.status_code >= 400:
                    body = await _safe_read_response_text(response)
                    raise InvalidRequestError(
                        _format_http_error(
                            self._definition.name, response, body, "client error"
                        )
                    )

                await self._stream_response(response, writer)
        except self._httpx.TimeoutException as exc:
            raise AgentTimeoutError(
                f"bridge '{self._definition.name}': request timeout"
            ) from exc
        except self._httpx.ConnectError as exc:
            raise TransportFailureError(
                f"bridge '{self._definition.name}': connection failure: {exc}"
            ) from exc
        except self._httpx.NetworkError as exc:
            raise TransportFailureError(
                f"bridge '{self._definition.name}': network error: {exc}"
            ) from exc
        except self._httpx.HTTPError as exc:
            raise TransportFailureError(
                f"bridge '{self._definition.name}': HTTP error: {exc}"
            ) from exc

    # -- request body building -----------------------------------------------

    def _build_request_body(self, env: "Envelope") -> dict[str, Any]:
        """Build an OpenAI-compatible Chat Completions request body."""
        payload = env.payload_json() or {}
        if not isinstance(payload, dict):
            raise InvalidRequestError(
                f"bridge '{self._definition.name}': payload must be a JSON object"
            )

        text = payload.get(self._text_field)
        if text is None:
            raise InvalidRequestError(
                f"bridge '{self._definition.name}': payload missing "
                f"'{self._text_field}' field"
            )

        messages = [{"role": "user", "content": str(text)}]
        body: dict[str, Any] = {
            "model": self._model,
            "messages": messages,
            "stream": True,
        }

        # Optional explicit session routing. We intentionally keep this
        # separate from OpenAI's ``user`` field; some deployments prefer
        # one or the other. Setting both is harmless for OpenClaw.
        if env.session_id:
            body["user"] = env.session_id

        # Merge any openclaw.* metadata keys into the request body, stripping
        # the prefix. Business context like channel_type / target_user flows
        # through without manual copying. Reserved protocol fields are never
        # overwritten.
        if env.metadata:
            prefix = self._metadata_prefix
            for key, value in env.metadata.items():
                if not isinstance(key, str):
                    continue
                if key.startswith("acp."):
                    continue
                if not key.startswith(prefix):
                    continue
                stripped = key[len(prefix):]
                if not stripped or stripped in _RESERVED_BODY_KEYS:
                    continue
                body.setdefault(stripped, value)

        # Payload keys (other than the text field) also pass through,
        # letting callers override metadata-derived values explicitly.
        # Reserved protocol fields are protected to keep the SSE contract.
        for key, value in payload.items():
            if key == self._text_field:
                continue
            if key in _RESERVED_BODY_KEYS:
                continue
            body[key] = value

        return body

    def _build_headers(self, env: "Envelope") -> dict[str, str]:
        """Add per-request headers, notably ``x-openclaw-session-key``."""
        headers: dict[str, str] = {"Authorization": self._auth_header}
        if env.session_id and self._session_header:
            headers[self._session_header] = env.session_id
        return headers

    # -- response streaming --------------------------------------------------

    async def _stream_response(
        self, response: httpx.Response, writer: "StreamWriter"
    ) -> None:
        """Consume the SSE response and emit delta/final frames."""
        accumulated = ""
        last_chunk: dict[str, Any] | None = None

        async for payload in parse_sse(response.aiter_text()):
            if payload == "[DONE]":
                break
            if not payload:
                continue
            try:
                chunk = json.loads(payload)
            except json.JSONDecodeError as exc:
                self._logger.warning(
                    "openclaw chat sse bridge: failed to decode SSE data: %s", exc
                )
                continue

            # OpenAI/OpenClaw may emit errors mid-stream as
            # ``{"error": {"message": ..., "code": ...}}``. Treat these as
            # failures rather than successful finals.
            if isinstance(chunk, dict) and "error" in chunk:
                raise _map_sse_error(self._definition.name, chunk)

            last_chunk = chunk
            content = _extract_delta_content(chunk)
            if content:
                accumulated += content
                try:
                    await writer.delta({"delta": content})
                except Exception:  # noqa: BLE001
                    self._logger.exception(
                        "openclaw chat sse bridge: writer.delta failed"
                    )

        await writer.final({"text": accumulated, "raw": last_chunk})


# ---------------------------------------------------------------------------
# User-facing convenience wrapper
# ---------------------------------------------------------------------------

class OpenClawChatBridge:
    """Small convenience wrapper for OpenClaw Gateway chat integration.

    This keeps the low-level ``BridgeDefinition`` API available for bridge
    authors, while giving application users a short path:

        bridge = OpenClawChatBridge.from_env(bus)
        await bridge.start()
    """

    def __init__(
        self,
        bus: "Bus",
        *,
        target: str = "openclaw.chat",
        base_url: str,
        token: str,
        model: str = "openclaw/default",
        request_timeout: float = 60.0,
    ) -> None:
        self.target = target
        definition = BridgeDefinition(
            name=target,
            type="openclaw_chat_sse",
            config={
                "base_url": base_url,
                "token": token,
                "model": model,
                "request_timeout": request_timeout,
            },
            mappings=BridgeMappings(
                text_field=_DEFAULT_TEXT_FIELD,
                session_field=_DEFAULT_SESSION_HEADER,
                metadata_prefix=_DEFAULT_METADATA_PREFIX,
            ),
        )
        self._bridge = OpenClawChatSSEBridge(bus, definition)

    @classmethod
    def from_env(
        cls,
        bus: "Bus",
        *,
        target: str = "openclaw.chat",
        base_url_env: str = "OPENCLAW_GATEWAY_BASE_URL",
        token_env: str = "OPENCLAW_GATEWAY_TOKEN",
        model_env: str = "OPENCLAW_GATEWAY_MODEL",
        timeout_env: str = "OPENCLAW_REQUEST_TIMEOUT",
    ) -> "OpenClawChatBridge":
        base_url = os.environ.get(base_url_env, "http://localhost:18789/v1")
        token = os.environ.get(token_env, "")
        if not token:
            raise ValueError(f"{token_env} is required")
        return cls(
            bus,
            target=target,
            base_url=base_url,
            token=token,
            model=os.environ.get(model_env, "openclaw/default"),
            request_timeout=float(os.environ.get(timeout_env, "60")),
        )

    async def start(self) -> None:
        await self._bridge.start()

    async def stop(self) -> None:
        await self._bridge.stop()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_delta_content(chunk: dict[str, Any]) -> str:
    """Extract ``choices[0].delta.content`` from a Chat Completions chunk."""
    if not isinstance(chunk, dict):
        return ""
    choices = chunk.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    delta = choices[0].get("delta") if isinstance(choices[0], dict) else None
    if not isinstance(delta, dict):
        return ""
    content = delta.get("content")
    return str(content) if content is not None else ""


def _map_sse_error(
    bridge_name: str, chunk: dict[str, Any]
) -> AgentUnavailableError | AuthFailureError | InvalidRequestError:
    """Map an OpenAI/OpenClaw-style SSE error chunk to a :class:`BusError`.

    OpenAI streaming errors look like ``{"error": {"message": ..., "code": ...}}``.
    We classify based on the error code / type / message text so the Bus emits
    the correct ``agent.response.error`` code.
    """
    error = chunk.get("error") if isinstance(chunk, dict) else None
    if not isinstance(error, dict):
        error = {"message": str(error)} if error is not None else {"message": str(chunk)}

    message = str(error.get("message", ""))
    code = str(error.get("code", ""))
    error_type = str(error.get("type", ""))

    parts = [code, error_type, message]
    text = " ".join(p for p in parts if p).lower()

    detail = message or str(chunk)
    if code:
        detail = f"[{code}] {detail}"
    full = f"bridge '{bridge_name}': SSE error {detail}"

    if any(k in text for k in ("auth", "permission", "unauthorized", "forbidden")):
        return AuthFailureError(full)
    if any(
        k in text
        for k in ("invalid", "validation", "bad_request", "context_length")
    ):
        return InvalidRequestError(full)
    return AgentUnavailableError(full)


async def _safe_read_response_text(response: httpx.Response) -> str:
    """Best-effort read of a (usually small) error response body."""
    try:
        data = await response.aread()
        return data.decode("utf-8", errors="replace") if data else ""
    except Exception:  # noqa: BLE001
        return ""


def _format_http_error(
    bridge_name: str,
    response: httpx.Response,
    body: str,
    category: str,
) -> str:
    """Build a concise error message including the status and body snippet."""
    snippet = body[:200].replace("\n", " ")
    return (
        f"bridge '{bridge_name}': {category}: HTTP {response.status_code} "
        f"{response.reason_phrase}: {snippet}"
    )


# ---------------------------------------------------------------------------
# Factory entry point
# ---------------------------------------------------------------------------

def openclaw_chat_sse_factory(
    bus: "Bus", definition: BridgeDefinition
) -> Bridge:
    """:class:`BridgeFactory` for ``type: "openclaw_chat_sse"`` entries."""
    return OpenClawChatSSEBridge(bus, definition)


__all__ = [
    "OpenClawChatBridge",
    "OpenClawChatSSEBridge",
    "openclaw_chat_sse_factory",
]
