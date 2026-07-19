"""QwenPaw Chat SSE bridge.

A :class:`Bridge` implementation that connects a single Bus target to
QwenPaw's ``POST /api/console/chat`` endpoint and consumes its SSE
response.

On each incoming ``stream_invoke`` request:

1. The Envelope payload is mapped to a QwenPaw chat request:
   * ``payload[<text_field>]`` -> ``input[0].content[0].text``
   * ``env.session_id``        -> body ``session_id`` (when set)
   * ``metadata["qwenpaw.*"]`` -> body extension fields (prefix stripped)
   * ``metadata["qwenpaw.user_id"]`` / ``metadata["qwenpaw.channel"]``
     are controlled overrides for ``user_id`` / ``channel``.

2. The SSE response is consumed and translated to Bus stream frames:
   * ``output[].content[].text`` (cumulative-text deduplicated) ->
     ``writer.delta({"delta": ...})``
   * ``status: completed`` or stream end ->
     ``writer.final({"text": accumulated, "raw": last_event})``

3. HTTP / transport / timeout errors are translated into the appropriate
   :class:`BusError` subclass.

This bridge intentionally supports only the streaming
``POST /api/console/chat`` path; non-streaming ``bus.invoke()``, ACP
stdio, and a generic HTTP stream bridge are out of scope for the first
version. See ``prompts/qwenpaw_chat_sse_plan.md`` for the full design.
"""

from __future__ import annotations

import json
import logging
import os
import re
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
    only needed when a ``QwenPawChatSSEBridge`` is actually constructed.
    """
    try:
        import httpx
    except ImportError as exc:
        raise ImportError(
            "QwenPawChatSSEBridge requires httpx. "
            "Install the optional bridge extra: pip install 'openagentio[bridge]'"
        ) from exc
    return httpx


# Default mapping values, documented in prompts/qwenpaw_chat_sse_plan.md.
_DEFAULT_TEXT_FIELD = "text"
_DEFAULT_SESSION_FIELD = "session_id"
_DEFAULT_METADATA_PREFIX = "qwenpaw."

# Default configuration values for the user-facing wrapper and the
# BridgeRunner YAML path. QwenPaw ships on port 8088; ``agent_id`` defaults
# to QwenPaw's built-in ``default`` agent; ``request_timeout`` is longer
# than OpenClaw because a QwenPaw agent may execute tools mid-turn.
_DEFAULT_BASE_URL = "http://127.0.0.1:8088"
_DEFAULT_AGENT_ID = "default"
_DEFAULT_USER_ID = "openagentio-user"
_DEFAULT_CHANNEL = "console"
_DEFAULT_REQUEST_TIMEOUT = 120.0
_DEFAULT_TARGET = "qwenpaw.chat"

# Request body keys managed by the bridge itself. Payload passthrough and
# ``qwenpaw.*`` metadata mapping are not allowed to override these fields,
# preserving the SSE-only contract and the QwenPaw request structure -
# except the two controlled overrides ``user_id`` / ``channel``.
_RESERVED_BODY_KEYS = frozenset({"input", "session_id", "user_id", "channel"})

# Matches ``${VAR}`` or ``${VAR:-default}``. Copied from the OpenClaw bridge
# pending a shared config-resolution helper (plan §5.1.1).
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


class QwenPawChatSSEBridge(Bridge):
    """Bridges one Bus target to QwenPaw via ``POST /api/console/chat`` SSE.

    Each incoming ``stream_invoke`` request is mapped to a QwenPaw chat
    request and the SSE response is translated back to Bus ``delta`` /
    ``final`` frames. See the module docstring for the full mapping.
    """

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
            f"openagentio.bridge.qwenpaw_chat_sse.{definition.name}"
        )

        cfg = dict(definition.config)

        # base_url is the only strictly required field; the chat endpoint is
        # always derived from it as <base>/api/console/chat.
        self._base_url = _resolve_config_value(
            self._require_string(cfg, "base_url"), definition.name
        )
        if not self._base_url:
            raise ValueError(
                f"bridge '{definition.name}': config 'base_url' is required"
            )
        self._chat_url = self._base_url.rstrip("/") + "/api/console/chat"

        # token is optional: QwenPaw skips Web login auth for 127.0.0.1/::1
        # requests, so local deployments may leave it empty.
        self._token = _resolve_config_value(
            self._require_string(cfg, "token"), definition.name
        )

        # agent_id maps to the mandatory X-Agent-Id header; default to
        # QwenPaw's built-in "default" agent when not supplied so the header
        # is never empty.
        self._agent_id = (
            _resolve_config_value(
                self._require_string(cfg, "agent_id"), definition.name
            )
            or _DEFAULT_AGENT_ID
        )

        # user_id / channel default to documented values; both remain
        # overridable at request time via qwenpaw.user_id / qwenpaw.channel
        # metadata.
        self._user_id = (
            _resolve_config_value(
                self._require_string(cfg, "user_id"), definition.name
            )
            or _DEFAULT_USER_ID
        )
        self._channel = (
            _resolve_config_value(
                self._require_string(cfg, "channel"), definition.name
            )
            or _DEFAULT_CHANNEL
        )

        self._request_timeout = float(
            _resolve_config_value(
                cfg.get("request_timeout", _DEFAULT_REQUEST_TIMEOUT),
                definition.name,
            )
        )
        if self._request_timeout <= 0:
            raise ValueError(
                f"bridge '{definition.name}': config 'request_timeout' must be "
                "positive"
            )

        mappings = definition.mappings
        self._text_field = mappings.text_field or _DEFAULT_TEXT_FIELD
        self._session_field = mappings.session_field or _DEFAULT_SESSION_FIELD
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
        self._stream_sub = await self._bus.handle_stream(
            self._definition.name, self._on_stream
        )
        self._logger.info(
            "qwenpaw chat sse bridge ready: target=%s url=%s agent_id=%s",
            self._definition.name,
            self._chat_url,
            self._agent_id,
        )

    async def stop(self) -> None:
        sub = self._stream_sub
        self._stream_sub = None
        if sub is not None:
            try:
                await sub.unsubscribe()
            except Exception:  # noqa: BLE001 - best-effort cleanup
                self._logger.exception(
                    "qwenpaw chat sse bridge: failed to unsubscribe bus handler"
                )
        client = self._client
        self._client = None
        if client is not None and self._own_client:
            await client.aclose()

    # -- request handler ----------------------------------------------------

    async def _on_stream(self, env: "Envelope", writer: "StreamWriter") -> None:
        """Bus stream handler: translate to a QwenPaw chat request and
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

    # -- request body building ---------------------------------------------

    def _build_request_body(self, env: "Envelope") -> dict[str, Any]:
        """Build a QwenPaw ``POST /api/console/chat`` request body.

        Field mapping (plan §3):

        * ``payload[<text_field>]`` -> ``input[0].content[0].text``
        * ``env.session_id``        -> body ``session_id`` (when set)
        * ``config.user_id`` / ``config.channel`` -> body defaults
        * ``metadata["qwenpaw.user_id"|"qwenpaw.channel"]`` -> controlled
          overrides of those two reserved fields
        * other ``metadata["qwenpaw.*"]`` -> body extension fields
        * payload non-reserved fields -> override metadata-derived values

        Reserved body keys (``input`` / ``session_id`` / ``user_id`` /
        ``channel``) can only be set by the bridge or the two controlled
        metadata overrides; payload passthrough never touches them.
        """
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

        body: dict[str, Any] = {
            "input": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": str(text)}],
                }
            ],
            "user_id": self._user_id,
            "channel": self._channel,
        }
        if env.session_id:
            body["session_id"] = env.session_id

        # Controlled overrides (user_id / channel) and extension fields
        # from qwenpaw.* metadata. Only the two controlled overrides may
        # touch reserved fields; input / session_id are never overridable
        # via metadata.
        prefix = self._metadata_prefix
        if env.metadata:
            for key, value in env.metadata.items():
                if not isinstance(key, str):
                    continue
                # Protocol-level acp.* metadata never leaks into the
                # downstream request body, regardless of metadata_prefix -
                # otherwise a custom prefix (e.g. "acp.") would forward
                # trace/session protocol keys to QwenPaw. Mirrors the
                # OpenClaw bridge guard.
                if key.startswith("acp."):
                    continue
                if not key.startswith(prefix):
                    continue
                stripped = key[len(prefix):]
                if not stripped:
                    continue
                if stripped in ("user_id", "channel"):
                    body[stripped] = value
                elif stripped in _RESERVED_BODY_KEYS:
                    continue
                else:
                    body[stripped] = value

        # Payload passthrough: extra non-reserved fields override
        # metadata-derived extension values. Reserved protocol fields are
        # always ignored so callers cannot break the QwenPaw request shape.
        for key, value in payload.items():
            if key == self._text_field or key in _RESERVED_BODY_KEYS:
                continue
            body[key] = value

        return body

    def _build_headers(self, env: "Envelope") -> dict[str, str]:
        """Build per-request headers: ``X-Agent-Id`` (mandatory) and an
        optional ``Authorization: Bearer`` when ``token`` is configured."""
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "X-Agent-Id": self._agent_id,
        }
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    # -- response streaming -------------------------------------------------

    async def _stream_response(
        self, response: httpx.Response, writer: "StreamWriter"
    ) -> None:
        """Consume the QwenPaw SSE response and emit delta/final frames.

        ``output[].content[].text`` may be cumulative across events, so each
        assistant text is deduplicated against the running accumulated text
        before being forwarded as a delta (plan §4).
        """
        accumulated = ""
        last_event: dict[str, Any] | None = None

        async for payload in parse_sse(response.aiter_text()):
            if not payload:
                continue
            # QwenPaw terminates on status:"completed" rather than [DONE],
            # but tolerate the OpenAI terminator defensively.
            if payload == "[DONE]":
                break
            try:
                chunk = json.loads(payload)
            except json.JSONDecodeError as exc:
                self._logger.warning(
                    "qwenpaw chat sse bridge: failed to decode SSE data: %s", exc
                )
                continue
            if not isinstance(chunk, dict):
                continue

            last_event = chunk
            status = chunk.get("status")

            if status == "failed" or "error" in chunk:
                raise _map_sse_error(self._definition.name, chunk)

            event_text = _extract_assistant_text(chunk)
            if event_text:
                if event_text.startswith(accumulated):
                    # Cumulative mode: the new text extends the prior total;
                    # only the new suffix is forwarded.
                    delta = event_text[len(accumulated):]
                    accumulated = event_text
                else:
                    # Incremental mode: the event text is a fresh fragment.
                    delta = event_text
                    accumulated += event_text
                if delta:
                    try:
                        await writer.delta({"delta": delta})
                    except Exception:  # noqa: BLE001
                        self._logger.exception(
                            "qwenpaw chat sse bridge: writer.delta failed"
                        )

            if status == "completed":
                break

        await writer.final({"text": accumulated, "raw": last_event})


# ---------------------------------------------------------------------------
# User-facing convenience wrapper
# ---------------------------------------------------------------------------

class QwenPawChatBridge:
    """Small convenience wrapper for QwenPaw chat integration.

    This keeps the low-level ``BridgeDefinition`` API available for bridge
    authors, while giving application users a short path:

        bridge = QwenPawChatBridge.from_env(bus, target="qwenpaw.chat")
        await bridge.start()
    """

    def __init__(
        self,
        bus: "Bus",
        *,
        target: str = _DEFAULT_TARGET,
        base_url: str = _DEFAULT_BASE_URL,
        token: str = "",
        agent_id: str = _DEFAULT_AGENT_ID,
        user_id: str = _DEFAULT_USER_ID,
        channel: str = _DEFAULT_CHANNEL,
        request_timeout: float = _DEFAULT_REQUEST_TIMEOUT,
    ) -> None:
        self.target = target
        definition = BridgeDefinition(
            name=target,
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
                text_field=_DEFAULT_TEXT_FIELD,
                session_field=_DEFAULT_SESSION_FIELD,
                metadata_prefix=_DEFAULT_METADATA_PREFIX,
            ),
        )
        self._bridge = QwenPawChatSSEBridge(bus, definition)

    @classmethod
    def from_env(
        cls,
        bus: "Bus",
        *,
        target: str = _DEFAULT_TARGET,
        base_url_env: str = "QWENPAW_BASE_URL",
        token_env: str = "QWENPAW_AUTH_TOKEN",
        agent_id_env: str = "QWENPAW_AGENT_ID",
        user_id_env: str = "QWENPAW_USER_ID",
        channel_env: str = "QWENPAW_CHANNEL",
        timeout_env: str = "QWENPAW_REQUEST_TIMEOUT",
    ) -> "QwenPawChatBridge":
        # Unlike OpenClaw, token is NOT required: QwenPaw's local
        # 127.0.0.1 path skips Web login auth, so an empty token is the
        # expected default for local development.
        return cls(
            bus,
            target=target,
            base_url=os.environ.get(base_url_env, _DEFAULT_BASE_URL),
            token=os.environ.get(token_env, ""),
            agent_id=os.environ.get(agent_id_env, _DEFAULT_AGENT_ID),
            user_id=os.environ.get(user_id_env, _DEFAULT_USER_ID),
            channel=os.environ.get(channel_env, _DEFAULT_CHANNEL),
            request_timeout=float(
                os.environ.get(timeout_env, str(int(_DEFAULT_REQUEST_TIMEOUT)))
            ),
        )

    async def start(self) -> None:
        await self._bridge.start()

    async def stop(self) -> None:
        await self._bridge.stop()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_assistant_text(chunk: dict[str, Any]) -> str:
    """Extract concatenated assistant text from a QwenPaw SSE event.

    QwenPaw nests model output under ``output[].content[]``; only
    ``role == "assistant"`` items and ``type == "text"`` parts carry text.
    Recent QwenPaw versions may also stream text as top-level content events:
    ``{"type": "text", "delta": true, "text": "..."}``.
    The returned string may be cumulative across events (the dedup against
    the running accumulated text is handled by the caller per plan §4).
    """
    if not isinstance(chunk, dict):
        return ""

    if chunk.get("type") == "text" and chunk.get("delta") is True:
        text = chunk.get("text")
        return str(text) if text is not None else ""

    output = chunk.get("output")
    if not isinstance(output, list):
        return ""
    texts: list[str] = []
    for item in output:
        if not isinstance(item, dict) or item.get("role") != "assistant":
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict) or part.get("type") != "text":
                continue
            text = part.get("text")
            if text is not None:
                texts.append(str(text))
    return "".join(texts)


def _map_sse_error(
    bridge_name: str, chunk: dict[str, Any]
) -> AgentUnavailableError | AuthFailureError | InvalidRequestError:
    """Map a QwenPaw SSE failure event to a :class:`BusError`.

    QwenPaw signals failures with ``status: "failed"`` or an ``error``
    field. ``MODEL_EXECUTION_FAILED`` and other unrecognised failures
    default to :class:`AgentUnavailableError` (plan §2.6); validation /
    bad-request failures map to :class:`InvalidRequestError`.
    """
    error = chunk.get("error") if isinstance(chunk, dict) else None
    if isinstance(error, dict):
        message = str(error.get("message", ""))
        code = str(error.get("code", ""))
    elif error is not None:
        message = str(error)
        code = ""
    elif isinstance(chunk, dict):
        message = str(chunk.get("message", ""))
        code = str(chunk.get("code", ""))
    else:
        message = str(chunk)
        code = ""

    text = " ".join(p for p in (code, message) if p).lower()
    detail = message or str(chunk)
    if code:
        detail = f"[{code}] {detail}"
    full = f"bridge '{bridge_name}': SSE error {detail}"

    if any(k in text for k in ("auth", "permission", "unauthorized", "forbidden")):
        return AuthFailureError(full)
    if any(k in text for k in ("invalid", "validation", "bad_request", "schema")):
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

def qwenpaw_chat_sse_factory(
    bus: "Bus", definition: BridgeDefinition
) -> Bridge:
    """:class:`BridgeFactory` for ``type: "qwenpaw_chat_sse"`` entries."""
    return QwenPawChatSSEBridge(bus, definition)


__all__ = [
    "QwenPawChatBridge",
    "QwenPawChatSSEBridge",
    "qwenpaw_chat_sse_factory",
]
