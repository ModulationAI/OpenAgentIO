"""Matrix event bridge.

Bridges Matrix room message events to the OpenAgentIO Bus as publish/subscribe
events. This is the minimal event bridge: text messages in configured Matrix
rooms become Bus events, and Bus events addressed back to a Matrix room are sent
as ``m.room.message`` events.

This module is intentionally narrow: it does not implement a full Matrix client,
homeserver, federation, E2EE, media handling, or thread/reply semantics. Those
are out of scope for the first phase and can be layered later.

Trace context:

Matrix itself does not standardise W3C trace propagation. This bridge treats a
top-level ``traceparent`` key in ``m.room.message`` ``content`` or ``unsigned``
metadata as a transport-specific convention and copies it into the outbound
Envelope's ``traceparent`` / ``trace_id`` / ``span_id`` fields. Downstream
agent workflows can therefore continue a trace that was started outside Matrix.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections import deque
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlparse

from openagentio.bridge.base import Bridge
from openagentio.bridge.config import BridgeConfigError, BridgeDefinition
from openagentio.bus.errors import (
    AgentTimeoutError,
    AgentUnavailableError,
    AuthFailureError,
    InvalidRequestError,
    TransportFailureError,
)
from openagentio.event.envelope import Envelope

if TYPE_CHECKING:  # pragma: no cover
    import httpx

    from openagentio.bus import Bus
    from openagentio.transport.base import Subscription


def _import_httpx() -> Any:
    """Lazily import httpx so the bridge extra remains optional."""
    try:
        import httpx
    except ImportError as exc:
        raise ImportError(
            "MatrixEventBridge requires httpx. "
            "Install the optional bridge extra: pip install 'openagentio[bridge]'"
        ) from exc
    return httpx


class _RateLimitedError(Exception):
    """Internal exception carrying Matrix ``retry_after_ms`` for the sync loop."""

    def __init__(self, message: str, retry_after_ms: int | None) -> None:
        super().__init__(message)
        self.retry_after_ms = retry_after_ms


def _require_string(
    config: dict[str, Any], key: str, bridge_name: str
) -> str:
    """Extract a required non-empty string value from bridge config.

    Leading/trailing whitespace is stripped; a whitespace-only value is treated
    as missing so that configurations like ``"   "`` are rejected early.
    """
    value = config.get(key)
    if not isinstance(value, str):
        raise BridgeConfigError(
            f"bridge '{bridge_name}': config '{key}' is required and must be a non-empty string"
        )
    value = value.strip()
    if not value:
        raise BridgeConfigError(
            f"bridge '{bridge_name}': config '{key}' is required and must be a non-empty string"
        )
    return value


def _require_positive_number(
    config: dict[str, Any], key: str, bridge_name: str, default: float
) -> float:
    """Extract a positive numeric config value, falling back to ``default``."""
    value = config.get(key, default)
    if value is None:
        return default
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise BridgeConfigError(
            f"bridge '{bridge_name}': config '{key}' must be a positive number"
        ) from exc
    if number <= 0:
        raise BridgeConfigError(
            f"bridge '{bridge_name}': config '{key}' must be positive, got {number}"
        )
    return number


def _require_enum(
    config: dict[str, Any],
    key: str,
    bridge_name: str,
    allowed: set[str],
    default: str,
    *,
    label: str = "config",
) -> str:
    """Extract a string config/mapping value that must be one of ``allowed``."""
    value = config.get(key, default)
    if not isinstance(value, str):
        raise BridgeConfigError(
            f"bridge '{bridge_name}': {label} '{key}' must be a string, "
            f"got {type(value).__name__}"
        )
    value = value.strip()
    if value not in allowed:
        raise BridgeConfigError(
            f"bridge '{bridge_name}': {label} '{key}' must be one of "
            f"{sorted(allowed)!r}, got {value!r}"
        )
    return value


def _require_string_list(
    config: dict[str, Any], key: str, bridge_name: str
) -> list[str]:
    """Extract a required list of non-empty strings from bridge config."""
    value = config.get(key)
    if not isinstance(value, list):
        raise BridgeConfigError(
            f"bridge '{bridge_name}': config '{key}' is required and must be a list of strings"
        )
    if not value:
        raise BridgeConfigError(
            f"bridge '{bridge_name}': config '{key}' must contain at least one entry"
        )
    result: list[str] = []
    for i, item in enumerate(value):
        if not isinstance(item, str):
            raise BridgeConfigError(
                f"bridge '{bridge_name}': config '{key}[{i}]' must be a non-empty string"
            )
        item = item.strip()
        if not item:
            raise BridgeConfigError(
                f"bridge '{bridge_name}': config '{key}[{i}]' must be a non-empty string"
            )
        result.append(item)
    return result


def _validate_homeserver_url(value: str, bridge_name: str) -> str:
    """Strip and validate that ``value`` looks like an HTTP(S) URL."""
    stripped = value.strip()
    parsed = urlparse(stripped)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise BridgeConfigError(
            f"bridge '{bridge_name}': config 'homeserver_url' must be an HTTP(S) URL, "
            f"got {stripped!r}"
        )
    # Normalise: no trailing slash so path construction is predictable.
    return stripped.rstrip("/")


def _validate_user_id(value: str, bridge_name: str) -> str:
    """Strip and validate a Matrix user ID (``@localpart:server``)."""
    stripped = value.strip()
    if not stripped.startswith("@") or ":" not in stripped:
        raise BridgeConfigError(
            f"bridge '{bridge_name}': config 'user_id' must be a Matrix user ID "
            f"like '@bot:example.com', got {stripped!r}"
        )
    return stripped


def _validate_room_id(value: str, bridge_name: str) -> str:
    """Strip and validate a Matrix room ID (``!localid:server``)."""
    stripped = value.strip()
    if not stripped.startswith("!") or ":" not in stripped:
        raise BridgeConfigError(
            f"bridge '{bridge_name}': config 'room_ids' entries must be Matrix room IDs "
            f"like '!room:example.com', got {stripped!r}"
        )
    return stripped


def _require_valid_room_id(value: str, bridge_name: str) -> str:
    """Runtime validation of a resolved Matrix room ID.

    Mirrors the config-time check but raises :class:`InvalidRequestError` so
    outbound payloads with bad ``room_id`` values produce a clear client error.
    """
    stripped = value.strip()
    if not stripped.startswith("!") or ":" not in stripped:
        raise InvalidRequestError(
            f"bridge '{bridge_name}': '{stripped}' is not a valid Matrix room ID "
            "like '!room:example.com'"
        )
    return stripped


class MatrixEventBridge(Bridge):
    """Bridge Matrix room message events to OpenAgentIO publish/subscribe.

    Configuration (``definition.config``):

    * ``homeserver_url`` — Matrix Client-Server API base URL (required).
    * ``access_token`` — Bearer token for the Matrix bot account (required).
    * ``user_id`` — Matrix user ID of the bridge account (required).
    * ``room_ids`` — List of room IDs to listen in (required).
    * ``sync_timeout`` — Long-poll ``/sync`` timeout in seconds (default 30).
    * ``reconnect_delay`` — Base delay between reconnects in seconds (default 2).
    * ``initial_sync_behavior`` — ``skip`` or ``replay`` (default ``skip``).
    * ``outbound_msgtype`` — ``m.text`` or ``m.notice`` (default ``m.text``).

    Mapping overrides (``definition.mappings.extra``):

    * ``event_prefix`` — Namespace for inbound/outbound events (default ``matrix``).
    * ``inbound_message_event`` — Event type published on incoming messages
      (default ``matrix.message.received``).
    * ``outbound_message_event`` — Event type subscribed to for outbound messages
      (default ``matrix.message.send``).
    * ``session_strategy`` — ``room`` or ``room_sender`` (default ``room``).
    """

    # Defaults for config fields.
    _DEFAULT_SYNC_TIMEOUT = 30.0
    _DEFAULT_RECONNECT_DELAY = 2.0
    _DEFAULT_INITIAL_SYNC_BEHAVIOR = "skip"
    _DEFAULT_OUTBOUND_MSGTYPE = "m.text"

    # Defaults for mapping fields (stored in mappings.extra).
    _DEFAULT_EVENT_PREFIX = "matrix"
    _DEFAULT_INBOUND_EVENT = "matrix.message.received"
    _DEFAULT_OUTBOUND_EVENT = "matrix.message.send"
    _DEFAULT_SESSION_STRATEGY = "room"

    _MAX_RECONNECT_DELAY = 300.0
    _HEALTH_FAILURE_THRESHOLD = 3

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
            f"openagentio.bridge.matrix_event.{definition.name}"
        )

        cfg = dict(definition.config)

        self._homeserver_url = _validate_homeserver_url(
            _require_string(cfg, "homeserver_url", definition.name), definition.name
        )
        self._access_token = _require_string(cfg, "access_token", definition.name).strip()
        self._user_id = _validate_user_id(
            _require_string(cfg, "user_id", definition.name), definition.name
        )
        self._room_ids = {
            _validate_room_id(r, definition.name)
            for r in _require_string_list(cfg, "room_ids", definition.name)
        }

        self._sync_timeout = _require_positive_number(
            cfg, "sync_timeout", definition.name, self._DEFAULT_SYNC_TIMEOUT
        )
        self._reconnect_delay = _require_positive_number(
            cfg, "reconnect_delay", definition.name, self._DEFAULT_RECONNECT_DELAY
        )
        self._initial_sync_behavior = _require_enum(
            cfg,
            "initial_sync_behavior",
            definition.name,
            {"skip", "replay"},
            self._DEFAULT_INITIAL_SYNC_BEHAVIOR,
        )
        self._outbound_msgtype = _require_enum(
            cfg,
            "outbound_msgtype",
            definition.name,
            {"m.text", "m.notice"},
            self._DEFAULT_OUTBOUND_MSGTYPE,
        )

        # Mapping overrides. Unknown mapping keys are preserved in ``extra`` by
        # ``BridgeMappings.from_dict``; these are the ones this bridge consumes.
        mappings_extra = dict(definition.mappings.extra)

        self._event_prefix = self._mapping_string(
            mappings_extra, "event_prefix", self._DEFAULT_EVENT_PREFIX
        )
        self._inbound_message_event = self._mapping_string(
            mappings_extra, "inbound_message_event", self._DEFAULT_INBOUND_EVENT
        )
        self._outbound_message_event = self._mapping_string(
            mappings_extra, "outbound_message_event", self._DEFAULT_OUTBOUND_EVENT
        )
        self._session_strategy = _require_enum(
            mappings_extra,
            "session_strategy",
            definition.name,
            {"room", "room_sender"},
            self._DEFAULT_SESSION_STRATEGY,
            label="mapping",
        )

        # Runtime state. The HTTP client is created on start() so construction
        # does not open network resources. Tests may inject a pre-configured
        # client via the ``client`` argument.
        self._client: httpx.AsyncClient | None = client
        self._own_client = client is None
        self._httpx: Any = None
        self._sync_task: asyncio.Task[Any] | None = None
        self._subscriptions: list[Subscription] = []
        self._next_batch: str | None = None
        self._stopped = False
        self._is_healthy = True
        self._consecutive_sync_failures = 0
        self._last_sync_error: BaseException | None = None
        self._last_sync_at: float | None = None
        self._recent_event_ids: deque[str] = deque(maxlen=1000)
        self._recent_txn_ids: deque[str] = deque(maxlen=100)
        self._bridge_instance_id = uuid.uuid4().hex[:12]

    def _mapping_string(
        self, extra: dict[str, Any], key: str, default: str
    ) -> str:
        """Return a non-empty string mapping override or ``default``."""
        value = extra.get(key, default)
        if value is None or value == "":
            return default
        if not isinstance(value, str):
            raise BridgeConfigError(
                f"bridge '{self._definition.name}': mapping '{key}' must be a string, "
                f"got {type(value).__name__}"
            )
        value = value.strip()
        if not value:
            raise BridgeConfigError(
                f"bridge '{self._definition.name}': mapping '{key}' must be non-empty"
            )
        return value

    # --- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        """Start the bridge: create the HTTP client and register bus handlers.

        Calling :meth:`start` on an already-started bridge is a no-op so the
        lifecycle remains idempotent. If any startup step fails, already-created
        resources are cleaned up by :meth:`stop` before the exception propagates.
        """
        if self._subscriptions:
            return

        httpx = _import_httpx()
        try:
            if self._client is None:
                self._client = httpx.AsyncClient(
                    headers={"Authorization": f"Bearer {self._access_token}"},
                    timeout=httpx.Timeout(self._sync_timeout + 10.0),
                    trust_env=False,
                )
                self._own_client = True
            self._stopped = False

            # Subscribe to outbound Bus events addressed back to Matrix rooms.
            sub = await self._bus.subscribe(
                self._outbound_message_event, self._on_outbound_event
            )
            self._subscriptions.append(sub)

            self._sync_task = asyncio.create_task(self._sync_loop())

            self._logger.info(
                "matrix event bridge ready: name=%s homeserver=%s user=%s rooms=%d",
                self._definition.name,
                self._homeserver_url,
                self._user_id,
                len(self._room_ids),
            )
        except Exception:
            await self.stop()
            raise

    async def stop(self) -> None:
        """Stop the bridge and release resources. Safe to call multiple times."""
        self._stopped = True
        self._is_healthy = False

        task = self._sync_task
        self._sync_task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001 - best-effort cleanup
                self._logger.exception("matrix event bridge: sync task cleanup failed")

        while self._subscriptions:
            sub = self._subscriptions.pop()
            try:
                await sub.unsubscribe()
            except Exception:  # noqa: BLE001 - best-effort cleanup
                self._logger.exception(
                    "matrix event bridge: failed to unsubscribe bus handler"
                )

        client = self._client
        self._client = None
        if client is not None and self._own_client:
            try:
                await client.aclose()
            except Exception:  # noqa: BLE001 - best-effort cleanup
                self._logger.exception("matrix event bridge: failed to close HTTP client")

    # --- outbound Bus -> Matrix ----------------------------------------------

    async def _on_outbound_event(self, env: "Envelope") -> None:
        """Handle an outbound ``matrix.message.send`` event."""
        payload = env.payload_json()
        if not isinstance(payload, dict):
            raise InvalidRequestError(
                f"bridge '{self._definition.name}': outbound payload must be a JSON object"
            )

        room_id = self._resolve_outbound_room_id(payload, env)
        text = payload.get("text")
        if not isinstance(text, str) or not text:
            raise InvalidRequestError(
                f"bridge '{self._definition.name}': outbound payload 'text' "
                "is required and must be a non-empty string"
            )

        html = payload.get("html")
        if html is not None and not isinstance(html, str):
            raise InvalidRequestError(
                f"bridge '{self._definition.name}': outbound payload 'html' must be a string"
            )

        reply_to_event_id = payload.get("reply_to_event_id")
        if reply_to_event_id is None and env.metadata:
            reply_to_event_id = env.metadata.get("matrix.reply_to_event_id")
        if reply_to_event_id is not None and not isinstance(reply_to_event_id, str):
            raise InvalidRequestError(
                f"bridge '{self._definition.name}': 'reply_to_event_id' must be a string"
            )

        txn_id = self._new_txn_id()
        body = self._build_send_body(text, html, reply_to_event_id)
        await self._send_matrix_message(room_id, body, txn_id)
        self._recent_txn_ids.append(txn_id)

    def _resolve_outbound_room_id(
        self, payload: dict[str, Any], env: "Envelope"
    ) -> str:
        """Resolve the target Matrix room ID from payload, metadata, or session.

        Resolution priority:
        1. ``payload["room_id"]``
        2. ``env.metadata["matrix.room_id"]``
        3. ``env.session_id`` according to ``session_strategy``
        """
        room_id = payload.get("room_id")
        if isinstance(room_id, str) and room_id:
            return _require_valid_room_id(room_id, self._definition.name)

        metadata = env.metadata or {}
        room_id = metadata.get("matrix.room_id")
        if isinstance(room_id, str) and room_id:
            return _require_valid_room_id(room_id, self._definition.name)

        session_id = env.session_id
        if not session_id:
            raise InvalidRequestError(
                f"bridge '{self._definition.name}': outbound event missing 'room_id' "
                "and cannot derive it from session_id"
            )

        if self._session_strategy == "room":
            return _require_valid_room_id(session_id, self._definition.name)

        if self._session_strategy == "room_sender":
            # session_id = "room_id:sender_id"; sender always starts with "@".
            if ":@" not in session_id:
                raise InvalidRequestError(
                    f"bridge '{self._definition.name}': cannot derive room_id from "
                    f"session_id {session_id!r} using strategy 'room_sender'"
                )
            room_id, sender_id = session_id.rsplit(":@", 1)
            sender_id = "@" + sender_id
            if not room_id or not sender_id or ":" not in room_id or ":" not in sender_id:
                raise InvalidRequestError(
                    f"bridge '{self._definition.name}': cannot derive room_id from "
                    f"session_id {session_id!r} using strategy 'room_sender'"
                )
            return _require_valid_room_id(room_id, self._definition.name)

        raise InvalidRequestError(
            f"bridge '{self._definition.name}': unsupported session_strategy "
            f"{self._session_strategy!r}"
        )

    def _build_send_body(
        self,
        text: str,
        html: str | None,
        reply_to_event_id: str | None,
    ) -> dict[str, Any]:
        """Build the Matrix ``m.room.message`` content object."""
        content: dict[str, Any] = {
            "msgtype": self._outbound_msgtype,
            "body": text,
        }
        if html:
            content["format"] = "org.matrix.custom.html"
            content["formatted_body"] = html
        if reply_to_event_id:
            content["m.relates_to"] = {
                "m.in_reply_to": {"event_id": reply_to_event_id}
            }
        return content

    def _new_txn_id(self) -> str:
        """Return a unique transaction ID for idempotent Matrix sends."""
        return f"openagentio-{self._bridge_instance_id}-{uuid.uuid4().hex}"

    async def _send_matrix_message(
        self, room_id: str, content: dict[str, Any], txn_id: str
    ) -> None:
        """Send a Matrix ``m.room.message`` event via the Client-Server API."""
        httpx = _import_httpx()
        client = self._client
        if client is None:
            raise TransportFailureError(
                f"bridge '{self._definition.name}': bridge is not started"
            )

        encoded_room = quote(room_id, safe="")
        encoded_txn = quote(txn_id, safe="")
        url = (
            f"{self._homeserver_url}/_matrix/client/v3/rooms/"
            f"{encoded_room}/send/m.room.message/{encoded_txn}"
        )

        try:
            response = await client.put(
                url,
                json=content,
                headers={"Authorization": f"Bearer {self._access_token}"},
            )
        except asyncio.TimeoutError as exc:
            raise AgentTimeoutError(
                f"bridge '{self._definition.name}': Matrix send timed out"
            ) from exc
        except httpx.TimeoutException as exc:
            raise AgentTimeoutError(
                f"bridge '{self._definition.name}': Matrix send timed out"
            ) from exc
        except httpx.ConnectError as exc:
            raise AgentUnavailableError(
                f"bridge '{self._definition.name}': Matrix homeserver unavailable: {exc}"
            ) from exc
        except httpx.NetworkError as exc:
            raise AgentUnavailableError(
                f"bridge '{self._definition.name}': Matrix network error: {exc}"
            ) from exc
        except httpx.HTTPError as exc:
            raise TransportFailureError(
                f"bridge '{self._definition.name}': Matrix send failed: {exc}"
            ) from exc

        status = response.status_code
        if status in (401, 403):
            body = self._read_error_body(response)
            raise AuthFailureError(
                f"bridge '{self._definition.name}': Matrix authentication failed: "
                f"HTTP {status} {body}"
            )
        if status == 429:
            retry_after = self._retry_after_ms(response)
            detail = f", retry_after_ms={retry_after}" if retry_after is not None else ""
            raise TransportFailureError(
                f"bridge '{self._definition.name}': Matrix rate limited{detail}"
            )
        if status == 404:
            body = self._read_error_body(response)
            raise InvalidRequestError(
                f"bridge '{self._definition.name}': Matrix room not found: "
                f"HTTP {status} {body}"
            )
        if status >= 500:
            body = self._read_error_body(response)
            raise TransportFailureError(
                f"bridge '{self._definition.name}': Matrix server error: "
                f"HTTP {status} {body}"
            )
        if status >= 400:
            body = self._read_error_body(response)
            raise InvalidRequestError(
                f"bridge '{self._definition.name}': Matrix request rejected: "
                f"HTTP {status} {body}"
            )

    def _read_error_body(self, response: "httpx.Response") -> str:
        """Best-effort extraction of a short error body for logs/messages."""
        try:
            data = response.json()
            if isinstance(data, dict):
                message = data.get("error") or data.get("errcode") or ""
                return str(message)
            return str(data)[:200]
        except Exception:  # noqa: BLE001
            text = response.text or ""
            return text[:200]

    def _retry_after_ms(self, response: "httpx.Response") -> int | None:
        """Extract ``retry_after_ms`` from a Matrix error response if present."""
        try:
            data = response.json()
            if isinstance(data, dict):
                value = data.get("retry_after_ms")
                if isinstance(value, int):
                    return value
        except Exception:  # noqa: BLE001
            pass
        return None


    # --- inbound Matrix -> Bus -----------------------------------------------

    async def _sync_loop(self) -> None:
        """Long-poll Matrix ``/sync`` and publish inbound room messages."""
        while not self._stopped:
            try:
                await self._sync_once()
                self._consecutive_sync_failures = 0
                self._last_sync_error = None
                self._last_sync_at = time.monotonic()
                self._is_healthy = True
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - sync loop must survive
                self._consecutive_sync_failures += 1
                self._last_sync_error = exc
                self._logger.exception("matrix event bridge: sync failed")
                if self._consecutive_sync_failures >= self._HEALTH_FAILURE_THRESHOLD:
                    self._is_healthy = False

                retry_after_ms = getattr(exc, "retry_after_ms", None)
                if isinstance(retry_after_ms, int) and retry_after_ms > 0:
                    delay = min(self._MAX_RECONNECT_DELAY, retry_after_ms / 1000.0)
                else:
                    delay = min(
                        self._MAX_RECONNECT_DELAY,
                        self._reconnect_delay * (2 ** (self._consecutive_sync_failures - 1)),
                    )
                if not self._stopped:
                    try:
                        await asyncio.sleep(delay)
                    except asyncio.CancelledError:
                        break

    async def _sync_once(self) -> None:
        """Perform one incremental ``/sync`` request and process the response."""
        httpx = _import_httpx()
        client = self._client
        if client is None:
            raise TransportFailureError(
                f"bridge '{self._definition.name}': bridge is not started"
            )

        params: dict[str, Any] = {
            "timeout": int(self._sync_timeout * 1000),
            "set_presence": "offline",
        }
        if self._next_batch is not None:
            params["since"] = self._next_batch

        url = f"{self._homeserver_url}/_matrix/client/v3/sync"

        try:
            response = await client.get(
                url,
                params=params,
                headers={"Authorization": f"Bearer {self._access_token}"},
            )
        except asyncio.TimeoutError as exc:
            raise AgentTimeoutError(
                f"bridge '{self._definition.name}': Matrix sync timed out"
            ) from exc
        except httpx.TimeoutException as exc:
            raise AgentTimeoutError(
                f"bridge '{self._definition.name}': Matrix sync timed out"
            ) from exc
        except httpx.ConnectError as exc:
            raise AgentUnavailableError(
                f"bridge '{self._definition.name}': Matrix homeserver unavailable: {exc}"
            ) from exc
        except httpx.NetworkError as exc:
            raise AgentUnavailableError(
                f"bridge '{self._definition.name}': Matrix network error: {exc}"
            ) from exc
        except httpx.HTTPError as exc:
            raise TransportFailureError(
                f"bridge '{self._definition.name}': Matrix sync failed: {exc}"
            ) from exc

        status = response.status_code
        if status in (401, 403):
            body = self._read_error_body(response)
            raise AuthFailureError(
                f"bridge '{self._definition.name}': Matrix authentication failed: "
                f"HTTP {status} {body}"
            )
        if status == 429:
            retry_after = self._retry_after_ms(response)
            detail = f", retry_after_ms={retry_after}" if retry_after is not None else ""
            raise _RateLimitedError(
                f"bridge '{self._definition.name}': Matrix rate limited{detail}",
                retry_after,
            )
        if status >= 500:
            body = self._read_error_body(response)
            raise TransportFailureError(
                f"bridge '{self._definition.name}': Matrix server error: "
                f"HTTP {status} {body}"
            )
        if status >= 400:
            body = self._read_error_body(response)
            raise TransportFailureError(
                f"bridge '{self._definition.name}': Matrix sync rejected: "
                f"HTTP {status} {body}"
            )

        try:
            data = response.json()
        except Exception as exc:
            raise TransportFailureError(
                f"bridge '{self._definition.name}': Matrix sync response is not valid JSON"
            ) from exc

        next_batch = data.get("next_batch")
        is_initial = self._next_batch is None

        # Initial sync with skip behavior only establishes the cursor; commit
        # immediately because no timeline events are processed in this batch.
        if is_initial and self._initial_sync_behavior == "skip":
            if next_batch:
                self._next_batch = next_batch
            self._logger.debug(
                "matrix event bridge: initial sync skipped; next_batch saved"
            )
            return

        # Process the batch first, then commit the cursor so a failure here
        # causes the same batch to be re-fetched on the next iteration.
        await self._process_sync_response(data)
        if next_batch:
            self._next_batch = next_batch

    async def _process_sync_response(self, data: dict[str, Any]) -> None:
        """Walk ``rooms.join`` and publish eligible text messages."""
        rooms = data.get("rooms") or {}
        join = rooms.get("join") or {}
        for room_id, room_data in join.items():
            if room_id not in self._room_ids:
                continue
            timeline = (room_data or {}).get("timeline") or {}
            for event in timeline.get("events") or []:
                await self._process_inbound_event(room_id, event)

    async def _process_inbound_event(
        self, room_id: str, event: dict[str, Any]
    ) -> None:
        """Publish a single Matrix event if it passes all filters."""
        if not isinstance(event, dict):
            return
        if event.get("type") != "m.room.message":
            return

        content = event.get("content") or {}
        if not isinstance(content, dict):
            return
        if content.get("msgtype") != "m.text":
            return

        body = content.get("body")
        if not isinstance(body, str) or not body:
            return

        sender = event.get("sender")
        if not sender or sender == self._user_id:
            return

        event_id = event.get("event_id")
        if not event_id:
            return
        if event_id in self._recent_event_ids:
            return

        unsigned = event.get("unsigned") or {}
        txn_id = unsigned.get("transaction_id")
        if txn_id and txn_id in self._recent_txn_ids:
            return

        html: str | None = None
        if content.get("format") == "org.matrix.custom.html":
            formatted = content.get("formatted_body")
            if isinstance(formatted, str):
                html = formatted

        traceparent = self._extract_traceparent(content, unsigned)

        await self._publish_inbound_event(
            room_id=room_id,
            sender=sender,
            event_id=event_id,
            body=body,
            html=html,
            msgtype="m.text",
            origin_server_ts=event.get("origin_server_ts"),
            traceparent=traceparent,
        )
        self._recent_event_ids.append(event_id)

    def _extract_traceparent(
        self, content: dict[str, Any], unsigned: dict[str, Any]
    ) -> str | None:
        """Return a W3C traceparent from Matrix content or unsigned metadata.

        Matrix itself does not define W3C trace propagation. This bridge treats
        a top-level ``traceparent`` key in either ``content`` or ``unsigned`` as
        a transport-specific convention and copies it into the outbound Envelope
        so that an agent workflow can continue a distributed trace that was
        started outside Matrix.
        """
        value = content.get("traceparent") or unsigned.get("traceparent")
        if isinstance(value, str) and value:
            return value
        return None

    def _apply_traceparent(self, env: "Envelope", traceparent: str | None) -> None:
        """Apply a W3C traceparent string to the envelope and its metadata."""
        if not traceparent:
            return
        env.traceparent = traceparent
        # Only populate trace_id / span_id for the standard W3C traceparent
        # format: {version}-{trace_id}-{span_id}-{flags}. Non-standard strings
        # are still preserved above so callers can inspect them, but we avoid
        # putting misleading values into trace_id / span_id.
        parts = traceparent.split("-")
        if len(parts) == 4:
            env.trace_id = parts[1]
            env.span_id = parts[2]

    async def _publish_inbound_event(
        self,
        room_id: str,
        sender: str,
        event_id: str,
        body: str,
        html: str | None,
        msgtype: str,
        origin_server_ts: Any,
        traceparent: str | None = None,
    ) -> None:
        """Publish a normalized ``matrix.message.received`` event."""
        payload: dict[str, Any] = {
            "text": body,
            "html": html,
            "room_id": room_id,
            "sender": sender,
            "event_id": event_id,
            "origin_server_ts": origin_server_ts,
            "msgtype": msgtype,
        }
        metadata: dict[str, Any] = {
            "matrix.room_id": room_id,
            "matrix.sender": sender,
            "matrix.event_id": event_id,
            "matrix.origin_server_ts": origin_server_ts,
            "matrix.msgtype": msgtype,
        }
        if html is not None:
            metadata["matrix.html"] = html
        if traceparent is not None:
            metadata["matrix.traceparent"] = traceparent

        env = Envelope.new(self._inbound_message_event)
        env.conversation_id = room_id
        env.correlation_id = event_id
        env.metadata = metadata
        env.payload = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self._apply_traceparent(env, traceparent)

        if self._session_strategy == "room":
            env.session_id = room_id
        elif self._session_strategy == "room_sender":
            env.session_id = f"{room_id}:{sender}"

        await self._bus.publish(env)

    @property
    def is_healthy(self) -> bool:
        """Return ``True`` if the sync loop is not in a failing streak."""
        return self._is_healthy

    @property
    def last_error(self) -> BaseException | None:
        """Return the last sync error, if any."""
        return self._last_sync_error


def matrix_event_factory(bus: "Bus", definition: BridgeDefinition) -> Bridge:
    """:class:`BridgeFactory` for ``type: "matrix_event"`` entries."""
    return MatrixEventBridge(bus, definition)
