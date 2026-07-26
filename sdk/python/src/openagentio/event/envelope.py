"""ACP-compatible Envelope. Wire-equivalent to pkg/event/envelope.go.

The Python attribute names match the JSON wire keys 1:1 with two exceptions:

  * `from_` (Python attribute) maps to `"from"` (JSON key) — `from` is reserved.
  * `payload` is held as raw JSON bytes, mirroring Go's `json.RawMessage`, so
    pre-serialized blobs can be embedded without double-encoding.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any

from openagentio.event.ids import new_id
from openagentio.event.types import SCHEMA_VERSION, SPEC_VERSION


@dataclass
class Envelope:
    spec_version: str = SPEC_VERSION
    schema_version: int = SCHEMA_VERSION
    event_id: str = field(default_factory=new_id)
    event_type: str = ""
    frame_type: str = ""
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    trace_id: str = ""
    span_id: str = ""
    traceparent: str = ""
    session_id: str = ""
    conversation_id: str = ""
    correlation_id: str = ""
    reply_to: str = ""

    from_: str = ""
    to: str = ""

    channel: str = ""
    tenant_id: str = ""
    user_id: str = ""

    seq: int = 0
    is_final: bool = False

    payload: bytes | None = None
    metadata: dict[str, Any] | None = None

    @classmethod
    def new(cls, event_type: str) -> "Envelope":
        """Construct an Envelope with a fresh UUIDv7 event_id and current UTC time."""
        return cls(event_type=event_type)

    @classmethod
    def new_request(cls) -> "Envelope":
        """Construct a request envelope for Invoke / StreamInvoke.

        Equivalent to ``Envelope.new(MessageReceived)`` but also sets the
        experimental ``frame_type`` to ``request``.
        """
        from openagentio.event.types import FrameTypeRequest, MessageReceived

        return cls(event_type=MessageReceived, frame_type=FrameTypeRequest)

    def clone(self) -> "Envelope":
        """Shallow copy. Metadata dict is copied; payload bytes are shared."""
        cp = replace(self)
        if self.metadata is not None:
            cp.metadata = dict(self.metadata)
        return cp

    def payload_json(self) -> Any:
        """Decode payload bytes as JSON, or None if unset."""
        if not self.payload:
            return None
        return json.loads(self.payload)

    def to_dict(self) -> dict[str, Any]:
        """Build the wire-shaped dict, matching Go's `json:"...,omitempty"` semantics."""
        d: dict[str, Any] = {
            "spec_version": self.spec_version,
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "occurred_at": _format_time(self.occurred_at),
        }
        if self.frame_type:
            d["frame_type"] = self.frame_type
        if self.trace_id:
            d["trace_id"] = self.trace_id
        if self.span_id:
            d["span_id"] = self.span_id
        if self.traceparent:
            d["traceparent"] = self.traceparent
        if self.session_id:
            d["session_id"] = self.session_id
        if self.conversation_id:
            d["conversation_id"] = self.conversation_id
        if self.correlation_id:
            d["correlation_id"] = self.correlation_id
        if self.reply_to:
            d["reply_to"] = self.reply_to
        if self.from_:
            d["from"] = self.from_
        if self.to:
            d["to"] = self.to
        if self.channel:
            d["channel"] = self.channel
        if self.tenant_id:
            d["tenant_id"] = self.tenant_id
        if self.user_id:
            d["user_id"] = self.user_id
        if self.seq:
            d["seq"] = self.seq
        if self.is_final:
            d["is_final"] = self.is_final
        if self.payload:
            # Embed as a structured value, not a string. Mirrors json.RawMessage.
            d["payload"] = json.loads(self.payload)
        if self.metadata:
            d["metadata"] = self.metadata
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Envelope":
        env = cls(
            spec_version=data.get("spec_version", SPEC_VERSION),
            schema_version=data.get("schema_version", SCHEMA_VERSION),
            event_id=data.get("event_id", ""),
            event_type=data.get("event_type", ""),
            frame_type=data.get("frame_type", "") or "",
            occurred_at=_parse_time(data.get("occurred_at")),
            trace_id=data.get("trace_id", "") or "",
            span_id=data.get("span_id", "") or "",
            traceparent=data.get("traceparent", "") or "",
            session_id=data.get("session_id", "") or "",
            conversation_id=data.get("conversation_id", "") or "",
            correlation_id=data.get("correlation_id", "") or "",
            reply_to=data.get("reply_to", "") or "",
            from_=data.get("from", "") or "",
            to=data.get("to", "") or "",
            channel=data.get("channel", "") or "",
            tenant_id=data.get("tenant_id", "") or "",
            user_id=data.get("user_id", "") or "",
            seq=int(data.get("seq", 0) or 0),
            is_final=bool(data.get("is_final", False)),
            metadata=data.get("metadata"),
        )
        if "payload" in data and data["payload"] is not None:
            # Re-encode with stable separators for deterministic byte output.
            env.payload = json.dumps(data["payload"], separators=(",", ":")).encode("utf-8")
        return env

    def to_bytes(self) -> bytes:
        return json.dumps(self.to_dict(), separators=(",", ":")).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> "Envelope":
        return cls.from_dict(json.loads(data))


def _format_time(dt: datetime) -> str:
    """Format as RFC3339Nano with UTC `Z`, trimming trailing zeros from fractional seconds."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    if dt.microsecond:
        frac = f"{dt.microsecond:06d}".rstrip("0")
        return dt.strftime(f"%Y-%m-%dT%H:%M:%S.{frac}Z")
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_time(s: str | None) -> datetime:
    if not s:
        return datetime.now(timezone.utc)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
