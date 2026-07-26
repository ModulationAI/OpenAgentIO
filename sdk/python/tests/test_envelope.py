"""Envelope round-trip and golden-sample tests.

Verifies that the Python Envelope can decode samples produced from the Go
side (``schema/samples/*.json``) and that re-encoding preserves the wire
shape.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from openagentio import (
    Envelope,
    FrameTypeRequest,
    FrameTypeResponseDelta,
    FrameTypeResponseError,
    FrameTypeResponseFinal,
    FrameTypeResponseStarted,
    MessageReceived,
    ResponseDelta,
    ResponseError,
    ResponseFinal,
    ResponseStarted,
    SCHEMA_VERSION,
    SPEC_VERSION,
    effective_frame_type,
    frame_type_for_event_type,
    is_terminal,
)

SAMPLES = Path(__file__).resolve().parents[3] / "schema" / "samples"


def test_new_defaults() -> None:
    env = Envelope.new(MessageReceived)
    assert env.spec_version == SPEC_VERSION
    assert env.schema_version == SCHEMA_VERSION
    assert env.event_type == MessageReceived
    # UUID format: 8-4-4-4-12 = 36 chars including 4 hyphens.
    assert len(env.event_id) == 36
    assert env.event_id.count("-") == 4
    assert env.occurred_at.tzinfo is not None


def test_omit_empty_zero_values() -> None:
    env = Envelope.new("test")
    env.from_ = "alice"
    raw = json.loads(env.to_bytes())
    assert raw["from"] == "alice"
    # Defaults that should be omitted by zero-value rules.
    for key in ("to", "session_id", "is_final", "seq", "trace_id", "metadata", "payload"):
        assert key not in raw, f"{key} leaked despite zero value"


def test_seq_and_is_final_emitted_when_set() -> None:
    env = Envelope.new(ResponseFinal)
    env.seq = 3
    env.is_final = True
    raw = json.loads(env.to_bytes())
    assert raw["seq"] == 3
    assert raw["is_final"] is True


def test_payload_round_trips_as_structured_value() -> None:
    env = Envelope.new(MessageReceived)
    env.payload = b'{"text":"hello"}'
    raw = json.loads(env.to_bytes())
    assert raw["payload"] == {"text": "hello"}

    env2 = Envelope.from_bytes(env.to_bytes())
    assert env2.payload_json() == {"text": "hello"}


def test_clone_independence() -> None:
    env = Envelope.new("test")
    env.metadata = {"k": "v"}
    cp = env.clone()
    cp.metadata["k"] = "v2"
    assert env.metadata == {"k": "v"}
    assert cp.metadata == {"k": "v2"}


def test_is_terminal_known_types() -> None:
    assert is_terminal(ResponseFinal)
    assert is_terminal(ResponseError)
    assert not is_terminal(ResponseStarted)
    assert not is_terminal(ResponseDelta)


def test_frame_type_for_event_type_maps_standard_event_types() -> None:
    assert frame_type_for_event_type(MessageReceived) == FrameTypeRequest
    assert frame_type_for_event_type(ResponseStarted) == FrameTypeResponseStarted
    assert frame_type_for_event_type(ResponseDelta) == FrameTypeResponseDelta
    assert frame_type_for_event_type(ResponseFinal) == FrameTypeResponseFinal
    assert frame_type_for_event_type(ResponseError) == FrameTypeResponseError


def test_frame_type_for_event_type_unknown_returns_empty() -> None:
    assert frame_type_for_event_type("goc.incident.created") == ""


def test_effective_frame_type_uses_canonical_mapping_for_known_event_type() -> None:
    env = Envelope.new(ResponseDelta)
    # A contradictory frame_type on a known event_type must be ignored so old
    # and new consumers stay consistent.
    env.frame_type = FrameTypeResponseFinal
    assert effective_frame_type(env) == FrameTypeResponseDelta


def test_effective_frame_type_falls_back_to_explicit_value_for_unknown_event_type() -> None:
    env = Envelope.new("goc.incident.created")
    env.frame_type = "custom.frame"
    assert effective_frame_type(env) == "custom.frame"


def test_effective_frame_type_falls_back_to_event_type() -> None:
    env = Envelope.new(ResponseDelta)
    assert effective_frame_type(env) == FrameTypeResponseDelta


def test_envelope_new_request_sets_frame_type() -> None:
    env = Envelope.new_request()
    assert env.event_type == MessageReceived
    assert env.frame_type == FrameTypeRequest


def test_frame_type_round_trips() -> None:
    env = Envelope.new(ResponseStarted)
    env.frame_type = FrameTypeResponseStarted
    env2 = Envelope.from_bytes(env.to_bytes())
    assert env2.frame_type == FrameTypeResponseStarted


def test_frame_type_omitted_when_empty() -> None:
    env = Envelope.new(ResponseDelta)
    raw = json.loads(env.to_bytes())
    assert "frame_type" not in raw


# --- Golden samples produced by the Go SDK ---------------------------------


@pytest.mark.parametrize(
    "filename,expected_frame_type",
    [
        ("message_received.json", FrameTypeRequest),
        ("response_started.json", FrameTypeResponseStarted),
        ("response_delta.json", FrameTypeResponseDelta),
        ("response_final.json", FrameTypeResponseFinal),
        ("response_error.json", FrameTypeResponseError),
    ],
)
def test_decode_go_sample(filename: str, expected_frame_type: str) -> None:
    raw = (SAMPLES / filename).read_bytes()
    env = Envelope.from_bytes(raw)
    assert env.spec_version == "acp/1.0"
    assert env.schema_version == 1
    assert env.event_id  # non-empty
    assert env.frame_type == expected_frame_type
    # Round-trip preserves identity for fields the Python side reads.
    env2 = Envelope.from_bytes(env.to_bytes())
    assert env2.event_id == env.event_id
    assert env2.event_type == env.event_type
    assert env2.frame_type == env.frame_type
    assert env2.from_ == env.from_
    assert env2.to == env.to
    assert env2.seq == env.seq
    assert env2.is_final == env.is_final


def test_message_received_payload_match() -> None:
    raw = (SAMPLES / "message_received.json").read_bytes()
    env = Envelope.from_bytes(raw)
    assert env.event_type == MessageReceived
    assert env.from_ == "user-gateway"
    assert env.to == "main-agent"
    assert env.tenant_id == "tenant_demo"
    assert env.payload_json() == {"text": "hello"}


def test_response_final_marks_is_final() -> None:
    raw = (SAMPLES / "response_final.json").read_bytes()
    env = Envelope.from_bytes(raw)
    assert env.event_type == ResponseFinal
    assert env.is_final is True
    assert env.seq == 3
    assert env.payload_json() == {"result": {"answer": "42"}}


def test_response_error_payload_shape() -> None:
    raw = (SAMPLES / "response_error.json").read_bytes()
    env = Envelope.from_bytes(raw)
    assert env.event_type == ResponseError
    assert env.is_final is True
    p = env.payload_json()
    assert p["code"] == "AGENT_TIMEOUT"
    assert p["message"] == "deadline exceeded"
    assert p["retryable"] is True


# --- RFC3339 formatting ----------------------------------------------------


def test_format_time_no_micros() -> None:
    env = Envelope.new("test")
    env.occurred_at = datetime(2026, 5, 2, 10, 0, 0, tzinfo=timezone.utc)
    raw = json.loads(env.to_bytes())
    assert raw["occurred_at"] == "2026-05-02T10:00:00Z"


def test_format_time_trims_trailing_zeros() -> None:
    env = Envelope.new("test")
    env.occurred_at = datetime(2026, 5, 2, 10, 0, 0, 123000, tzinfo=timezone.utc)
    raw = json.loads(env.to_bytes())
    assert raw["occurred_at"] == "2026-05-02T10:00:00.123Z"


def test_parse_time_handles_z_suffix() -> None:
    env = Envelope.from_dict(
        {
            "spec_version": "acp/1.0",
            "schema_version": 1,
            "event_id": "00000000-0000-0000-0000-000000000000",
            "event_type": "test",
            "occurred_at": "2026-05-02T10:00:00.123Z",
        }
    )
    assert env.occurred_at.tzinfo is not None
    assert env.occurred_at.microsecond == 123000
