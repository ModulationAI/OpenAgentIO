"""Golden sample / schema contract tests. Mirrors pkg/event/golden_test.go."""
from __future__ import annotations

import json
import re
import uuid
from pathlib import Path

import pytest

from openagentio import Envelope, ResponseError, ResponseFinal, is_terminal
from openagentio.codec.json_codec import JSONCodec

SAMPLES = Path(__file__).resolve().parents[3] / "schema" / "samples"
SCHEMA = Path(__file__).resolve().parents[3] / "schema" / "envelope.schema.json"

CODEC = JSONCodec()

UUID_V7_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
RFC3339_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$"
)


def _load(filename: str) -> bytes:
    return (SAMPLES / filename).read_bytes()


SAMPLE_FILENAMES = [
    "message_received.json",
    "response_started.json",
    "response_delta.json",
    "response_final.json",
    "response_error.json",
]


# --- Schema validation -------------------------------------------------------


def test_samples_validate_against_schema() -> None:
    """All golden samples satisfy the JSON Schema."""
    import jsonschema

    schema = json.loads(SCHEMA.read_text())
    for name in SAMPLE_FILENAMES:
        data = json.loads(_load(name))
        jsonschema.validate(data, schema)


# --- Round-trip --------------------------------------------------------------


@pytest.mark.parametrize("name", SAMPLE_FILENAMES)
def test_envelope_round_trip_preserves_samples(name: str) -> None:
    """Decoding a sample and re-encoding it twice yields the same bytes."""
    original = _load(name)
    env = Envelope.from_bytes(original)
    encoded1 = env.to_bytes()
    env2 = Envelope.from_bytes(encoded1)
    encoded2 = env2.to_bytes()
    assert encoded1 == encoded2


# --- Required fields ---------------------------------------------------------


def test_new_envelope_has_required_fields() -> None:
    env = Envelope.new("test")
    assert env.spec_version == "acp/1.0"
    assert env.schema_version == 1
    assert env.event_id
    assert env.occurred_at.tzinfo is not None

    raw = json.loads(env.to_bytes())
    assert raw["spec_version"]
    assert raw["schema_version"] == 1
    assert raw["event_id"]
    assert raw["event_type"] == "test"
    assert raw["occurred_at"]


# --- Unknown field forward compatibility -------------------------------------


@pytest.mark.parametrize("name", SAMPLE_FILENAMES)
def test_unknown_field_forward_compat(name: str) -> None:
    """Extra wire fields must not break decoding."""
    data = json.loads(_load(name))
    data["future_field"] = "ignored"
    env = Envelope.from_bytes(json.dumps(data).encode("utf-8"))
    assert env.event_type


# --- seq=0 omission ----------------------------------------------------------


def test_seq_zero_is_omitted() -> None:
    env = Envelope.new("test")
    env.seq = 0
    raw = json.loads(env.to_bytes())
    assert "seq" not in raw


# --- Terminal event / is_final ----------------------------------------------


def test_terminal_event_types() -> None:
    assert is_terminal(ResponseFinal)
    assert is_terminal(ResponseError)


def test_response_final_sample_is_final() -> None:
    env = Envelope.from_bytes(_load("response_final.json"))
    assert env.is_final is True


def test_response_error_sample_is_final() -> None:
    env = Envelope.from_bytes(_load("response_error.json"))
    assert env.is_final is True


# --- UUID / time format ------------------------------------------------------


def test_event_id_is_uuid_v7() -> None:
    for _ in range(10):
        env = Envelope.new("test")
        assert UUID_V7_RE.match(env.event_id)


def test_occurred_at_is_rfc3339() -> None:
    env = Envelope.new("test")
    raw = json.loads(env.to_bytes())
    assert RFC3339_RE.match(raw["occurred_at"])


# --- ErrorPayload ------------------------------------------------------------


def test_response_error_payload_shape() -> None:
    raw = json.loads(_load("response_error.json"))
    payload = raw["payload"]
    assert "code" in payload
    assert "message" in payload
    assert "retryable" in payload


def test_response_error_decodes_retryable() -> None:
    env = Envelope.from_bytes(_load("response_error.json"))
    payload = env.payload_json()
    assert isinstance(payload["retryable"], bool)
    assert payload["code"] == "AGENT_TIMEOUT"


# --- Time format parsing -----------------------------------------------------


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
