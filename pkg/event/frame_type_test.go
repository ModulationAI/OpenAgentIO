package event

import (
	"encoding/json"
	"testing"
)

func TestFrameTypeForEventType_MapsStandardEventTypes(t *testing.T) {
	cases := map[string]string{
		MessageReceived: FrameTypeRequest,
		ResponseStarted: FrameTypeResponseStarted,
		ResponseDelta:   FrameTypeResponseDelta,
		ResponseFinal:   FrameTypeResponseFinal,
		ResponseError:   FrameTypeResponseError,
		ToolCall:        FrameTypeToolCall,
		ToolResult:      FrameTypeToolResult,
	}
	for eventType, want := range cases {
		if got := FrameTypeForEventType(eventType); got != want {
			t.Errorf("FrameTypeForEventType(%q) = %q, want %q", eventType, got, want)
		}
	}
}

func TestFrameTypeForEventType_UnknownReturnsEmpty(t *testing.T) {
	if got := FrameTypeForEventType("goc.incident.created"); got != "" {
		t.Errorf("FrameTypeForEventType(unknown) = %q, want empty", got)
	}
}

func TestNewRequest_SetsFrameType(t *testing.T) {
	env := NewRequest()
	if env.EventType != MessageReceived {
		t.Errorf("NewRequest().EventType = %q, want %q", env.EventType, MessageReceived)
	}
	if env.FrameType != FrameTypeRequest {
		t.Errorf("NewRequest().FrameType = %q, want %q", env.FrameType, FrameTypeRequest)
	}
}

func TestNewEvent_DoesNotSetFrameType(t *testing.T) {
	env := NewEvent("goc.incident.created")
	if env.FrameType != "" {
		t.Errorf("NewEvent().FrameType = %q, want empty", env.FrameType)
	}
}

func TestEnvelope_EffectiveFrameType_UsesCanonicalMappingForKnownEventType(t *testing.T) {
	env := New(ResponseDelta)
	// Even if a user (or buggy producer) writes a contradictory frame_type, a
	// known event_type always wins so old and new consumers stay consistent.
	env.FrameType = FrameTypeResponseFinal
	if got := env.EffectiveFrameType(); got != FrameTypeResponseDelta {
		t.Errorf("EffectiveFrameType() = %q, want %q", got, FrameTypeResponseDelta)
	}
}

func TestEnvelope_EffectiveFrameType_FallsBackToExplicitValueForUnknownEventType(t *testing.T) {
	env := New("goc.incident.created")
	env.FrameType = "custom.frame"
	if got := env.EffectiveFrameType(); got != "custom.frame" {
		t.Errorf("EffectiveFrameType() = %q, want %q", got, "custom.frame")
	}
}

func TestEnvelope_EffectiveFrameType_FallsBackToEventType(t *testing.T) {
	env := New(ResponseDelta)
	if got := env.EffectiveFrameType(); got != FrameTypeResponseDelta {
		t.Errorf("EffectiveFrameType() = %q, want %q", got, FrameTypeResponseDelta)
	}
}

func TestEnvelope_FrameTypeRoundTrip(t *testing.T) {
	env := New(ResponseStarted)
	env.FrameType = FrameTypeResponseStarted

	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	var decoded Envelope
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}

	if decoded.FrameType != FrameTypeResponseStarted {
		t.Errorf("round-trip FrameType = %q, want %q", decoded.FrameType, FrameTypeResponseStarted)
	}
}

func TestEnvelope_FrameTypeOmittedWhenEmpty(t *testing.T) {
	env := New(ResponseDelta)
	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("json.Unmarshal into map: %v", err)
	}
	if _, ok := raw["frame_type"]; ok {
		t.Errorf("frame_type should be omitted when empty, got %v", raw)
	}
}
