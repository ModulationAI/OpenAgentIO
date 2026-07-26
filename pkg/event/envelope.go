// Package event defines the ACP-compatible Envelope and the standard set of
// event type identifiers exchanged across the bus.
package event

import (
	"encoding/json"
	"time"
)

// Envelope is the wire-level container for every message routed by the bus.
type Envelope struct {
	SpecVersion   string `json:"spec_version"`
	SchemaVersion int    `json:"schema_version"`

	EventID    string    `json:"event_id"`
	EventType  string    `json:"event_type"`
	FrameType  string    `json:"frame_type,omitempty"`
	OccurredAt time.Time `json:"occurred_at"`

	TraceID        string `json:"trace_id,omitempty"`
	SpanID         string `json:"span_id,omitempty"`
	Traceparent    string `json:"traceparent,omitempty"`
	SessionID      string `json:"session_id,omitempty"`
	ConversationID string `json:"conversation_id,omitempty"`
	CorrelationID  string `json:"correlation_id,omitempty"`
	ReplyTo        string `json:"reply_to,omitempty"`

	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`

	Channel  string `json:"channel,omitempty"`
	TenantID string `json:"tenant_id,omitempty"`
	UserID   string `json:"user_id,omitempty"`

	Seq     uint64 `json:"seq,omitempty"`
	IsFinal bool   `json:"is_final,omitempty"`

	Payload  json.RawMessage `json:"payload,omitempty"`
	Metadata map[string]any  `json:"metadata,omitempty"`
}

// New constructs an Envelope pre-populated with spec/schema version, a fresh
// UUIDv7 event_id, and the current UTC timestamp.
func New(eventType string) *Envelope {
	return &Envelope{
		SpecVersion:   SpecVersion,
		SchemaVersion: SchemaVersion,
		EventID:       NewID(),
		EventType:     eventType,
		OccurredAt:    time.Now().UTC(),
	}
}

// NewEvent creates an envelope for pub/sub broadcasting.
// eventType should be a business routing key, e.g. "goc.incident.created".
func NewEvent(eventType string) *Envelope {
	return New(eventType)
}

// NewRequest creates an envelope for Invoke / StreamInvoke payloads.
// It always uses MessageReceived as the event type.
func NewRequest() *Envelope {
	env := New(MessageReceived)
	env.FrameType = FrameTypeRequest
	return env
}

// EffectiveFrameType returns the canonical frame_type for the envelope's
// event_type when the event_type is a known protocol frame. For unknown
// event_types it falls back to any explicit frame_type. This keeps old and new
// consumers consistent: a known event_type always implies the same frame_type,
// regardless of what a producer (or user code) may have written into the
// frame_type field.
func (e *Envelope) EffectiveFrameType() string {
	if e == nil {
		return ""
	}
	if ft := FrameTypeForEventType(e.EventType); ft != "" {
		return ft
	}
	return e.FrameType
}

// Clone returns a shallow copy suitable for in-pipeline mutation. Payload bytes
// are shared by reference; callers that mutate the payload should copy it.
func (e *Envelope) Clone() *Envelope {
	if e == nil {
		return nil
	}
	cp := *e
	if e.Metadata != nil {
		cp.Metadata = make(map[string]any, len(e.Metadata))
		for k, v := range e.Metadata {
			cp.Metadata[k] = v
		}
	}
	return &cp
}
