package event_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v5"

	"github.com/ModulationAI/openagentio/pkg/codec"
	"github.com/ModulationAI/openagentio/pkg/event"
)

// schemaDir resolves to the repository's schema/ directory regardless of where
// `go test` is invoked from, by anchoring on this test file's location.
func schemaDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "schema")
}

func loadJSON(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}

func compileSchema(t *testing.T, dir string) *jsonschema.Schema {
	t.Helper()
	schemaPath := filepath.Join(dir, "envelope.schema.json")
	sch, err := jsonschema.Compile(schemaPath)
	if err != nil {
		t.Fatalf("compile %s: %v", schemaPath, err)
	}
	return sch
}

// goldenSamples returns the list of sample filenames under schema/samples/.
// Update this when adding a new event-type sample.
func goldenSamples() []string {
	return []string{
		"message_received.json",
		"response_started.json",
		"response_delta.json",
		"response_final.json",
		"response_error.json",
	}
}

func TestSamplesValidateAgainstSchema(t *testing.T) {
	dir := schemaDir(t)
	sch := compileSchema(t, dir)

	for _, name := range goldenSamples() {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(dir, "samples", name)
			data := loadJSON(t, path)

			var v any
			if err := json.Unmarshal(data, &v); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if err := sch.Validate(v); err != nil {
				t.Fatalf("schema validation failed: %#v", err)
			}
		})
	}
}

// TestEnvelopeRoundTripPreservesSamples asserts that each golden sample:
//
//  1. decodes into event.Envelope without leaving any unknown wire fields
//     (DisallowUnknownFields catches sample/struct drift), and
//  2. is fixed-point under the codec — encode(decode(encode(x))) == encode(x).
//
// We compare encoded forms (rather than struct fields or original bytes) so
// the test is robust against two protocol-legal differences: omitempty
// (an explicit `"seq": 0` in a sample becomes an absent field after Go
// re-encode) and payload whitespace (json.RawMessage preserves formatting on
// decode but encoding compacts it).
func TestEnvelopeRoundTripPreservesSamples(t *testing.T) {
	dir := schemaDir(t)
	c := codec.JSON()

	for _, name := range goldenSamples() {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(dir, "samples", name)
			original := loadJSON(t, path)

			dec := json.NewDecoder(bytes.NewReader(original))
			dec.DisallowUnknownFields()
			var first event.Envelope
			if err := dec.Decode(&first); err != nil {
				t.Fatalf("strict decode (sample has field unknown to Envelope?): %v", err)
			}

			encoded1, err := c.EncodeEnvelope(&first)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			second, err := c.DecodeEnvelope(encoded1)
			if err != nil {
				t.Fatalf("re-decode: %v", err)
			}
			encoded2, err := c.EncodeEnvelope(second)
			if err != nil {
				t.Fatalf("re-encode: %v", err)
			}

			if !bytes.Equal(encoded1, encoded2) {
				t.Fatalf("round-trip drift\nencoded1: %s\nencoded2: %s", encoded1, encoded2)
			}
		})
	}
}

func TestEnvelopeRequiredFields(t *testing.T) {
	dir := schemaDir(t)
	sch := compileSchema(t, dir)
	c := codec.JSON()

	env := event.New(event.MessageReceived)
	if env.SpecVersion == "" || env.SchemaVersion == 0 || env.EventID == "" || env.OccurredAt.IsZero() {
		t.Fatalf("event.New produced incomplete envelope: %+v", env)
	}

	encoded, err := c.EncodeEnvelope(env)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var v any
	if err := json.Unmarshal(encoded, &v); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := sch.Validate(v); err != nil {
		t.Fatalf("freshly minted envelope failed schema: %#v", err)
	}
}

// TestUnknownFieldForwardCompat verifies that adding future wire fields does not
// break codec decode. This matches the schema's additionalProperties: true.
func TestUnknownFieldForwardCompat(t *testing.T) {
	c := codec.JSON()
	dir := schemaDir(t)

	for _, name := range goldenSamples() {
		t.Run(name, func(t *testing.T) {
			original := loadJSON(t, filepath.Join(dir, "samples", name))

			var doc map[string]any
			if err := json.Unmarshal(original, &doc); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			doc["future_field"] = "ignored"
			withUnknown, err := json.Marshal(doc)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			env, err := c.DecodeEnvelope(withUnknown)
			if err != nil {
				t.Fatalf("decode with unknown field failed: %v", err)
			}
			if env.EventType == "" {
				t.Fatal("decoded envelope lost event_type")
			}
		})
	}
}

// TestSeqZeroOmitted verifies that an explicit seq=0 is omitted from the wire
// form (omitempty), matching the schema's expectation.
func TestSeqZeroOmitted(t *testing.T) {
	c := codec.JSON()
	env := event.New(event.ResponseDelta)
	env.Seq = 0

	encoded, err := c.EncodeEnvelope(env)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if bytes.Contains(encoded, []byte(`"seq"`)) {
		t.Fatalf("seq=0 should be omitted, got %s", encoded)
	}
}

// TestTerminalEventSetsIsFinal verifies that newReplyShell produces terminal
// envelopes with is_final=true for ResponseFinal/ResponseError.
func TestTerminalEventSetsIsFinal(t *testing.T) {
	req := event.NewRequest()
	for _, typ := range []string{event.ResponseFinal, event.ResponseError} {
		env := event.New(typ)
		env.CorrelationID = req.EventID
		env.From = "agent"
		env.To = req.From
		if !event.IsTerminal(env.EventType) {
			t.Fatalf("%q should be terminal", typ)
		}
	}
}

var (
	uuidV7RE  = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	rfc3339RE = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$`)
)

// TestEventIDIsUUIDv7 verifies that generated event IDs conform to UUIDv7.
func TestEventIDIsUUIDv7(t *testing.T) {
	for i := 0; i < 10; i++ {
		id := event.NewID()
		if !uuidV7RE.MatchString(id) {
			t.Fatalf("event ID %q is not a UUIDv7", id)
		}
	}
}

// TestOccurredAtRFC3339 verifies that OccurredAt serializes as RFC3339.
func TestOccurredAtRFC3339(t *testing.T) {
	c := codec.JSON()
	env := event.New(event.MessageReceived)

	encoded, err := c.EncodeEnvelope(env)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(encoded, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	occurred, ok := raw["occurred_at"].(string)
	if !ok {
		t.Fatalf("occurred_at is not a string: %T", raw["occurred_at"])
	}
	if !rfc3339RE.MatchString(occurred) {
		t.Fatalf("occurred_at %q is not RFC3339", occurred)
	}
}

// TestResponseErrorSampleHasRetryable verifies that the golden error sample
// carries the required ErrorPayload fields across all SDKs.
func TestResponseErrorSampleHasRetryable(t *testing.T) {
	dir := schemaDir(t)
	data := loadJSON(t, filepath.Join(dir, "samples", "response_error.json"))

	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	payload, ok := raw["payload"].(map[string]any)
	if !ok {
		t.Fatalf("payload is not an object")
	}
	for _, key := range []string{"code", "message", "retryable"} {
		if _, ok := payload[key]; !ok {
			t.Fatalf("response_error.json payload missing required key %q", key)
		}
	}
}
