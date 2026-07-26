package bus_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ModulationAI/openagentio/pkg/bus"
	"github.com/ModulationAI/openagentio/pkg/event"
	"github.com/ModulationAI/openagentio/pkg/transport/inmem"
)

// newTestBus boots a Bus on an in-memory transport. Returned cleanup must be
// deferred by callers; it closes the bus and the underlying driver.
func newTestBus(t *testing.T, agentID string, opts ...bus.Option) (bus.Bus, func()) {
	t.Helper()
	tr := inmem.New()
	allOpts := append([]bus.Option{
		bus.WithAgentID(agentID),
		bus.WithTransport(tr),
		bus.WithDefaultTimeout(2 * time.Second),
	}, opts...)
	b, err := bus.New(allOpts...)
	if err != nil {
		t.Fatalf("bus.New: %v", err)
	}
	return b, func() { _ = b.Close() }
}

func TestPublishSubscribeRoundTrip(t *testing.T) {
	b, done := newTestBus(t, "pubsub-agent")
	defer done()

	got := make(chan *event.Envelope, 1)
	sub, err := b.Subscribe(context.Background(), event.MessageReceived, func(_ context.Context, e *event.Envelope) error {
		got <- e
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	out := event.New(event.MessageReceived)
	out.From = "tester"
	out.Payload = json.RawMessage(`{"text":"hi"}`)
	if err := b.Publish(context.Background(), out); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case e := <-got:
		if e.EventType != event.MessageReceived {
			t.Fatalf("event_type = %q", e.EventType)
		}
		if string(e.Payload) != `{"text":"hi"}` {
			t.Fatalf("payload = %q", string(e.Payload))
		}
		if e.EventID != out.EventID {
			t.Fatalf("event_id changed: %q vs %q", e.EventID, out.EventID)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for delivery")
	}
}

func TestInvokeRoundTrip(t *testing.T) {
	b, done := newTestBus(t, "invoke-agent")
	defer done()

	if err := b.HandleInvoke("echo", func(_ context.Context, e *event.Envelope) (any, error) {
		return map[string]any{"echo": json.RawMessage(e.Payload)}, nil
	}); err != nil {
		t.Fatalf("HandleInvoke: %v", err)
	}

	resp, err := b.Invoke(context.Background(), "echo", map[string]any{"msg": "ping"})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if resp.EventType != event.ResponseFinal {
		t.Fatalf("event_type = %q want %q", resp.EventType, event.ResponseFinal)
	}
	if !resp.IsFinal {
		t.Fatalf("is_final not set")
	}
	if resp.CorrelationID == "" {
		t.Fatal("correlation_id should be set to req.event_id")
	}
}

func TestInvokeHandlerMetadataMergesWithRequest(t *testing.T) {
	b, done := newTestBus(t, "merge-agent")
	defer done()

	if err := b.HandleInvoke("merge", func(_ context.Context, _ *event.Envelope) (any, error) {
		resp := event.New(event.ResponseFinal)
		resp.Metadata = map[string]any{"handler_key": "handler_value"}
		return resp, nil
	}); err != nil {
		t.Fatalf("HandleInvoke: %v", err)
	}

	req := event.NewRequest()
	req.Metadata = map[string]any{
		"request_key":  "request_value",
		"shared_key":   "request_value",
		"acp.internal": "must_be_filtered",
	}

	resp, err := b.Invoke(context.Background(), "merge", req)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	md := resp.Metadata
	if md == nil {
		t.Fatal("response metadata is nil")
	}
	if md["request_key"] != "request_value" {
		t.Fatalf("request_key not inherited: got %v", md["request_key"])
	}
	if md["handler_key"] != "handler_value" {
		t.Fatalf("handler_key missing: got %v", md["handler_key"])
	}
	if md["shared_key"] != "request_value" {
		t.Fatalf("shared_key should keep request value when handler does not override: got %v", md["shared_key"])
	}
	if _, ok := md["acp.internal"]; ok {
		t.Fatalf("acp.* key from request leaked into response")
	}
}

func TestInvokeHandlerMetadataOverridesRequest(t *testing.T) {
	b, done := newTestBus(t, "override-agent")
	defer done()

	if err := b.HandleInvoke("override", func(_ context.Context, _ *event.Envelope) (any, error) {
		resp := event.New(event.ResponseFinal)
		resp.Metadata = map[string]any{"shared_key": "handler_value", "acp.handler": "filtered"}
		return resp, nil
	}); err != nil {
		t.Fatalf("HandleInvoke: %v", err)
	}

	req := event.NewRequest()
	req.Metadata = map[string]any{"shared_key": "request_value"}

	resp, err := b.Invoke(context.Background(), "override", req)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	md := resp.Metadata
	if md["shared_key"] != "handler_value" {
		t.Fatalf("shared_key should be overridden by handler: got %v", md["shared_key"])
	}
	if _, ok := md["acp.handler"]; ok {
		t.Fatalf("acp.* key from handler leaked into response")
	}
}

func TestInvokeHandlerEmptyMetadataInheritsRequest(t *testing.T) {
	b, done := newTestBus(t, "empty-agent")
	defer done()

	if err := b.HandleInvoke("empty", func(_ context.Context, _ *event.Envelope) (any, error) {
		resp := event.New(event.ResponseFinal)
		resp.Metadata = map[string]any{}
		return resp, nil
	}); err != nil {
		t.Fatalf("HandleInvoke: %v", err)
	}

	req := event.NewRequest()
	req.Metadata = map[string]any{"request_key": "request_value"}

	resp, err := b.Invoke(context.Background(), "empty", req)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	md := resp.Metadata
	if md == nil || md["request_key"] != "request_value" {
		t.Fatalf("empty handler metadata should still inherit request metadata: got %v", md)
	}
}

func TestInvokeHandlerMetadataDoesNotMutateInputs(t *testing.T) {
	b, done := newTestBus(t, "no-mutate-agent")
	defer done()

	var handlerResp *event.Envelope
	if err := b.HandleInvoke("no-mutate", func(_ context.Context, _ *event.Envelope) (any, error) {
		resp := event.New(event.ResponseFinal)
		resp.Metadata = map[string]any{"handler_key": "handler_value"}
		handlerResp = resp
		return resp, nil
	}); err != nil {
		t.Fatalf("HandleInvoke: %v", err)
	}

	req := event.NewRequest()
	req.Metadata = map[string]any{"request_key": "request_value"}

	resp, err := b.Invoke(context.Background(), "no-mutate", req)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	// Mutate the merged response metadata and ensure the original input maps are untouched.
	resp.Metadata["new_key"] = "new_value"
	delete(resp.Metadata, "request_key")
	resp.Metadata["handler_key"] = "mutated"

	if _, ok := req.Metadata["new_key"]; ok {
		t.Fatalf("modifying response metadata mutated request.Metadata")
	}
	if req.Metadata["request_key"] != "request_value" {
		t.Fatalf("request.Metadata key was deleted or overwritten: got %v", req.Metadata)
	}
	if _, ok := handlerResp.Metadata["new_key"]; ok {
		t.Fatalf("modifying response metadata mutated handler-returned Metadata")
	}
	if handlerResp.Metadata["handler_key"] != "handler_value" {
		t.Fatalf("handler-returned Metadata key was overwritten: got %v", handlerResp.Metadata)
	}
}

func TestHandleInvokeDefaultsToTargetQueue(t *testing.T) {
	b, done := newTestBus(t, "invoke-agent")
	defer done()

	const N = 6
	hits := make([]int, 2)
	for i := range hits {
		idx := i
		if err := b.HandleInvoke("shared", func(_ context.Context, _ *event.Envelope) (any, error) {
			hits[idx]++
			return map[string]int{"handler": idx}, nil
		}); err != nil {
			t.Fatalf("HandleInvoke[%d]: %v", i, err)
		}
	}

	for i := 0; i < N; i++ {
		if _, err := b.Invoke(context.Background(), "shared", nil); err != nil {
			t.Fatalf("Invoke[%d]: %v", i, err)
		}
	}

	total := hits[0] + hits[1]
	if total != N {
		t.Fatalf("handler total = %d want %d; hits=%v", total, N, hits)
	}
	if hits[0] == 0 || hits[1] == 0 {
		t.Fatalf("target queue did not balance: %v", hits)
	}
}

func TestHandleInvokeExplicitEmptyQueueFanout(t *testing.T) {
	b, done := newTestBus(t, "invoke-agent")
	defer done()

	hits := make([]int, 2)
	for i := range hits {
		idx := i
		if err := b.HandleInvoke("fanout", func(_ context.Context, _ *event.Envelope) (any, error) {
			hits[idx]++
			return map[string]int{"handler": idx}, nil
		}, bus.WithHandleQueue("")); err != nil {
			t.Fatalf("HandleInvoke[%d]: %v", i, err)
		}
	}

	if _, err := b.Invoke(context.Background(), "fanout", nil); err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	if hits[0] != 1 || hits[1] != 1 {
		t.Fatalf("fanout hits = %v want [1 1]", hits)
	}
}

func TestInvokeHandlerErrorMapsToErrorEnvelope(t *testing.T) {
	b, done := newTestBus(t, "invoke-agent")
	defer done()

	if err := b.HandleInvoke("boom", func(_ context.Context, _ *event.Envelope) (any, error) {
		return nil, errBoom
	}); err != nil {
		t.Fatalf("HandleInvoke: %v", err)
	}

	resp, err := b.Invoke(context.Background(), "boom", nil)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if resp.EventType != event.ResponseError {
		t.Fatalf("event_type = %q want %q", resp.EventType, event.ResponseError)
	}
	if !resp.IsFinal {
		t.Fatalf("is_final not set on error response")
	}

	var p event.ErrorPayload
	if err := json.Unmarshal(resp.Payload, &p); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if p.Code != event.CodeAgentUnavailable {
		t.Fatalf("code = %q", p.Code)
	}
	if p.Message != errBoom.Error() {
		t.Fatalf("message = %q", p.Message)
	}
}

func TestSubscribeQueueGroupBalances(t *testing.T) {
	b, done := newTestBus(t, "queue-agent")
	defer done()

	const N = 6
	hits := make([]int, 2)
	for i := range hits {
		idx := i
		_, err := b.Subscribe(context.Background(), event.MessageReceived, func(_ context.Context, _ *event.Envelope) error {
			hits[idx]++
			return nil
		}, bus.WithQueue("workers"))
		if err != nil {
			t.Fatalf("Subscribe[%d]: %v", i, err)
		}
	}

	for i := 0; i < N; i++ {
		out := event.New(event.MessageReceived)
		if err := b.Publish(context.Background(), out); err != nil {
			t.Fatalf("Publish[%d]: %v", i, err)
		}
	}

	total := hits[0] + hits[1]
	if total != N {
		t.Fatalf("queue total = %d want %d", total, N)
	}
	if hits[0] == 0 || hits[1] == 0 {
		t.Fatalf("queue not balanced: %v", hits)
	}
}

func TestInvokeMissingTarget(t *testing.T) {
	b, done := newTestBus(t, "noop-agent")
	defer done()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	if _, err := b.Invoke(ctx, "nobody-home", nil); err == nil {
		t.Fatal("expected timeout error when no handler is registered")
	}
}

type stringErr string

func (e stringErr) Error() string { return string(e) }

const errBoom stringErr = "boom"

// TestEnvelopePreparerRunsBeforePublish covers the OTel-style hook on each
// outbound path (Publish, Invoke, StreamInvoke). The preparer stamps a
// well-known traceparent value; the receiving handler asserts it landed.
func TestEnvelopePreparerRunsBeforePublish(t *testing.T) {
	const want = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	prep := func(_ context.Context, e *event.Envelope) {
		e.Traceparent = want
	}

	t.Run("Publish", func(t *testing.T) {
		b, done := newTestBus(t, "pub-agent", bus.WithEnvelopePreparer(prep))
		defer done()

		got := make(chan *event.Envelope, 1)
		sub, err := b.Subscribe(context.Background(), event.MessageReceived,
			func(_ context.Context, e *event.Envelope) error { got <- e; return nil })
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		defer sub.Unsubscribe()

		out := event.New(event.MessageReceived)
		if err := b.Publish(context.Background(), out); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		select {
		case e := <-got:
			if e.Traceparent != want {
				t.Fatalf("Publish traceparent = %q want %q", e.Traceparent, want)
			}
		case <-time.After(time.Second):
			t.Fatal("Publish: handler not called")
		}
	})

	t.Run("Invoke", func(t *testing.T) {
		b, done := newTestBus(t, "inv-agent", bus.WithEnvelopePreparer(prep))
		defer done()

		got := make(chan *event.Envelope, 1)
		if err := b.HandleInvoke("ping", func(_ context.Context, e *event.Envelope) (any, error) {
			got <- e
			return map[string]string{"ok": "1"}, nil
		}); err != nil {
			t.Fatalf("HandleInvoke: %v", err)
		}

		if _, err := b.Invoke(context.Background(), "ping", nil); err != nil {
			t.Fatalf("Invoke: %v", err)
		}
		select {
		case e := <-got:
			if e.Traceparent != want {
				t.Fatalf("Invoke traceparent = %q want %q", e.Traceparent, want)
			}
		case <-time.After(time.Second):
			t.Fatal("Invoke: handler not called")
		}
	})

	t.Run("StreamInvoke", func(t *testing.T) {
		b, done := newTestBus(t, "str-agent", bus.WithEnvelopePreparer(prep))
		defer done()

		got := make(chan *event.Envelope, 1)
		if err := b.HandleStream("counter", func(_ context.Context, e *event.Envelope, w bus.StreamWriter) error {
			got <- e
			return w.Final(map[string]int{"n": 0})
		}); err != nil {
			t.Fatalf("HandleStream: %v", err)
		}

		s, err := b.StreamInvoke(context.Background(), "counter", nil)
		if err != nil {
			t.Fatalf("StreamInvoke: %v", err)
		}
		defer s.Close()
		// Drain to completion so handler returns cleanly.
		for range s.Events() {
		}
		select {
		case e := <-got:
			if e.Traceparent != want {
				t.Fatalf("StreamInvoke traceparent = %q want %q", e.Traceparent, want)
			}
		case <-time.After(time.Second):
			t.Fatal("StreamInvoke: handler not called")
		}
	})
}

// TestEnvelopePreparerOrderMatchesAppend asserts preparers run in
// registration order and a later preparer sees mutations from earlier ones.
func TestEnvelopePreparerOrderMatchesAppend(t *testing.T) {
	first := func(_ context.Context, e *event.Envelope) {
		e.Traceparent = "first"
	}
	second := func(_ context.Context, e *event.Envelope) {
		// Sees the value first wrote.
		e.Traceparent = e.Traceparent + "-second"
	}

	b, done := newTestBus(t, "order-agent",
		bus.WithEnvelopePreparer(first),
		bus.WithEnvelopePreparer(second))
	defer done()

	got := make(chan *event.Envelope, 1)
	sub, err := b.Subscribe(context.Background(), event.MessageReceived,
		func(_ context.Context, e *event.Envelope) error { got <- e; return nil })
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	if err := b.Publish(context.Background(), event.New(event.MessageReceived)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case e := <-got:
		if e.Traceparent != "first-second" {
			t.Fatalf("traceparent = %q want %q", e.Traceparent, "first-second")
		}
	case <-time.After(time.Second):
		t.Fatal("handler not called")
	}
}
