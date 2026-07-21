package bus

// End-to-end regression for P0#3 seq-reservation fix.
//
// Before the fix: a failed terminal publish reserved a Seq that never landed
// on the wire, and the runtime's fallback Error(...) then consumed the *next*
// Seq. The client's reorder buffer accepted the fallback frame but waited
// forever for the reserved-but-missing Seq — the stream would idle-timeout
// even though a terminal frame had reached the client. This test exercises
// that path over a real Bus + inmem transport wired together, so any
// regression in the seq-reuse path surfaces as an idle timeout.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ModulationAI/openagentio/pkg/codec"
	"github.com/ModulationAI/openagentio/pkg/event"
	"github.com/ModulationAI/openagentio/pkg/transport"
	"github.com/ModulationAI/openagentio/pkg/transport/inmem"
)

// failFinalReplyTransport wraps an inmem driver and fails the first Publish
// carrying a ResponseFinal envelope (i.e. the handler's terminal frame). Every
// other Publish — invoke request, Started/Delta reply frames, fallback Error
// frame — is delegated to the inner driver.
type failFinalReplyTransport struct {
	inner *inmem.Driver

	mu        sync.Mutex
	tripped   bool
	failCount int // for assertions
	okCount   int // reply-subject publishes that went through
}

func (t *failFinalReplyTransport) Connect(ctx context.Context) error { return t.inner.Connect(ctx) }
func (t *failFinalReplyTransport) Close() error                      { return t.inner.Close() }
func (t *failFinalReplyTransport) Capabilities() transport.Capabilities {
	return t.inner.Capabilities()
}
func (t *failFinalReplyTransport) Publish(ctx context.Context, msg *transport.RawMessage) error {
	if strings.HasPrefix(msg.Subject, "_INBOX.") {
		// Decode the envelope so we can trip on ResponseFinal specifically —
		// blanket-failing every reply frame would drop Started/Delta too and
		// mask the exact scenario we're testing.
		env, err := codec.JSON().DecodeEnvelope(msg.Data)
		if err == nil && env.EventType == event.ResponseFinal {
			t.mu.Lock()
			if !t.tripped {
				t.tripped = true
				t.failCount++
				t.mu.Unlock()
				return errors.New("transport: intentional Final publish failure")
			}
			t.mu.Unlock()
		}
		t.mu.Lock()
		t.okCount++
		t.mu.Unlock()
	}
	return t.inner.Publish(ctx, msg)
}
func (t *failFinalReplyTransport) Subscribe(ctx context.Context, subject, queue string, h transport.Handler) (transport.Subscription, error) {
	return t.inner.Subscribe(ctx, subject, queue, h)
}
func (t *failFinalReplyTransport) Request(ctx context.Context, msg *transport.RawMessage) (*transport.RawMessage, error) {
	return t.inner.Request(ctx, msg)
}
func (t *failFinalReplyTransport) OpenInbox(ctx context.Context) (transport.Inbox, error) {
	return t.inner.OpenInbox(ctx)
}

// TestStreamInvoke_FailedFinal_FallbackErrorReachesClient pins the whole
// point of the seq-reservation fix: even when the handler's Final publish
// fails, the client receives the fallback Error frame AT THE RESERVED SEQ
// so the reorder buffer terminates cleanly instead of blocking on a
// permanent seq hole.
func TestStreamInvoke_FailedFinal_FallbackErrorReachesClient(t *testing.T) {
	tr := &failFinalReplyTransport{inner: inmem.New()}
	b, err := New(
		WithAgentID("test-agent"),
		WithTransport(tr),
		WithDefaultTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer b.Close()

	// Handler emits two Deltas (seq 0, 1) then Final(nil) at seq 2. The
	// Final publish trips the transport; the runtime's auto-terminal path
	// observes FAILED and lands a fallback Error at seq 2 (the reserved
	// seq). If the seq-reuse fix regresses the fallback would land at
	// seq=3 and the client's reorder buffer would idle-timeout.
	if err := b.HandleStream("failed-final", func(_ context.Context, _ *event.Envelope, w StreamWriter) error {
		if err := w.Delta("a"); err != nil {
			return err
		}
		if err := w.Delta("b"); err != nil {
			return err
		}
		// Return the Final error to the runtime so it takes the
		// FAILED -> fallback Error path. The regression symptom is
		// still an idle timeout below even if we swallowed the error
		// (auto-terminal would still see FAILED), but surfacing it
		// keeps the handler honest.
		return w.Final(nil)
	}); err != nil {
		t.Fatalf("HandleStream: %v", err)
	}

	// Tight idle timeout — the bug would idle-out here.
	s, err := b.StreamInvoke(
		context.Background(),
		"failed-final",
		nil,
		WithIdleTimeout(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("StreamInvoke: %v", err)
	}
	defer s.Close()

	var frames []*event.Envelope
	var frameErr error
	for env, err := range s.Events() {
		if err != nil {
			frameErr = err
			break
		}
		frames = append(frames, env)
	}

	// The stream must have terminated cleanly with a terminal frame.
	if frameErr != nil {
		t.Fatalf("stream errored (regression — likely idle timeout on seq hole): %v (got frames: %d)", frameErr, len(frames))
	}
	if len(frames) < 3 {
		t.Fatalf("received %d frames, want >=3 (2 deltas + terminal)", len(frames))
	}
	last := frames[len(frames)-1]
	if !last.IsFinal {
		t.Fatalf("last frame not IsFinal: %+v", last)
	}
	if last.EventType != event.ResponseError {
		t.Fatalf("last event type = %q, want %q", last.EventType, event.ResponseError)
	}

	// The core seq-reservation assertion: fallback landed at seq 2, not 3.
	if last.Seq != 2 {
		t.Fatalf("terminal Seq = %d, want 2 (reused reservation from failed Final)", last.Seq)
	}
	// Deltas must have arrived contiguously first.
	for i := 0; i < len(frames)-1; i++ {
		if frames[i].Seq != uint64(i) {
			t.Fatalf("frame[%d].Seq = %d, want %d", i, frames[i].Seq, i)
		}
	}

	// The transport should have tripped exactly once (only ResponseFinal
	// was targeted). The successful okCount includes the deltas AND the
	// fallback Error.
	tr.mu.Lock()
	failCount, okCount, tripped := tr.failCount, tr.okCount, tr.tripped
	tr.mu.Unlock()
	if !tripped || failCount != 1 {
		t.Fatalf("failFinalReplyTransport: tripped=%v failCount=%d, want tripped=true failCount=1", tripped, failCount)
	}
	if okCount < 3 {
		t.Fatalf("failFinalReplyTransport: okCount=%d, want >=3 (2 deltas + fallback error)", okCount)
	}
}
