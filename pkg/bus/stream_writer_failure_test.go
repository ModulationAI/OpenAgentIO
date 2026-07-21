package bus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/ModulationAI/openagentio/pkg/codec"
	"github.com/ModulationAI/openagentio/pkg/event"
	"github.com/ModulationAI/openagentio/pkg/transport"
)

// P0#3 regression suite for StreamWriter terminal-state handling.
//
// Prior to P0#3 the writer flipped `closed=true` before encode+publish, so a
// codec/transport failure left the writer wedged in `closed` with no terminal
// frame on the wire — the peer had no way to distinguish "handler done" from
// "handler exploded mid-terminate" and would only find out via idle timeout.
// These tests exercise the state machine directly.

// --- fakes -------------------------------------------------------------------

// scriptedTransport is a transport whose Publish() delegates to an override
// function. Only Publish is implemented; every other method panics — this test
// suite never calls them.
type scriptedTransport struct {
	mu        sync.Mutex
	published []*transport.RawMessage
	publishFn func(subject string, data []byte) error
}

func (s *scriptedTransport) Connect(context.Context) error { return nil }
func (s *scriptedTransport) Close() error                  { return nil }
func (s *scriptedTransport) Capabilities() transport.Capabilities {
	return transport.Capabilities{}
}
func (s *scriptedTransport) Publish(ctx context.Context, msg *transport.RawMessage) error {
	s.mu.Lock()
	fn := s.publishFn
	s.mu.Unlock()
	if fn != nil {
		if err := fn(msg.Subject, msg.Data); err != nil {
			return err
		}
	}
	s.mu.Lock()
	// Copy the raw message so later mutations by the caller (if any) don't
	// change what tests observe.
	dup := &transport.RawMessage{Subject: msg.Subject, Data: append([]byte(nil), msg.Data...)}
	s.published = append(s.published, dup)
	s.mu.Unlock()
	return nil
}
func (s *scriptedTransport) Subscribe(context.Context, string, string, transport.Handler) (transport.Subscription, error) {
	panic("not used")
}
func (s *scriptedTransport) Request(context.Context, *transport.RawMessage) (*transport.RawMessage, error) {
	panic("not used")
}
func (s *scriptedTransport) OpenInbox(context.Context) (transport.Inbox, error) {
	panic("not used")
}

func (s *scriptedTransport) snapshot() []*transport.RawMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*transport.RawMessage, len(s.published))
	copy(out, s.published)
	return out
}

// failingPayloadCodec wraps codec.JSON and returns an error from EncodePayload
// when the input is a marker sentinel. Envelope encoding still works, so the
// writer can distinguish "payload encode failure" from "envelope encode
// failure".
type failingPayloadCodec struct {
	inner codec.Codec
	trip  any
}

func (c *failingPayloadCodec) Name() string { return c.inner.Name() }
func (c *failingPayloadCodec) EncodeEnvelope(e *event.Envelope) ([]byte, error) {
	return c.inner.EncodeEnvelope(e)
}
func (c *failingPayloadCodec) DecodeEnvelope(data []byte) (*event.Envelope, error) {
	return c.inner.DecodeEnvelope(data)
}
func (c *failingPayloadCodec) EncodePayload(v any) (json.RawMessage, error) {
	if v == c.trip {
		return nil, errors.New("codec: intentional payload encode failure")
	}
	return c.inner.EncodePayload(v)
}
func (c *failingPayloadCodec) DecodePayload(raw json.RawMessage, v any) error {
	return c.inner.DecodePayload(raw, v)
}

// --- helpers -----------------------------------------------------------------

func newTestBusForWriter(t *testing.T, tr transport.Transport, cd codec.Codec, logBuf *bytes.Buffer) *defaultBus {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return &defaultBus{
		opts: Options{
			AgentID:       "test-agent",
			SubjectPrefix: DefaultSubjectPrefix,
			Codec:         cd,
			Transport:     tr,
			Logger:        logger,
		},
	}
}

func newTestRequest() *event.Envelope {
	req := event.New(event.MessageReceived)
	req.EventID = "corr-1"
	req.From = "caller"
	req.To = "stream-target"
	req.ReplyTo = "reply.inbox.1"
	return req
}

// --- Final: payload codec failure -------------------------------------------

func TestStreamWriter_Final_PayloadCodecFailure_TransitionsToFailed(t *testing.T) {
	// Before P0#3: encoding failure inside Final() would return the codec
	// error, but writer.closed was already true and w.seq was already
	// consumed, so the reorder buffer on the client would wait forever for
	// the reserved-but-never-published Seq. Now: Final returns the error,
	// state = failed, and the runtime can fall back to Error(...).
	tr := &scriptedTransport{}
	sentinel := struct{ marker string }{marker: "boom"}
	cd := &failingPayloadCodec{inner: codec.JSON(), trip: sentinel}
	b := newTestBusForWriter(t, tr, cd, &bytes.Buffer{})
	w := &streamWriter{bus: b, ctx: context.Background(), req: newTestRequest()}

	if err := w.Final(sentinel); err == nil {
		t.Fatalf("Final: expected codec error, got nil")
	}
	if got := w.stateSnapshot(); got != writerFailed {
		t.Fatalf("state = %v, want writerFailed", got)
	}
	if got := len(tr.snapshot()); got != 0 {
		t.Fatalf("published %d frames, want 0 (encode failed)", got)
	}
	// Recovery: caller may still land a terminal error frame.
	if err := w.Error(errors.New("handler observed codec failure")); err != nil {
		t.Fatalf("recovery Error() should succeed after failed Final: %v", err)
	}
	if got := w.stateSnapshot(); got != writerClosed {
		t.Fatalf("post-recovery state = %v, want writerClosed", got)
	}
	if got := len(tr.snapshot()); got != 1 {
		t.Fatalf("published %d frames, want 1 (recovery error)", got)
	}
}

// --- Final: transport publish failure ---------------------------------------

func TestStreamWriter_Final_PublishFailure_ReturnsErrorAndAllowsRecovery(t *testing.T) {
	// A transport-level publish failure during Final() must (a) return the
	// error to the handler, (b) leave the writer in `failed`, and (c) permit
	// a fallback Error() to still land a terminal frame.
	var failNext atomicOnce
	tr := &scriptedTransport{}
	tr.publishFn = func(_ string, _ []byte) error {
		if failNext.take() {
			return errors.New("transport: intentional failure")
		}
		return nil
	}
	b := newTestBusForWriter(t, tr, codec.JSON(), &bytes.Buffer{})
	w := &streamWriter{bus: b, ctx: context.Background(), req: newTestRequest()}

	failNext.arm()
	if err := w.Final(nil); err == nil {
		t.Fatalf("Final: expected transport error, got nil")
	}
	if got := w.stateSnapshot(); got != writerFailed {
		t.Fatalf("state = %v, want writerFailed", got)
	}
	// Recovery Error() must land on the wire (publishFn no longer trips).
	if err := w.Error(errors.New("handler surfaces failure")); err != nil {
		t.Fatalf("recovery Error(): %v", err)
	}
	if got := w.stateSnapshot(); got != writerClosed {
		t.Fatalf("post-recovery state = %v, want writerClosed", got)
	}
	pub := tr.snapshot()
	if len(pub) != 1 {
		t.Fatalf("published %d frames, want 1", len(pub))
	}
	if !bytes.Contains(pub[0].Data, []byte(event.ResponseError)) {
		t.Fatalf("recovery frame did not carry ResponseError: %s", pub[0].Data)
	}
}

// --- Error: publish failure is surfaced -------------------------------------

func TestStreamWriter_Error_PublishFailure_ReturnsError(t *testing.T) {
	// Pre-P0#3 Error swallowed the codec error and never returned it, so a
	// publish failure looked like success to handleStream. Now the caller
	// gets the error and can log/react.
	tr := &scriptedTransport{}
	tr.publishFn = func(_ string, _ []byte) error {
		return errors.New("transport: intentional failure")
	}
	b := newTestBusForWriter(t, tr, codec.JSON(), &bytes.Buffer{})
	w := &streamWriter{bus: b, ctx: context.Background(), req: newTestRequest()}

	err := w.Error(errors.New("handler err"))
	if err == nil {
		t.Fatalf("Error: expected transport error, got nil")
	}
	if got := w.stateSnapshot(); got != writerFailed {
		t.Fatalf("state = %v, want writerFailed", got)
	}
}

// --- Error: nil argument is rejected ----------------------------------------

func TestStreamWriter_Error_NilArgument_Rejected(t *testing.T) {
	// Pre-P0#3 Error(nil) would panic on srcErr.Error() dereferences.
	// Now it returns a validation error instead.
	tr := &scriptedTransport{}
	b := newTestBusForWriter(t, tr, codec.JSON(), &bytes.Buffer{})
	w := &streamWriter{bus: b, ctx: context.Background(), req: newTestRequest()}

	if err := w.Error(nil); err == nil {
		t.Fatalf("Error(nil): expected validation error, got nil")
	}
	if got := w.stateSnapshot(); got != writerOpen {
		t.Fatalf("state = %v, want writerOpen (Error(nil) should not change state)", got)
	}
}

// --- Post-terminal rejection ------------------------------------------------

func TestStreamWriter_AfterFinal_RejectsFurtherFrames(t *testing.T) {
	tr := &scriptedTransport{}
	b := newTestBusForWriter(t, tr, codec.JSON(), &bytes.Buffer{})
	w := &streamWriter{bus: b, ctx: context.Background(), req: newTestRequest()}

	if err := w.Final(nil); err != nil {
		t.Fatalf("Final: %v", err)
	}
	if err := w.Delta("nope"); err == nil {
		t.Fatalf("Delta after Final should have failed")
	}
	if err := w.Final(nil); err == nil {
		t.Fatalf("second Final should have failed")
	}
	if err := w.Error(errors.New("x")); err == nil {
		t.Fatalf("Error after successful Final should have failed")
	}
	if got := w.stateSnapshot(); got != writerClosed {
		t.Fatalf("state = %v, want writerClosed", got)
	}
}

// --- handleStream fallback: FAILED → synthesized Error ---------------------

func TestHandleStream_HandlerFinalFailed_EmitsFallbackError(t *testing.T) {
	// End-to-end: the handler successfully calls Final(...), but the very
	// first publish call fails. handleStream must observe the FAILED state
	// and emit a fallback Error frame so the client's reorder buffer
	// terminates instead of idling out.
	logBuf := &bytes.Buffer{}
	var failFirst atomicOnce
	tr := &scriptedTransport{}
	tr.publishFn = func(_ string, _ []byte) error {
		if failFirst.take() {
			return errors.New("transport: first-publish failure")
		}
		return nil
	}
	b := newTestBusForWriter(t, tr, codec.JSON(), logBuf)
	b.lifeCtx, b.cancel = context.WithCancel(context.Background())
	defer b.cancel()

	failFirst.arm()
	handler := func(_ context.Context, _ *event.Envelope, w StreamWriter) error {
		// This Final() will fail internally; the handler surfaces the error.
		return w.Final(nil)
	}
	b.handleStream(newTestRequest(), handler)

	// Two publish attempts happen: the failed Final and the fallback Error.
	// Only the successful Error frame lands in tr.published.
	pub := tr.snapshot()
	if len(pub) != 1 {
		t.Fatalf("published %d frames, want 1 (fallback error)", len(pub))
	}
	if !bytes.Contains(pub[0].Data, []byte(event.ResponseError)) {
		t.Fatalf("fallback frame did not carry ResponseError: %s", pub[0].Data)
	}
}

func TestHandleStream_FallbackErrorAlsoFails_LogsStructured(t *testing.T) {
	// Pathological case: both the handler's Final and the fallback Error
	// fail. handleStream must log a structured error line with the request
	// identifiers so operators can correlate — never swallow silently.
	logBuf := &bytes.Buffer{}
	tr := &scriptedTransport{}
	tr.publishFn = func(_ string, _ []byte) error {
		return errors.New("transport: total outage")
	}
	b := newTestBusForWriter(t, tr, codec.JSON(), logBuf)
	b.lifeCtx, b.cancel = context.WithCancel(context.Background())
	defer b.cancel()

	handler := func(_ context.Context, _ *event.Envelope, w StreamWriter) error {
		return w.Final(nil)
	}
	b.handleStream(newTestRequest(), handler)

	log := logBuf.String()
	if !strings.Contains(log, "correlation_id=corr-1") {
		t.Fatalf("log missing correlation_id: %s", log)
	}
	if !strings.Contains(log, "reply_to=reply.inbox.1") {
		t.Fatalf("log missing reply_to: %s", log)
	}
	if !strings.Contains(log, "publish_err") {
		t.Fatalf("log missing publish_err: %s", log)
	}
}

// --- Seq reuse: fallback Error after failed Final must not open a hole ------

// The reorder buffer on the client waits for a contiguous Seq. Prior to this
// fix, the failed Final reserved (say) Seq=1, then the runtime's fallback
// Error consumed Seq=2 — the client accepted Seq=2 into pending but Seq=1
// was never on the wire and never would be, so the stream idle-timed out.
// The fix: FAILED -> Error() reuses the reserved seq.
func TestStreamWriter_FallbackError_ReusesReservedSeq(t *testing.T) {
	// Unit test on the writer directly. We arrange for the Final publish
	// to fail (but earlier Deltas succeed) and the fallback Error to
	// succeed, then assert the fallback frame landed at the same Seq as
	// the failed Final.
	tr := &scriptedTransport{}
	var attempts []uint64
	var attemptsMu sync.Mutex
	// Fail only when we see a ResponseFinal frame; let everything else
	// (Started/Delta/fallback-Error) through. This targets the exact
	// scenario the user complaint pins on: a Final that fails to publish
	// must not leave a seq hole for a following Error to widen.
	tr.publishFn = func(_ string, data []byte) error {
		env, err := codec.JSON().DecodeEnvelope(data)
		if err != nil {
			t.Fatalf("decode envelope in publishFn: %v", err)
		}
		attemptsMu.Lock()
		attempts = append(attempts, env.Seq)
		attemptsMu.Unlock()
		if env.EventType == event.ResponseFinal {
			return errors.New("transport: intentional Final publish failure")
		}
		return nil
	}
	b := newTestBusForWriter(t, tr, codec.JSON(), &bytes.Buffer{})
	w := &streamWriter{bus: b, ctx: context.Background(), req: newTestRequest()}

	// Consume a couple of Deltas so the reserved Seq is not 0 (0 would
	// mask a bug where reservedValid is not respected and the reservation
	// silently defaults to zero).
	if err := w.Delta("a"); err != nil {
		t.Fatalf("Delta a: %v", err)
	}
	if err := w.Delta("b"); err != nil {
		t.Fatalf("Delta b: %v", err)
	}

	if err := w.Final(nil); err == nil {
		t.Fatalf("Final: expected transport error, got nil")
	}
	if got := w.stateSnapshot(); got != writerFailed {
		t.Fatalf("state after failed Final = %v, want writerFailed", got)
	}

	if err := w.Error(errors.New("fallback")); err != nil {
		t.Fatalf("fallback Error: %v", err)
	}

	attemptsMu.Lock()
	defer attemptsMu.Unlock()
	// Attempts: Delta seq=0, Delta seq=1, failed Final seq=2, fallback Error seq=2.
	wantSeqs := []uint64{0, 1, 2, 2}
	if len(attempts) != len(wantSeqs) {
		t.Fatalf("publish attempts = %v, want %v", attempts, wantSeqs)
	}
	for i, want := range wantSeqs {
		if attempts[i] != want {
			t.Fatalf("publish attempt[%d] Seq = %d, want %d (full: %v)", i, attempts[i], want, attempts)
		}
	}
	// Only the successful publishes land in tr.published: 2 deltas + 1 Error.
	pub := tr.snapshot()
	if len(pub) != 3 {
		t.Fatalf("published %d frames, want 3 (2 deltas + fallback error)", len(pub))
	}
}

// TestStreamWriter_MultipleFailedFinals_KeepReservedSeq documents the (rare
// but possible) case where a caller invokes Final twice: the writer is in
// FAILED after the first attempt, so the second Final is rejected. If
// something later drives an Error, it must still use the *first* Final's
// reserved seq.
func TestStreamWriter_FailedFinal_ThenErrorReusesFirstReservedSeq(t *testing.T) {
	tr := &scriptedTransport{}
	tr.publishFn = func(_ string, _ []byte) error {
		return errors.New("transport: total outage")
	}
	b := newTestBusForWriter(t, tr, codec.JSON(), &bytes.Buffer{})
	w := &streamWriter{bus: b, ctx: context.Background(), req: newTestRequest()}

	if err := w.Final(nil); err == nil {
		t.Fatalf("Final: expected transport error, got nil")
	}
	// A second Final is rejected by state (writer is FAILED); it must not
	// silently allocate a new seq or overwrite the reservation.
	if err := w.Final(nil); err == nil {
		t.Fatalf("second Final: expected state error, got nil")
	}

	// Now flip publish back on and try an Error(). Assert it uses the
	// reserved seq (0), not the next-fresh seq.
	tr.mu.Lock()
	tr.publishFn = nil
	tr.mu.Unlock()

	if err := w.Error(errors.New("fallback")); err != nil {
		t.Fatalf("Error: %v", err)
	}
	pub := tr.snapshot()
	if len(pub) != 1 {
		t.Fatalf("published %d frames, want 1", len(pub))
	}
	env, err := codec.JSON().DecodeEnvelope(pub[0].Data)
	if err != nil {
		t.Fatalf("decode published: %v", err)
	}
	if env.Seq != 0 {
		t.Fatalf("fallback error Seq = %d, want 0 (reused reservation)", env.Seq)
	}
}

// once and then always false. Avoids importing sync/atomic for a one-shot.
type atomicOnce struct {
	mu   sync.Mutex
	on   bool
	used bool
}

func (a *atomicOnce) arm() {
	a.mu.Lock()
	a.on = true
	a.used = false
	a.mu.Unlock()
}

func (a *atomicOnce) take() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.on && !a.used {
		a.used = true
		return true
	}
	return false
}
