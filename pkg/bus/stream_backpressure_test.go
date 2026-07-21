package bus

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/ModulationAI/openagentio/pkg/codec"
	"github.com/ModulationAI/openagentio/pkg/event"
	"github.com/ModulationAI/openagentio/pkg/transport"
)

// fakeInbox is a scripted transport.Inbox that returns pre-encoded envelopes
// in order, then blocks. Enables white-box testing of the reorder buffer's
// backpressure behaviour without spinning up a full Bus + handler round-trip.
type fakeInbox struct {
	msgs  []*transport.RawMessage
	block chan struct{}
}

func newFakeInbox(envelopes []*event.Envelope, cd codec.Codec) *fakeInbox {
	msgs := make([]*transport.RawMessage, 0, len(envelopes))
	for _, env := range envelopes {
		data, err := cd.EncodeEnvelope(env)
		if err != nil {
			panic(err)
		}
		msgs = append(msgs, &transport.RawMessage{Data: data})
	}
	return &fakeInbox{msgs: msgs, block: make(chan struct{})}
}

func (f *fakeInbox) Subject() string { return "test.inbox" }

func (f *fakeInbox) Recv(ctx context.Context) (*transport.RawMessage, error) {
	if len(f.msgs) == 0 {
		// No more scripted frames — block until the caller cancels ctx or
		// closes the inbox, matching a real inbox's semantics.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-f.block:
			return nil, errors.New("fakeInbox: closed")
		}
	}
	m := f.msgs[0]
	f.msgs = f.msgs[1:]
	return m, nil
}

func (f *fakeInbox) Close() error {
	select {
	case <-f.block:
	default:
		close(f.block)
	}
	return nil
}

// buildEnvWithSeq constructs a minimal ResponseDelta envelope tagged with the
// given Seq. Payload contents are irrelevant to the reorder buffer.
func buildEnvWithSeq(seq uint64) *event.Envelope {
	e := event.New(event.ResponseDelta)
	e.Seq = seq
	return e
}

func newTestStream(t *testing.T, envs []*event.Envelope, opts ...InvokeOption) *stream {
	t.Helper()
	cd := codec.JSON()
	inbox := newFakeInbox(envs, cd)
	o := collectInvokeOpts(opts)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &stream{
		ctx:        ctx,
		cancel:     cancel,
		inbox:      inbox,
		codec:      cd,
		maxPending: resolveMaxPending(o.MaxPendingFrames),
		maxGap:     resolveMaxGap(o.MaxSequenceGap),
	}
}

func TestStreamHappyPathReordersFrames(t *testing.T) {
	// Frames arrive out of order: 2, 0, 1, final=3. Iterator must yield in
	// Seq order (0, 1, 2, 3).
	e0 := buildEnvWithSeq(0)
	e1 := buildEnvWithSeq(1)
	e2 := buildEnvWithSeq(2)
	e3 := buildEnvWithSeq(3)
	e3.EventType = event.ResponseFinal
	e3.IsFinal = true

	s := newTestStream(t, []*event.Envelope{e2, e0, e1, e3})

	var got []uint64
	for env, err := range s.Events() {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got = append(got, env.Seq)
	}
	want := []uint64{0, 1, 2, 3}
	if len(got) != len(want) {
		t.Fatalf("got seqs %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("seqs[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestStreamDropsDuplicateFrames(t *testing.T) {
	// Duplicate Seq=0 must be silently dropped; final at Seq=1 still fires.
	e0 := buildEnvWithSeq(0)
	e0dup := buildEnvWithSeq(0)
	e1 := buildEnvWithSeq(1)
	e1.EventType = event.ResponseFinal
	e1.IsFinal = true

	s := newTestStream(t, []*event.Envelope{e0, e0dup, e1})

	var count int
	for _, err := range s.Events() {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		count++
	}
	if count != 2 {
		t.Fatalf("got %d frames, want 2 (duplicate must be dropped)", count)
	}
}

func TestStreamBackpressureDropOnSequenceGap(t *testing.T) {
	// Seq=0 never arrives; Seq=100 with MaxSequenceGap=8 must terminate
	// with ErrBackpressureDrop before pending grows unbounded.
	e100 := buildEnvWithSeq(100)
	s := newTestStream(t, []*event.Envelope{e100}, WithMaxSequenceGap(8))

	var lastErr error
	var frames int
	for _, err := range s.Events() {
		if err != nil {
			lastErr = err
			break
		}
		frames++
	}
	if frames != 0 {
		t.Fatalf("expected no frames yielded before drop, got %d", frames)
	}
	if !errors.Is(lastErr, ErrBackpressureDrop) {
		t.Fatalf("got err %v, want ErrBackpressureDrop", lastErr)
	}
}

func TestStreamBackpressureDropOnPendingCap(t *testing.T) {
	// MaxPendingFrames=3 and Seq=0 is missing. After receiving 3 out-of-order
	// frames (Seq=1..3), a fourth (Seq=4) must trigger BACKPRESSURE_DROP.
	// Note: Seq=4 is within MaxSequenceGap (default 1024), so it's the
	// pending-cap guard that fires — this isolates the two conditions.
	envs := []*event.Envelope{
		buildEnvWithSeq(1),
		buildEnvWithSeq(2),
		buildEnvWithSeq(3),
		buildEnvWithSeq(4),
	}
	s := newTestStream(t, envs, WithMaxPendingFrames(3))

	var lastErr error
	var frames int
	for _, err := range s.Events() {
		if err != nil {
			lastErr = err
			break
		}
		frames++
	}
	if frames != 0 {
		t.Fatalf("expected no frames yielded (Seq=0 missing), got %d", frames)
	}
	if !errors.Is(lastErr, ErrBackpressureDrop) {
		t.Fatalf("got err %v, want ErrBackpressureDrop", lastErr)
	}
}

func TestStreamAcceptsExpectedFrameEvenWhenPendingFull(t *testing.T) {
	// If the buffer is at capacity but the incoming frame IS the expected
	// Seq, we must accept it — draining pending is the correct forward path.
	// Sequence: 1, 2, 3 (fills pending with cap=3), then 0 (expected) drains
	// everything, then 4 as final.
	e0 := buildEnvWithSeq(0)
	e1 := buildEnvWithSeq(1)
	e2 := buildEnvWithSeq(2)
	e3 := buildEnvWithSeq(3)
	e4 := buildEnvWithSeq(4)
	e4.EventType = event.ResponseFinal
	e4.IsFinal = true

	s := newTestStream(t, []*event.Envelope{e1, e2, e3, e0, e4}, WithMaxPendingFrames(3))

	var seqs []uint64
	for env, err := range s.Events() {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		seqs = append(seqs, env.Seq)
	}
	want := []uint64{0, 1, 2, 3, 4}
	if len(seqs) != len(want) {
		t.Fatalf("got seqs %v, want %v", seqs, want)
	}
	for i := range want {
		if seqs[i] != want[i] {
			t.Fatalf("seqs[%d] = %d, want %d", i, seqs[i], want[i])
		}
	}
}

func TestStreamGapCheckSafeNearMaxUint64(t *testing.T) {
	// Regression: `env.Seq >= expected + maxGap` overflows uint64 when
	// expected is near MaxUint64. For expected = MaxUint64 - 4 and
	// maxGap = 8, `expected + maxGap` wraps to 3, so ANY frame with
	// Seq >= 3 (i.e., any frame at all in the plausible range) would be
	// wrongly rejected. Verified by pre-seeding stream.expected past the
	// point where the additive expression wraps, then feeding a valid frame.
	//
	// The subtraction form `env.Seq - expected >= maxGap` is exactly what
	// we want because env.Seq >= expected is already guaranteed at this
	// point in the flow (late frames are filtered above).
	base := uint64(math.MaxUint64) - 4
	e := buildEnvWithSeq(base) // Seq == expected → happy path, expected+maxGap would wrap
	efinal := buildEnvWithSeq(base + 1)
	efinal.EventType = event.ResponseFinal
	efinal.IsFinal = true

	s := newTestStream(t, []*event.Envelope{e, efinal}, WithMaxSequenceGap(8))
	s.expected = base

	var seqs []uint64
	for env, err := range s.Events() {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		seqs = append(seqs, env.Seq)
	}
	want := []uint64{base, base + 1}
	if len(seqs) != len(want) {
		t.Fatalf("got seqs %v, want %v", seqs, want)
	}
	for i := range want {
		if seqs[i] != want[i] {
			t.Fatalf("seqs[%d] = %d, want %d", i, seqs[i], want[i])
		}
	}
}

func TestStreamGapCheckRejectsOverGapNearMaxUint64(t *testing.T) {
	// The mirror of TestStreamGapCheckSafeNearMaxUint64: a frame that is
	// genuinely more than maxGap ahead of expected must still be rejected
	// even when the arithmetic sits near the uint64 boundary. Here
	// expected = MaxUint64 - 100, maxGap = 8, incoming Seq = MaxUint64
	// (differ by 100 > 8) → BACKPRESSURE_DROP.
	expected := uint64(math.MaxUint64) - 100
	over := uint64(math.MaxUint64)

	s := newTestStream(t, []*event.Envelope{buildEnvWithSeq(over)}, WithMaxSequenceGap(8))
	s.expected = expected

	var lastErr error
	var frames int
	for _, err := range s.Events() {
		if err != nil {
			lastErr = err
			break
		}
		frames++
	}
	if frames != 0 {
		t.Fatalf("expected no frames before drop, got %d", frames)
	}
	if !errors.Is(lastErr, ErrBackpressureDrop) {
		t.Fatalf("got err %v, want ErrBackpressureDrop", lastErr)
	}
}
