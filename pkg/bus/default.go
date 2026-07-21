package bus

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/ModulationAI/openagentio/pkg/codec"
	"github.com/ModulationAI/openagentio/pkg/event"
	"github.com/ModulationAI/openagentio/pkg/transport"
)

// ErrNotImplemented is returned by methods whose bodies are reserved for the
// upcoming v0.1 implementation milestones. Callers can
// `errors.Is(err, bus.ErrNotImplemented)` to detect skeleton calls in tests.
var ErrNotImplemented = errors.New("bus: not implemented (v0.1 skeleton)")

// ErrIdleTimeout is reported by a Stream when the gap between two received
// frames exceeds the InvokeOption WithIdleTimeout deadline.
var ErrIdleTimeout = errors.New("bus: stream idle timeout")

// ErrBackpressureDrop is reported by a Stream when the client-side
// out-of-order buffer would exceed WithMaxPendingFrames, or when a received
// frame's Seq jumps more than WithMaxSequenceGap ahead of the currently
// expected Seq. This maps to the BACKPRESSURE_DROP error code on the wire.
var ErrBackpressureDrop = errors.New("bus: stream backpressure drop")

// New constructs a Bus from the supplied options. It validates the required
// inputs (Transport, AgentID) and connects the underlying transport.
func New(opts ...Option) (Bus, error) {
	o := Options{
		SubjectPrefix:  DefaultSubjectPrefix,
		Codec:          codec.JSON(),
		Logger:         slog.Default(),
		DefaultTimeout: 30 * time.Second,
	}
	for _, f := range opts {
		f(&o)
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Transport == nil {
		return nil, errors.New("bus: transport is required")
	}
	if o.AgentID == "" {
		return nil, errors.New("bus: agent id is required")
	}
	if err := o.Transport.Connect(context.Background()); err != nil {
		return nil, err
	}
	b := &defaultBus{opts: o}
	b.lifeCtx, b.cancel = context.WithCancel(context.Background())
	return b, nil
}

type defaultBus struct {
	opts Options

	mu      sync.Mutex
	owned   []transport.Subscription // bus-owned subs from HandleInvoke / HandleStream
	lifeCtx context.Context
	cancel  context.CancelFunc
	closed  bool
}

func (b *defaultBus) trackOwned(s transport.Subscription) {
	b.mu.Lock()
	b.owned = append(b.owned, s)
	b.mu.Unlock()
}

func (b *defaultBus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	owned := b.owned
	b.owned = nil
	b.mu.Unlock()

	if b.cancel != nil {
		b.cancel()
	}

	var firstErr error
	for _, s := range owned {
		if err := s.Unsubscribe(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if b.opts.Transport != nil {
		if err := b.opts.Transport.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// subscription is the bus-side wrapper around transport.Subscription. It
// satisfies bus.Subscription; the bus does not track its lifecycle (caller
// owns it and is expected to Unsubscribe).
type subscription struct {
	sub transport.Subscription
}

func (s *subscription) Unsubscribe() error { return s.sub.Unsubscribe() }

// prepareEnvelope runs every registered EnvelopePreparer against e, in
// registration order. It is the single chokepoint that outbound paths
// (Publish / Invoke / StreamInvoke) call just before EncodeEnvelope so a
// preparer like the OpenTelemetry bridge can stamp traceparent on the
// envelope without each call site reaching into the OTel API directly.
func (b *defaultBus) prepareEnvelope(ctx context.Context, e *event.Envelope) {
	if e == nil {
		return
	}
	for _, p := range b.opts.EnvelopePreparers {
		if p == nil {
			continue
		}
		p(ctx, e)
	}
}
