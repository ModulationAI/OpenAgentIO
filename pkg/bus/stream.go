package bus

import (
	"context"
	"errors"
	"iter"
	"sync"
	"time"

	"github.com/ModulationAI/openagentio/pkg/codec"
	"github.com/ModulationAI/openagentio/pkg/event"
	"github.com/ModulationAI/openagentio/pkg/middleware"
	"github.com/ModulationAI/openagentio/pkg/transport"
)

// StreamInvoke publishes a request to {prefix}.invoke.{target} with a fresh
// _INBOX as reply_to and returns a Stream over the received frames. Frames
// are reordered by Envelope.Seq; the iterator terminates after a frame with
// IsFinal=true, or earlier on idle/overall timeout.
func (b *defaultBus) StreamInvoke(ctx context.Context, target string, payload any, opts ...InvokeOption) (Stream, error) {
	if target == "" {
		return nil, errors.New("bus: empty invoke target")
	}
	o := collectInvokeOpts(opts)
	timeout := o.Timeout
	if timeout == 0 {
		timeout = b.opts.DefaultTimeout
	}

	streamCtx, cancel := context.WithCancel(ctx)
	if timeout > 0 {
		streamCtx, cancel = context.WithTimeout(ctx, timeout)
	}

	env, err := b.buildRequestEnvelope(target, payload)
	if err != nil {
		cancel()
		return nil, err
	}

	inbox, err := b.opts.Transport.OpenInbox(streamCtx)
	if err != nil {
		cancel()
		return nil, err
	}
	env.ReplyTo = inbox.Subject()

	b.prepareEnvelope(streamCtx, env)
	data, err := b.opts.Codec.EncodeEnvelope(env)
	if err != nil {
		_ = inbox.Close()
		cancel()
		return nil, err
	}
	if err := b.opts.Transport.Publish(streamCtx, &transport.RawMessage{
		Subject: b.invokeSubject(target, b.resolveTenant(env.TenantID)),
		Data:    data,
	}); err != nil {
		_ = inbox.Close()
		cancel()
		return nil, err
	}

	return &stream{
		ctx:        streamCtx,
		cancel:     cancel,
		inbox:      inbox,
		codec:      b.opts.Codec,
		idle:       o.IdleTimeout,
		maxPending: resolveMaxPending(o.MaxPendingFrames),
		maxGap:     resolveMaxGap(o.MaxSequenceGap),
	}, nil
}

func resolveMaxPending(n int) int {
	if n <= 0 {
		return DefaultMaxPendingFrames
	}
	return n
}

func resolveMaxGap(g uint64) uint64 {
	if g == 0 {
		return DefaultMaxSequenceGap
	}
	return g
}

// HandleStream subscribes to {prefix}.invoke.{target} and dispatches each
// request into a goroutine running the supplied handler. A StreamWriter is
// provided that publishes started/delta/final/error frames back to
// req.ReplyTo with monotonically increasing Seq numbers. If the handler
// returns without calling Final or Error, the runtime auto-emits one based on
// the returned error.
func (b *defaultBus) HandleStream(target string, h StreamHandler, opts ...HandleOption) error {
	if target == "" {
		return errors.New("bus: empty invoke target")
	}
	if h == nil {
		return errors.New("bus: nil stream handler")
	}
	o := collectHandleOpts(opts)
	if !o.QueueSet {
		o.Queue = target
	}
	subject := b.invokeSubject(target, b.opts.Tenant)

	dispatch := func(_ context.Context, msg *transport.RawMessage) error {
		req, err := b.opts.Codec.DecodeEnvelope(msg.Data)
		if err != nil {
			return err
		}
		if req.ReplyTo == "" {
			return errors.New("bus: stream request missing reply_to")
		}
		go b.handleStream(req, h)
		return nil
	}

	sub, err := b.opts.Transport.Subscribe(b.lifeCtx, subject, o.Queue, dispatch)
	if err != nil {
		return err
	}
	b.trackOwned(sub)
	return nil
}

func (b *defaultBus) handleStream(req *event.Envelope, h StreamHandler) {
	ctx, cancel := context.WithCancel(b.lifeCtx)
	defer cancel()

	w := &streamWriter{
		bus: b,
		ctx: ctx,
		req: req,
	}

	chained := middleware.Chain(middleware.Handler(func(c context.Context, e *event.Envelope) error {
		return h(c, e, w)
	}), b.opts.Middleware...)

	herr := chained(ctx, req)

	// The auto-terminal logic must handle every writer state — not just
	// `open`. If the handler called Final/Error and it succeeded, the writer
	// is `closed` and there is nothing to do. If Final/Error was attempted
	// but publish failed (writer is `failed`), the handler already saw the
	// error but the peer is still waiting on a terminal frame — we must try
	// once more with a synthetic Error so the client doesn't idle out.
	switch w.stateSnapshot() {
	case writerClosed:
		return
	case writerFailed:
		// Handler's Final/Error left no terminal frame on the wire.
		// Attempt a fallback Error so the peer transitions to a terminal
		// state instead of blocking on the idle timeout. Use handler's
		// error if any, else surface the last publish failure explicitly.
		fallback := herr
		if fallback == nil {
			fallback = w.lastErr()
		}
		if fallback == nil {
			fallback = errors.New("stream: terminal publish failed")
		}
		if ferr := w.forceError(fallback); ferr != nil {
			b.opts.Logger.Error("bus: stream fallback error publish failed",
				"target", subjectTargetFrom(req),
				"correlation_id", req.EventID,
				"reply_to", req.ReplyTo,
				"handler_err", herrString(herr),
				"publish_err", ferr,
			)
		}
	default: // writerOpen — handler returned without terminating
		var termErr error
		if herr != nil {
			termErr = w.Error(herr)
		} else {
			termErr = w.Final(nil)
		}
		if termErr != nil {
			b.opts.Logger.Error("bus: stream auto-terminal failed",
				"target", subjectTargetFrom(req),
				"correlation_id", req.EventID,
				"reply_to", req.ReplyTo,
				"handler_err", herrString(herr),
				"publish_err", termErr,
			)
		}
	}
}

// herrString renders a possibly-nil handler error for structured logging.
func herrString(e error) string {
	if e == nil {
		return ""
	}
	return e.Error()
}

// subjectTargetFrom extracts the invoke target for logging. Falls back to the
// event type when the target is not available on the request envelope.
func subjectTargetFrom(req *event.Envelope) string {
	if req == nil {
		return ""
	}
	if req.To != "" {
		return req.To
	}
	return req.EventType
}

// --- client-side stream ------------------------------------------------------

type stream struct {
	ctx        context.Context
	cancel     context.CancelFunc
	inbox      transport.Inbox
	codec      codec.Codec
	idle       time.Duration
	maxPending int
	maxGap     uint64

	// expected is the next Seq the iterator will yield. Promoted to a field
	// (rather than a local in Events()) so white-box tests can start the
	// iterator near uint64 boundaries without having to feed 2^64 frames.
	expected uint64

	closeOnce sync.Once
	closeErr  error
}

func (s *stream) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		s.closeErr = s.inbox.Close()
	})
	return s.closeErr
}

func (s *stream) Events() iter.Seq2[*event.Envelope, error] {
	return func(yield func(*event.Envelope, error) bool) {
		pending := make(map[uint64]*event.Envelope)

		for {
			recvCtx := s.ctx
			var recvCancel context.CancelFunc
			if s.idle > 0 {
				recvCtx, recvCancel = context.WithTimeout(s.ctx, s.idle)
			}
			msg, err := s.inbox.Recv(recvCtx)
			if recvCancel != nil {
				recvCancel()
			}

			if err != nil {
				switch {
				case s.ctx.Err() != nil:
					yield(nil, s.ctx.Err())
				case s.idle > 0 && errors.Is(err, context.DeadlineExceeded):
					yield(nil, ErrIdleTimeout)
				default:
					yield(nil, err)
				}
				return
			}

			env, err := s.codec.DecodeEnvelope(msg.Data)
			if err != nil {
				yield(nil, err)
				return
			}

			if env.Seq < s.expected {
				continue // duplicate / late frame, drop
			}
			if _, dup := pending[env.Seq]; dup {
				continue
			}
			// Backpressure guards. Two conditions terminate the stream:
			//   1. Seq jumps too far ahead of the currently-expected one.
			//      This bounds unbounded pending accumulation when a low-Seq
			//      frame is permanently missing (or a malicious/buggy server
			//      injects a huge Seq).
			//   2. Pending buffer already at capacity and this frame is not
			//      the currently-expected Seq. If it *is* the expected one,
			//      we accept it — flushing it drains the buffer immediately.
			//
			// Gap check uses subtraction rather than `expected + maxGap` to
			// avoid uint64 overflow when expected is near MaxUint64: the
			// addition would wrap around and misclassify a valid frame as
			// over-gap. The `env.Seq < expected` branch above guarantees
			// `env.Seq - expected` never underflows here.
			if env.Seq-s.expected >= s.maxGap {
				yield(nil, ErrBackpressureDrop)
				return
			}
			if env.Seq != s.expected && len(pending) >= s.maxPending {
				yield(nil, ErrBackpressureDrop)
				return
			}
			pending[env.Seq] = env

			for {
				e, ok := pending[s.expected]
				if !ok {
					break
				}
				delete(pending, s.expected)
				s.expected++
				if !yield(e, nil) {
					return
				}
				if e.IsFinal {
					return
				}
			}
		}
	}
}

// --- server-side writer ------------------------------------------------------

// writerState models the lifecycle of a StreamWriter. Prior to this design
// the writer had a single `closed bool` that was flipped to true before the
// terminal encode+publish; a codec error or transport failure would then leave
// the writer wedged in `closed` with no terminal frame on the wire, and the
// peer would only notice via the idle timeout. The state machine below makes
// the "publish in progress" and "publish failed" cases first-class so the
// runtime can react (fallback error, observability, mutual exclusion of
// concurrent terminal calls) instead of silently swallowing the error.
type writerState int

const (
	writerOpen    writerState = iota // Started/Delta permitted; no terminal frame yet
	writerClosing                    // Terminal frame reserved a seq; publish in flight
	writerClosed                     // Terminal frame acknowledged by transport
	writerFailed                     // Terminal frame encode/publish failed; recoverable via forceError
)

type streamWriter struct {
	bus *defaultBus
	ctx context.Context
	req *event.Envelope

	mu      sync.Mutex
	seq     uint64
	started bool
	state   writerState
	// lastErr captures the last codec/publish failure so handleStream can
	// surface it when the handler returned nil but the writer is stuck.
	// Never read while the writer is `open`.
	lastErrVal error
	// reservedSeq holds the Seq that the failed terminal frame consumed.
	// When the writer transitions to `failed`, the seq that was allocated
	// for the doomed terminal frame has never reached the wire — so the
	// client's reorder buffer never saw it. A fallback Error() from FAILED
	// must reuse this seq (not consume a new one), otherwise the client
	// gets a permanent seq hole and idle-times out despite receiving a
	// terminal frame. `reservedValid` distinguishes "no reserved seq" from
	// "reserved seq is 0".
	reservedSeq   uint64
	reservedValid bool
}

func (w *streamWriter) Started(meta any) error {
	w.mu.Lock()
	if err := w.checkOpenLocked(); err != nil {
		w.mu.Unlock()
		return err
	}
	if w.started {
		w.mu.Unlock()
		return errors.New("stream: started already emitted")
	}
	w.started = true
	seq := w.nextSeqLocked()
	w.mu.Unlock()

	env := newReplyShell(w.bus.opts.AgentID, w.req, event.ResponseStarted)
	env.Seq = seq
	if meta != nil {
		data, err := w.bus.opts.Codec.EncodePayload(meta)
		if err != nil {
			return err
		}
		env.Payload = data
	}
	return w.publish(env)
}

func (w *streamWriter) Delta(chunk any) error {
	w.mu.Lock()
	if err := w.checkOpenLocked(); err != nil {
		w.mu.Unlock()
		return err
	}
	seq := w.nextSeqLocked()
	w.mu.Unlock()

	env := newReplyShell(w.bus.opts.AgentID, w.req, event.ResponseDelta)
	env.Seq = seq
	if chunk != nil {
		data, err := w.bus.opts.Codec.EncodePayload(chunk)
		if err != nil {
			return err
		}
		env.Payload = data
	}
	return w.publish(env)
}

// Final publishes a terminal ResponseFinal frame. Unlike the pre-P0#3
// implementation, the writer only transitions to `closed` after publish
// succeeds. If payload encoding or publish fails the writer transitions to
// `failed` and the error is returned to the caller so retry/fallback logic
// (e.g. handleStream's auto-terminal) can observe it. Callers may recover a
// failed writer by calling Error(...), which the state machine still permits.
func (w *streamWriter) Final(result any) error {
	w.mu.Lock()
	if err := w.checkOpenLocked(); err != nil {
		w.mu.Unlock()
		return err
	}
	w.state = writerClosing
	seq := w.nextSeqLocked()
	w.mu.Unlock()

	env := newReplyShell(w.bus.opts.AgentID, w.req, event.ResponseFinal)
	env.Seq = seq
	env.IsFinal = true
	if result != nil {
		data, err := w.bus.opts.Codec.EncodePayload(result)
		if err != nil {
			w.markFailedReserving(err, seq)
			return err
		}
		env.Payload = data
	}
	if err := w.publish(env); err != nil {
		w.markFailedReserving(err, seq)
		return err
	}
	w.markClosed()
	return nil
}

// Error publishes a terminal ResponseError frame. Like Final, the writer
// only reaches `closed` when publish succeeds; a publish failure leaves the
// writer in `failed` and returns the error. `Error(nil)` is a caller bug —
// there is no meaningful payload to encode — and we reject it explicitly
// rather than dereferencing a nil error further down.
func (w *streamWriter) Error(srcErr error) error {
	if srcErr == nil {
		return errors.New("stream: Error requires non-nil error")
	}

	w.mu.Lock()
	// Error is permitted from both `open` (handler decided to fail) and
	// `failed` (a prior Final couldn't reach the wire — try to publish
	// an error frame instead so the peer still sees a terminal).
	if w.state != writerOpen && w.state != writerFailed {
		st := w.state
		w.mu.Unlock()
		return terminalStateErrorFor(st)
	}
	// Reuse the reserved seq from a prior failed terminal so the client's
	// reorder buffer sees a contiguous sequence. If we allocated a fresh
	// seq here, the failed terminal's reserved-but-unpublished seq would
	// stay missing forever and the client would idle-time-out even though
	// this fallback Error frame reached it.
	var seq uint64
	if w.state == writerFailed && w.reservedValid {
		seq = w.reservedSeq
		w.reservedValid = false
	} else {
		seq = w.nextSeqLocked()
	}
	w.state = writerClosing
	w.mu.Unlock()

	env := newReplyShell(w.bus.opts.AgentID, w.req, event.ResponseError)
	env.Seq = seq
	env.IsFinal = true
	payload := event.ErrorPayload{
		Code:    event.CodeAgentUnavailable,
		Message: srcErr.Error(),
	}
	// Encode into a local var first — on encode failure we still transition
	// to `failed` so callers see a consistent state.
	data, err := w.bus.opts.Codec.EncodePayload(payload)
	if err != nil {
		w.markFailedReserving(err, seq)
		return err
	}
	env.Payload = data
	if err := w.publish(env); err != nil {
		w.markFailedReserving(err, seq)
		return err
	}
	w.markClosed()
	return nil
}

// forceError is the internal fallback path used by handleStream when the
// user's handler returned but the writer is in `failed`. It uses the same
// Error() code path so that concurrent guards and observability are shared.
func (w *streamWriter) forceError(src error) error {
	return w.Error(src)
}

func (w *streamWriter) checkOpenLocked() error {
	switch w.state {
	case writerOpen:
		return nil
	case writerClosing:
		return errors.New("stream: terminal frame in flight")
	case writerClosed:
		return errors.New("stream: already closed")
	case writerFailed:
		return errors.New("stream: writer failed; only Error is permitted")
	default:
		return errors.New("stream: unknown writer state")
	}
}

func terminalStateErrorFor(s writerState) error {
	switch s {
	case writerClosing:
		return errors.New("stream: terminal frame in flight")
	case writerClosed:
		return errors.New("stream: already closed")
	default:
		return errors.New("stream: writer in unexpected state")
	}
}

func (w *streamWriter) markClosed() {
	w.mu.Lock()
	w.state = writerClosed
	w.lastErrVal = nil
	// A successful terminal has consumed its seq; clear any prior
	// reservation so a defensive future call doesn't accidentally reuse it.
	w.reservedValid = false
	w.mu.Unlock()
}

func (w *streamWriter) markFailed(err error) {
	w.mu.Lock()
	w.state = writerFailed
	w.lastErrVal = err
	// No reserved seq — this path is for markers that don't own a seq
	// (currently unused after markFailedReserving was introduced, kept for
	// future callers that may want to fail without a seq context).
	w.reservedValid = false
	w.mu.Unlock()
}

// markFailedReserving is the failure path used by Final/Error: it records
// the seq that the failed terminal reserved so a later fallback Error can
// reuse it and avoid a seq hole in the client's reorder buffer.
func (w *streamWriter) markFailedReserving(err error, seq uint64) {
	w.mu.Lock()
	w.state = writerFailed
	w.lastErrVal = err
	w.reservedSeq = seq
	w.reservedValid = true
	w.mu.Unlock()
}

func (w *streamWriter) stateSnapshot() writerState {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.state
}

func (w *streamWriter) lastErr() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastErrVal
}

// isClosed retains its historical name for callers outside stream.go, but
// now means "reached a terminal state" (either `closed` or `failed`) — both
// mean the writer will not accept further Started/Delta frames.
func (w *streamWriter) isClosed() bool {
	s := w.stateSnapshot()
	return s == writerClosed || s == writerFailed
}

func (w *streamWriter) nextSeqLocked() uint64 {
	s := w.seq
	w.seq++
	return s
}

func (w *streamWriter) publish(env *event.Envelope) error {
	data, err := w.bus.opts.Codec.EncodeEnvelope(env)
	if err != nil {
		return err
	}
	return w.bus.opts.Transport.Publish(w.ctx, &transport.RawMessage{
		Subject: w.req.ReplyTo,
		Data:    data,
	})
}
