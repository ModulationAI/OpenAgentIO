package bus

import (
	"context"
	"log/slog"
	"time"

	"github.com/ModulationAI/openagentio/pkg/codec"
	"github.com/ModulationAI/openagentio/pkg/event"
	"github.com/ModulationAI/openagentio/pkg/middleware"
	"github.com/ModulationAI/openagentio/pkg/transport"
)

// DefaultSubjectPrefix is used when WithSubjectPrefix is not supplied.
// Override it via bus.WithSubjectPrefix("...") to coexist with legacy
// `agent.*` namespaces.
const DefaultSubjectPrefix = "acp.v1"

// EnvelopePreparer mutates an outbound envelope just before it is encoded
// onto the wire. The chain runs for every Bus.Publish / Invoke / StreamInvoke
// call, in registration order, so a later preparer observes the mutations of
// earlier ones. The canonical use case is the OpenTelemetry bridge in
// pkg/middleware/otel, which writes the active span's traceparent into
// envelope.Traceparent.
//
// Preparers must be cheap and non-blocking — they run on the publishing
// goroutine inside the hot path. Errors are not propagated; if a preparer
// needs to bail, it should leave the envelope unchanged.
type EnvelopePreparer func(ctx context.Context, e *event.Envelope)

// Options bundles every Bus-level setting.
type Options struct {
	AgentID           string
	Tenant            string
	SubjectPrefix     string
	Codec             codec.Codec
	Transport         transport.Transport
	Logger            *slog.Logger
	Middleware        []middleware.Middleware
	EnvelopePreparers []EnvelopePreparer
	DefaultTimeout    time.Duration
}

// Option mutates Options.
type Option func(*Options)

func WithAgentID(id string) Option      { return func(o *Options) { o.AgentID = id } }
func WithTenant(t string) Option        { return func(o *Options) { o.Tenant = t } }
func WithSubjectPrefix(p string) Option { return func(o *Options) { o.SubjectPrefix = p } }
func WithCodec(c codec.Codec) Option    { return func(o *Options) { o.Codec = c } }
func WithLogger(l *slog.Logger) Option  { return func(o *Options) { o.Logger = l } }

func WithTransport(t transport.Transport) Option {
	return func(o *Options) { o.Transport = t }
}

func WithMiddleware(mw ...middleware.Middleware) Option {
	return func(o *Options) { o.Middleware = append(o.Middleware, mw...) }
}

// WithEnvelopePreparer registers one or more preparers that run on every
// outbound envelope just before it is encoded. See EnvelopePreparer.
func WithEnvelopePreparer(p ...EnvelopePreparer) Option {
	return func(o *Options) { o.EnvelopePreparers = append(o.EnvelopePreparers, p...) }
}

func WithDefaultTimeout(d time.Duration) Option {
	return func(o *Options) { o.DefaultTimeout = d }
}

// --- Per-call options ---------------------------------------------------------

// SubOption configures Bus.Subscribe.
type SubOption func(*subOpts)

type subOpts struct {
	Queue string
}

// WithQueue puts the subscriber into a queue group; messages on the subject
// are load-balanced across all members of the same queue.
func WithQueue(q string) SubOption { return func(o *subOpts) { o.Queue = q } }

// InvokeOption configures Bus.Invoke / Bus.StreamInvoke.
type InvokeOption func(*invokeOpts)

type invokeOpts struct {
	Timeout          time.Duration
	IdleTimeout      time.Duration
	MaxPendingFrames int
	MaxSequenceGap   uint64
}

// DefaultMaxPendingFrames caps how many out-of-order frames Stream.Events
// keeps buffered while waiting for the missing Seq. See WithMaxPendingFrames.
const DefaultMaxPendingFrames = 256

// DefaultMaxSequenceGap caps how far ahead of the currently-expected Seq a
// received frame may jump. See WithMaxSequenceGap.
const DefaultMaxSequenceGap = 1024

// WithTimeout sets the overall deadline for the invocation.
func WithTimeout(d time.Duration) InvokeOption {
	return func(o *invokeOpts) { o.Timeout = d }
}

// WithIdleTimeout sets the maximum gap between two streaming frames.
func WithIdleTimeout(d time.Duration) InvokeOption {
	return func(o *invokeOpts) { o.IdleTimeout = d }
}

// WithMaxPendingFrames caps the client-side out-of-order buffer used by
// StreamInvoke's iterator. A frame arriving out of order is held in the
// buffer until the missing lower-Seq frames arrive; when the buffer would
// exceed this limit and the new frame is not the currently-expected Seq,
// the stream is terminated with BACKPRESSURE_DROP. Zero uses
// DefaultMaxPendingFrames.
func WithMaxPendingFrames(n int) InvokeOption {
	return func(o *invokeOpts) { o.MaxPendingFrames = n }
}

// WithMaxSequenceGap caps how far ahead of the currently-expected Seq a
// received frame may be. A frame with Seq - expected >= gap (subtraction
// order matters — the client checks it that way to stay safe against
// uint64 wrap when expected is near MaxUint64) terminates the stream with
// BACKPRESSURE_DROP. This guards against an attacker or buggy server
// injecting a huge Seq that would otherwise sit indefinitely in the
// out-of-order buffer waiting for the missing frames. Zero uses
// DefaultMaxSequenceGap.
func WithMaxSequenceGap(gap uint64) InvokeOption {
	return func(o *invokeOpts) { o.MaxSequenceGap = gap }
}

// HandleOption configures HandleInvoke / HandleStream.
type HandleOption func(*handleOpts)

type handleOpts struct {
	Queue    string
	QueueSet bool
}

// WithHandleQueue places the handler in a queue group so multiple replicas
// load-balance the work.
func WithHandleQueue(q string) HandleOption {
	return func(o *handleOpts) {
		o.Queue = q
		o.QueueSet = true
	}
}

// --- Internal helpers ---------------------------------------------------------

func collectSubOpts(opts []SubOption) subOpts {
	var o subOpts
	for _, f := range opts {
		f(&o)
	}
	return o
}

func collectInvokeOpts(opts []InvokeOption) invokeOpts {
	var o invokeOpts
	for _, f := range opts {
		f(&o)
	}
	return o
}

func collectHandleOpts(opts []HandleOption) handleOpts {
	var o handleOpts
	for _, f := range opts {
		f(&o)
	}
	return o
}
