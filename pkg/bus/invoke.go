package bus

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"github.com/ModulationAI/openagentio/pkg/event"
	"github.com/ModulationAI/openagentio/pkg/middleware"
	"github.com/ModulationAI/openagentio/pkg/transport"
)

func (b *defaultBus) Invoke(ctx context.Context, target string, payload any, opts ...InvokeOption) (*event.Envelope, error) {
	if target == "" {
		return nil, errors.New("bus: empty invoke target")
	}
	o := collectInvokeOpts(opts)
	timeout := o.Timeout
	if timeout == 0 {
		timeout = b.opts.DefaultTimeout
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	env, err := b.buildRequestEnvelope(target, payload)
	if err != nil {
		return nil, err
	}

	inbox, err := b.opts.Transport.OpenInbox(ctx)
	if err != nil {
		return nil, err
	}
	defer inbox.Close()
	env.ReplyTo = inbox.Subject()

	b.prepareEnvelope(ctx, env)
	data, err := b.opts.Codec.EncodeEnvelope(env)
	if err != nil {
		return nil, err
	}
	if err := b.opts.Transport.Publish(ctx, &transport.RawMessage{
		Subject: b.invokeSubject(target, b.resolveTenant(env.TenantID)),
		Data:    data,
	}); err != nil {
		return nil, err
	}

	msg, err := inbox.Recv(ctx)
	if err != nil {
		return nil, err
	}
	return b.opts.Codec.DecodeEnvelope(msg.Data)
}

func (b *defaultBus) HandleInvoke(target string, h InvokeHandler, opts ...HandleOption) error {
	if target == "" {
		return errors.New("bus: empty invoke target")
	}
	if h == nil {
		return errors.New("bus: nil invoke handler")
	}
	o := collectHandleOpts(opts)
	if !o.QueueSet {
		o.Queue = target
	}
	subject := b.invokeSubject(target, b.opts.Tenant)

	dispatch := func(ctx context.Context, msg *transport.RawMessage) error {
		env, err := b.opts.Codec.DecodeEnvelope(msg.Data)
		if err != nil {
			return err
		}
		return b.handleOne(ctx, env, h)
	}

	sub, err := b.opts.Transport.Subscribe(b.lifeCtx, subject, o.Queue, dispatch)
	if err != nil {
		return err
	}
	b.trackOwned(sub)
	return nil
}

// handleOne wraps the user's InvokeHandler in the middleware chain, captures
// its return value through a closure, and publishes the response envelope to
// req.ReplyTo. If the request has no ReplyTo we still run the handler (for
// observability/side effects) but skip the publish.
func (b *defaultBus) handleOne(ctx context.Context, req *event.Envelope, h InvokeHandler) error {
	var (
		result  any
		userErr error
	)
	chained := middleware.Chain(middleware.Handler(func(ctx context.Context, e *event.Envelope) error {
		result, userErr = h(ctx, e)
		return userErr
	}), b.opts.Middleware...)

	mwErr := chained(ctx, req)

	if req.ReplyTo == "" {
		// Fire-and-forget invocation: nothing to reply to.
		return mwErr
	}

	var resp *event.Envelope
	switch {
	case mwErr != nil:
		resp = b.errorResponse(req, mwErr)
	case isEnvelope(result):
		resp = adoptResponse(b.opts.AgentID, req, result.(*event.Envelope))
	default:
		resp, mwErr = b.finalResponse(req, result)
		if mwErr != nil {
			resp = b.errorResponse(req, mwErr)
		}
	}

	data, err := b.opts.Codec.EncodeEnvelope(resp)
	if err != nil {
		return err
	}
	return b.opts.Transport.Publish(ctx, &transport.RawMessage{
		Subject: req.ReplyTo,
		Data:    data,
	})
}

func (b *defaultBus) buildRequestEnvelope(target string, payload any) (*event.Envelope, error) {
	if e, ok := payload.(*event.Envelope); ok {
		env := e.Clone()
		// ADR-010 runtime contract check: warn if a non-request event type is
		// used in an invoke/streamInvoke payload. EventType in request/reply
		// scenarios should be MessageReceived; other values usually indicate
		// mixing pub/sub and request/reply semantics.
		if env.EventType != "" && env.EventType != event.MessageReceived {
			b.opts.Logger.Warn("bus: invoke payload envelope carries non-request event type",
				slog.String("event_type", env.EventType),
				slog.String("target", target),
				slog.String("hint", "use event.NewRequest() for invoke/stream, event.NewEvent() for pub/sub"),
			)
		}
		if env.From == "" {
			env.From = b.opts.AgentID
		}
		if env.To == "" {
			env.To = target
		}
		if env.TenantID == "" {
			env.TenantID = b.opts.Tenant
		}
		if env.FrameType == "" && env.EventType == event.MessageReceived {
			env.FrameType = event.FrameTypeRequest
		}
		return env, nil
	}

	env := event.NewRequest()
	env.From = b.opts.AgentID
	env.To = target
	env.TenantID = b.opts.Tenant
	if payload != nil {
		data, err := b.opts.Codec.EncodePayload(payload)
		if err != nil {
			return nil, err
		}
		env.Payload = data
	}
	return env, nil
}

func (b *defaultBus) finalResponse(req *event.Envelope, payload any) (*event.Envelope, error) {
	resp := newReplyShell(b.opts.AgentID, req, event.ResponseFinal)
	resp.IsFinal = true
	if payload != nil {
		data, err := b.opts.Codec.EncodePayload(payload)
		if err != nil {
			return nil, err
		}
		resp.Payload = data
	}
	return resp, nil
}

func (b *defaultBus) errorResponse(req *event.Envelope, srcErr error) *event.Envelope {
	resp := newReplyShell(b.opts.AgentID, req, event.ResponseError)
	resp.IsFinal = true
	payload := event.ErrorPayload{
		Code:    event.CodeAgentUnavailable,
		Message: srcErr.Error(),
	}
	data, _ := b.opts.Codec.EncodePayload(payload)
	resp.Payload = data
	return resp
}

// newReplyShell pre-populates a response envelope with correlation metadata
// copied from req, leaving Payload/IsFinal for the caller to fill in.
// Non-acp metadata keys are inherited so business context (e.g. dingtalk.*)
// flows back through cascading invocations without manual copying.
func newReplyShell(agentID string, req *event.Envelope, eventType string) *event.Envelope {
	resp := event.New(eventType)
	resp.FrameType = event.FrameTypeForEventType(eventType)
	resp.From = agentID
	resp.To = req.From
	resp.SessionID = req.SessionID
	resp.ConversationID = req.ConversationID
	resp.TenantID = req.TenantID
	resp.UserID = req.UserID
	resp.Channel = req.Channel
	resp.TraceID = req.TraceID
	resp.SpanID = req.SpanID
	resp.Traceparent = req.Traceparent
	resp.CorrelationID = req.EventID
	resp.Metadata = inheritMetadata(req.Metadata)
	return resp
}

// inheritMetadata copies metadata while filtering out runtime-internal keys
// prefixed with "acp." (e.g. acp.retry.attempt, acp.dlq.last_error).
func inheritMetadata(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		if !strings.HasPrefix(k, "acp.") {
			dst[k] = v
		}
	}
	if len(dst) == 0 {
		return nil
	}
	return dst
}

// mergeMetadata combines inherited request metadata with handler-provided
// metadata. Handler keys override inherited keys for the same name. Runtime
// keys prefixed with "acp." are never inherited and are stripped from the
// handler overlay so they cannot leak upstream.
func mergeMetadata(inherited, handler map[string]any) map[string]any {
	if inherited == nil && handler == nil {
		return nil
	}
	dst := make(map[string]any)
	for k, v := range inherited {
		dst[k] = v
	}
	for k, v := range handler {
		if strings.HasPrefix(k, "acp.") {
			continue
		}
		dst[k] = v
	}
	if len(dst) == 0 {
		return nil
	}
	return dst
}

// adoptResponse merges fields from the user-supplied response envelope with
// correlation metadata from the request, preferring the user's values when
// they are explicitly set. If the user envelope carries no metadata,
// non-acp keys from the request are inherited.
func adoptResponse(agentID string, req, user *event.Envelope) *event.Envelope {
	resp := user.Clone()
	if resp.From == "" {
		resp.From = agentID
	}
	if resp.To == "" {
		resp.To = req.From
	}
	if resp.CorrelationID == "" {
		resp.CorrelationID = req.EventID
	}
	if resp.SessionID == "" {
		resp.SessionID = req.SessionID
	}
	if resp.ConversationID == "" {
		resp.ConversationID = req.ConversationID
	}
	if resp.TenantID == "" {
		resp.TenantID = req.TenantID
	}
	if resp.TraceID == "" {
		resp.TraceID = req.TraceID
	}
	if resp.Traceparent == "" {
		resp.Traceparent = req.Traceparent
	}
	if !resp.IsFinal && event.IsTerminal(resp.EventType) {
		resp.IsFinal = true
	}
	// For known protocol event types, frame_type is canonical and must not
	// contradict event_type. Derive it from event_type whenever a mapping exists.
	if ft := event.FrameTypeForEventType(resp.EventType); ft != "" {
		resp.FrameType = ft
	}
	resp.Metadata = mergeMetadata(inheritMetadata(req.Metadata), resp.Metadata)
	return resp
}

func isEnvelope(v any) bool {
	_, ok := v.(*event.Envelope)
	return ok
}
