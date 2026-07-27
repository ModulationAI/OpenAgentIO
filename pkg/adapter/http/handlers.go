package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/ModulationAI/openagentio/pkg/bus"
	"github.com/ModulationAI/openagentio/pkg/event"
)

// authenticate runs the configured AuthFunc and returns the derived context.
// Returns (nil, nil, false) when authentication failed and a 401 has already
// been written; callers must short-circuit. Returns (ac, true) when allowed.
func (a *Adapter) authenticate(w http.ResponseWriter, r *http.Request) (*AuthContext, bool) {
	if a.auth == nil {
		return nil, true
	}
	ac, err := a.auth(r)
	if err != nil {
		writeErrorJSON(w, http.StatusUnauthorized, event.CodeAuthFailure, err.Error(), false)
		return nil, false
	}
	return ac, true
}

// handleInvoke implements POST /v1/agents/{target}/invoke.
func (a *Adapter) handleInvoke(w http.ResponseWriter, r *http.Request) {
	ac, ok := a.authenticate(w, r)
	if !ok {
		return
	}
	target := r.PathValue("target")
	if target == "" {
		writeErrorJSON(w, http.StatusBadRequest, event.CodeInvalidRequest, "missing target", false)
		return
	}
	env, err := readEnvelope(r, "", ac)
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, event.CodeInvalidRequest, err.Error(), false)
		return
	}

	ctx := r.Context()
	if a.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, a.timeout)
		defer cancel()
	}

	resp, err := a.bus.Invoke(ctx, target, env, bus.WithTimeout(a.timeout))
	if err != nil {
		writeBusError(w, err)
		return
	}

	if resp.EventType == event.ResponseError {
		writeEnvelopeError(w, resp)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if len(resp.Payload) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(resp.Payload)
}

// handleStream implements POST /v1/agents/{target}/stream and emits SSE.
func (a *Adapter) handleStream(w http.ResponseWriter, r *http.Request) {
	ac, ok := a.authenticate(w, r)
	if !ok {
		return
	}
	target := r.PathValue("target")
	if target == "" {
		writeErrorJSON(w, http.StatusBadRequest, event.CodeInvalidRequest, "missing target", false)
		return
	}
	env, err := readEnvelope(r, "", ac)
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, event.CodeInvalidRequest, err.Error(), false)
		return
	}

	// SSE responses must keep the request-scoped context alive for the entire
	// stream; do not wrap with WithTimeout — the per-call bus options already
	// carry the same deadlines into the bus runtime.
	streamOpts := []bus.InvokeOption{}
	if a.timeout > 0 {
		streamOpts = append(streamOpts, bus.WithTimeout(a.timeout))
	}
	if a.idle > 0 {
		streamOpts = append(streamOpts, bus.WithIdleTimeout(a.idle))
	}

	s, err := a.bus.StreamInvoke(r.Context(), target, env, streamOpts...)
	if err != nil {
		writeBusError(w, err)
		return
	}
	defer s.Close()

	// SSE headers — must flush before the loop so clients see them
	// immediately even on slow first frames.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable nginx buffering
	w.WriteHeader(http.StatusOK)

	rc := http.NewResponseController(w)
	_ = rc.Flush()

	retryMs := a.sseRetry.Milliseconds()
	for frame, ferr := range s.Events() {
		if ferr != nil {
			writeSSEError(w, rc, ferr, retryMs)
			return
		}
		if err := writeSSEEnvelope(w, rc, frame, retryMs); err != nil {
			a.log.Warn("http: sse write failed",
				"err", err,
				"target", target,
				"event_type", frame.EventType,
			)
			return
		}
	}
}

// handlePublish implements POST /v1/events/{event_type}.
func (a *Adapter) handlePublish(w http.ResponseWriter, r *http.Request) {
	ac, ok := a.authenticate(w, r)
	if !ok {
		return
	}
	eventType := r.PathValue("event_type")
	if eventType == "" {
		writeErrorJSON(w, http.StatusBadRequest, event.CodeInvalidRequest, "missing event_type", false)
		return
	}
	env, err := readEnvelope(r, eventType, ac)
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, event.CodeInvalidRequest, err.Error(), false)
		return
	}

	ctx := r.Context()
	wait := defaultPublishWait
	if a.timeout > 0 && a.timeout < wait {
		wait = a.timeout
	}
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, wait)
	defer cancel()

	if err := a.bus.Publish(ctx, env); err != nil {
		writeBusError(w, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// writeSSEEnvelope serializes one envelope as a complete SSE event:
//
//	event: <event_type>
//	id:    <event_id>
//	data:  <envelope-json>
//
// followed by a blank line. The writer is flushed so each frame reaches the
// client immediately.
func writeSSEEnvelope(w http.ResponseWriter, rc *http.ResponseController, env *event.Envelope, retryMs int64) error {
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "event: %s\n", env.EventType); err != nil {
		return err
	}
	if env.EventID != "" {
		if _, err := fmt.Fprintf(w, "id: %s\n", env.EventID); err != nil {
			return err
		}
	}
	if retryMs > 0 {
		if _, err := fmt.Fprintf(w, "retry: %d\n", retryMs); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", body); err != nil {
		return err
	}
	return rc.Flush()
}

// writeSSEError emits a synthetic agent.response.error frame derived from a
// Bus-side error (idle/overall timeout, ctx cancel, decode error). Used when
// Stream.Events() yields a non-nil error.
func writeSSEError(w http.ResponseWriter, rc *http.ResponseController, srcErr error, retryMs int64) {
	code := event.CodeAgentUnavailable
	switch {
	case errors.Is(srcErr, context.DeadlineExceeded), errors.Is(srcErr, bus.ErrIdleTimeout):
		code = event.CodeAgentTimeout
	}
	frame := event.New(event.ResponseError)
	frame.FrameType = event.FrameTypeResponseError
	frame.IsFinal = true
	body, _ := json.Marshal(event.ErrorPayload{
		Code:      code,
		Message:   srcErr.Error(),
		Retryable: code == event.CodeAgentTimeout,
	})
	frame.Payload = body
	_ = writeSSEEnvelope(w, rc, frame, retryMs)
}
