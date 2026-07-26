package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/ModulationAI/openagentio/pkg/bus"
	"github.com/ModulationAI/openagentio/pkg/event"
)

// statusForCode maps an event.ErrorPayload code to the closest HTTP status.
// Unknown codes default to 500.
func statusForCode(code string) int {
	switch code {
	case event.CodeAuthFailure:
		return http.StatusUnauthorized
	case event.CodeInvalidRequest:
		return http.StatusBadRequest
	case event.CodeNoHandler:
		return http.StatusNotFound
	case event.CodeAgentTimeout:
		return http.StatusGatewayTimeout
	case event.CodeAgentUnavailable, event.CodeTransportFailure:
		return http.StatusBadGateway
	case event.CodeBackpressureDrop:
		return http.StatusTooManyRequests
	case event.CodeCodecFailure:
		return http.StatusInternalServerError
	default:
		return http.StatusInternalServerError
	}
}

// statusForBusError maps Bus-side error sentinels (timeout, idle, ctx cancel)
// to HTTP statuses. Returns (status, code) where code is one of the standard
// event.Code* constants suitable for ErrorPayload.
func statusForBusError(err error) (int, string) {
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, bus.ErrIdleTimeout):
		return http.StatusGatewayTimeout, event.CodeAgentTimeout
	case errors.Is(err, context.Canceled):
		// Client closed connection; report it but the response will likely never reach them.
		return 499, event.CodeInvalidRequest
	default:
		return http.StatusBadGateway, event.CodeAgentUnavailable
	}
}

// writeErrorJSON writes an ErrorPayload-shaped body with the given status.
// Used by middleware (Recover) and the request handlers.
func writeErrorJSON(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(event.ErrorPayload{
		Code:    code,
		Message: message,
	})
}

// writeBusError translates a Bus.Invoke / StreamInvoke error into a JSON 5xx
// response. It does not write the response body if the request was cancelled
// by the client (status 499 is informative only).
func writeBusError(w http.ResponseWriter, err error) {
	status, code := statusForBusError(err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(event.ErrorPayload{
		Code:      code,
		Message:   err.Error(),
		Retryable: code == event.CodeAgentTimeout,
	})
}

// writeEnvelopeError unpacks an envelope whose EventType == ResponseError and
// emits the embedded ErrorPayload with the corresponding HTTP status.
func writeEnvelopeError(w http.ResponseWriter, env *event.Envelope) {
	var ep event.ErrorPayload
	if len(env.Payload) > 0 {
		_ = json.Unmarshal(env.Payload, &ep)
	}
	if ep.Code == "" {
		ep.Code = event.CodeAgentUnavailable
	}
	if ep.Message == "" {
		ep.Message = "agent error"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusForCode(ep.Code))
	_ = json.NewEncoder(w).Encode(ep)
}
