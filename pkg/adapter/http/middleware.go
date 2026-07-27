package http

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"
)

// Recover guards downstream handlers from panics. A recovered panic is logged
// and translated to a 500 response with an INVALID_REQUEST-shaped JSON body so
// clients see consistent error envelopes.
func Recover(log *slog.Logger) Middleware {
	if log == nil {
		log = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					log.Error("http: panic recovered",
						"path", r.URL.Path,
						"method", r.Method,
						"panic", rec,
						"stack", string(debug.Stack()),
					)
					writeErrorJSON(w, http.StatusInternalServerError, "INTERNAL_ERROR", "internal server error", false)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// Logging emits a structured log line per request after the handler returns.
// Status code is captured via a thin ResponseWriter wrapper.
func Logging(log *slog.Logger) Middleware {
	if log == nil {
		log = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			lw := &loggingWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(lw, r)
			log.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", lw.status,
				"duration_ms", time.Since(start).Milliseconds(),
			)
		})
	}
}

// loggingWriter captures the status code so Logging can report it. It also
// implements http.Flusher passthrough so SSE handlers downstream still flush.
type loggingWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *loggingWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status = code
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *loggingWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.wroteHeader = true
	}
	return w.ResponseWriter.Write(b)
}

// Flush forwards to the underlying writer when supported. Required so
// http.NewResponseController(w).Flush() in stream handlers still works when
// Logging is in the chain.
func (w *loggingWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap exposes the underlying ResponseWriter so http.NewResponseController
// can find the real Flusher / Hijacker / etc.
func (w *loggingWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
