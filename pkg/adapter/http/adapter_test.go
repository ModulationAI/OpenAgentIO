package http_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	httpadapter "github.com/ModulationAI/openagentio/pkg/adapter/http"
	"github.com/ModulationAI/openagentio/pkg/bus"
	"github.com/ModulationAI/openagentio/pkg/event"
	"github.com/ModulationAI/openagentio/pkg/transport/inmem"
)

// newAdapterServer wires a Bus(inmem) + Adapter into an httptest.Server and
// returns both so tests can register handlers on the bus before issuing
// requests. Cleanup is registered with t.Cleanup.
func newAdapterServer(t *testing.T, opts ...httpadapter.Option) (*httptest.Server, bus.Bus) {
	t.Helper()
	return newAdapterServerOnBus(t, "", opts...)
}

// newAdapterServerOnBus is like newAdapterServer but lets tests pin the
// underlying bus tenant — required when assertions involve X-Tenant-Id since
// that header drives subject routing and the handler must subscribe under the
// same tenant.
func newAdapterServerOnBus(t *testing.T, tenant string, opts ...httpadapter.Option) (*httptest.Server, bus.Bus) {
	t.Helper()
	busOpts := []bus.Option{
		bus.WithAgentID("adapter-test"),
		bus.WithTransport(inmem.New()),
		bus.WithDefaultTimeout(2 * time.Second),
	}
	if tenant != "" {
		busOpts = append(busOpts, bus.WithTenant(tenant))
	}
	b, err := bus.New(busOpts...)
	if err != nil {
		t.Fatalf("bus.New: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	a := httpadapter.New(b, opts...)
	srv := httptest.NewServer(a)
	t.Cleanup(srv.Close)
	return srv, b
}

func TestInvokeHappyPath(t *testing.T) {
	srv, b := newAdapterServer(t)

	if err := b.HandleInvoke("echo", func(_ context.Context, e *event.Envelope) (any, error) {
		return json.RawMessage(e.Payload), nil
	}); err != nil {
		t.Fatalf("HandleInvoke: %v", err)
	}

	req, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/v1/agents/echo/invoke",
		strings.NewReader(`{"msg":"hi"}`))
	req.Header.Set("Content-Type", "application/json")

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	got := strings.TrimSpace(string(body))
	if got != `{"msg":"hi"}` {
		t.Fatalf("body = %q want %q", got, `{"msg":"hi"}`)
	}
}

func TestInvokeHandlerErrorMapsTo502(t *testing.T) {
	srv, b := newAdapterServer(t)

	if err := b.HandleInvoke("boom", func(_ context.Context, _ *event.Envelope) (any, error) {
		return nil, errors.New("kaboom")
	}); err != nil {
		t.Fatalf("HandleInvoke: %v", err)
	}

	resp, err := srv.Client().Post(
		srv.URL+"/v1/agents/boom/invoke",
		"application/json",
		strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d want 502", resp.StatusCode)
	}
	var ep event.ErrorPayload
	if err := json.NewDecoder(resp.Body).Decode(&ep); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if ep.Code != event.CodeAgentUnavailable {
		t.Fatalf("code = %q", ep.Code)
	}
	if ep.Message != "kaboom" {
		t.Fatalf("message = %q", ep.Message)
	}
}

func TestInvokeMapsHeadersToEnvelope(t *testing.T) {
	// Tenant must match between bus subscription and request — X-Tenant-Id
	// drives subject routing on the bus side.
	srv, b := newAdapterServerOnBus(t, "tenant-1")

	got := make(chan *event.Envelope, 1)
	if err := b.HandleInvoke("inspect", func(_ context.Context, e *event.Envelope) (any, error) {
		got <- e
		return map[string]any{"ok": true}, nil
	}); err != nil {
		t.Fatalf("HandleInvoke: %v", err)
	}

	req, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/v1/agents/inspect/invoke",
		strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Trace-Id", "trace-xyz")
	req.Header.Set("X-Tenant-Id", "tenant-1")
	req.Header.Set("X-Session-Id", "sess-1")
	req.Header.Set("X-User-Id", "user-1")
	req.Header.Set("X-Channel", "dingtalk")

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	select {
	case e := <-got:
		if e.TraceID != "trace-xyz" {
			t.Errorf("TraceID = %q", e.TraceID)
		}
		if e.TenantID != "tenant-1" {
			t.Errorf("TenantID = %q", e.TenantID)
		}
		if e.SessionID != "sess-1" {
			t.Errorf("SessionID = %q", e.SessionID)
		}
		if e.UserID != "user-1" {
			t.Errorf("UserID = %q", e.UserID)
		}
		if e.Channel != "dingtalk" {
			t.Errorf("Channel = %q", e.Channel)
		}
	case <-time.After(time.Second):
		t.Fatal("handler not called")
	}
}

func TestStreamHappyPath(t *testing.T) {
	srv, b := newAdapterServer(t)

	if err := b.HandleStream("count", func(_ context.Context, _ *event.Envelope, w bus.StreamWriter) error {
		if err := w.Started(event.StartedPayload{Meta: map[string]any{"model": "test"}}); err != nil {
			return err
		}
		for i := 0; i < 3; i++ {
			if err := w.Delta(event.DeltaPayload{Data: map[string]any{"i": i}}); err != nil {
				return err
			}
		}
		return w.Final(event.FinalPayload{Result: map[string]any{"total": 3}})
	}); err != nil {
		t.Fatalf("HandleStream: %v", err)
	}

	req, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/v1/agents/count/stream",
		strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type = %q", ct)
	}

	frames := readSSEFrames(t, resp.Body, time.Second)
	if len(frames) != 5 {
		t.Fatalf("frames = %d want 5", len(frames))
	}

	wantTypes := []string{
		event.ResponseStarted,
		event.ResponseDelta,
		event.ResponseDelta,
		event.ResponseDelta,
		event.ResponseFinal,
	}
	for i, f := range frames {
		if f.event != wantTypes[i] {
			t.Errorf("frame[%d].event = %q want %q", i, f.event, wantTypes[i])
		}
		var env event.Envelope
		if err := json.Unmarshal([]byte(f.data), &env); err != nil {
			t.Fatalf("frame[%d] decode: %v", i, err)
		}
		if env.EventType != wantTypes[i] {
			t.Errorf("frame[%d].envelope.event_type = %q", i, env.EventType)
		}
		if uint64(i) != env.Seq {
			t.Errorf("frame[%d].seq = %d", i, env.Seq)
		}
	}
	if !mustDecodeEnvelope(t, frames[4].data).IsFinal {
		t.Errorf("last frame is_final not set")
	}
}

func TestStreamIdleTimeoutEmitsErrorFrame(t *testing.T) {
	srv, b := newAdapterServer(t,
		httpadapter.WithIdleTimeout(50*time.Millisecond),
	)

	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })

	if err := b.HandleStream("hang", func(ctx context.Context, _ *event.Envelope, w bus.StreamWriter) error {
		if err := w.Started(nil); err != nil {
			return err
		}
		select {
		case <-hold:
		case <-ctx.Done():
		}
		return nil
	}); err != nil {
		t.Fatalf("HandleStream: %v", err)
	}

	req, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/v1/agents/hang/stream",
		strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	frames := readSSEFrames(t, resp.Body, 2*time.Second)
	if len(frames) < 2 {
		t.Fatalf("expected started + error, got %d frames", len(frames))
	}
	last := frames[len(frames)-1]
	if last.event != event.ResponseError {
		t.Fatalf("last event = %q want %q", last.event, event.ResponseError)
	}
	env := mustDecodeEnvelope(t, last.data)
	if !env.IsFinal {
		t.Errorf("error frame is_final not set")
	}
	var ep event.ErrorPayload
	if err := json.Unmarshal(env.Payload, &ep); err != nil {
		t.Fatalf("decode error payload: %v", err)
	}
	if ep.Code != event.CodeAgentTimeout {
		t.Errorf("code = %q want %q", ep.Code, event.CodeAgentTimeout)
	}
}

func TestPublishReturns202(t *testing.T) {
	srv, b := newAdapterServer(t)

	got := make(chan *event.Envelope, 1)
	sub, err := b.Subscribe(context.Background(),
		event.MessageReceived,
		func(_ context.Context, e *event.Envelope) error {
			got <- e
			return nil
		})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	resp, err := srv.Client().Post(
		srv.URL+"/v1/events/agent.message.received",
		"application/json",
		strings.NewReader(`{"text":"hi"}`))
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d want 202", resp.StatusCode)
	}

	select {
	case e := <-got:
		if e.EventType != event.MessageReceived {
			t.Fatalf("event_type = %q", e.EventType)
		}
		if string(e.Payload) != `{"text":"hi"}` {
			t.Fatalf("payload = %q", string(e.Payload))
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber never received published event")
	}
}

func TestAuthRejects401(t *testing.T) {
	srv, _ := newAdapterServer(t,
		httpadapter.WithAuth(func(_ *http.Request) (*httpadapter.AuthContext, error) {
			return nil, errors.New("nope")
		}),
	)

	resp, err := srv.Client().Post(
		srv.URL+"/v1/agents/echo/invoke",
		"application/json",
		strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d want 401", resp.StatusCode)
	}
	var ep event.ErrorPayload
	if err := json.NewDecoder(resp.Body).Decode(&ep); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if ep.Code != event.CodeAuthFailure {
		t.Errorf("code = %q", ep.Code)
	}
	if ep.Retryable {
		t.Errorf("auth failure should not be retryable")
	}
}

func TestAuthOverridesHeaders(t *testing.T) {
	// AuthContext.TenantID would change the subject and require a
	// matching bus tenant; this test just exercises the override of fields
	// that don't drive routing (UserID, SessionID, Channel).
	srv, b := newAdapterServer(t,
		httpadapter.WithAuth(func(_ *http.Request) (*httpadapter.AuthContext, error) {
			return &httpadapter.AuthContext{
				UserID:    "auth-user",
				SessionID: "auth-sess",
				Channel:   "auth-chan",
			}, nil
		}),
	)

	got := make(chan *event.Envelope, 1)
	if err := b.HandleInvoke("inspect", func(_ context.Context, e *event.Envelope) (any, error) {
		got <- e
		return map[string]any{"ok": true}, nil
	}); err != nil {
		t.Fatalf("HandleInvoke: %v", err)
	}

	req, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/v1/agents/inspect/invoke",
		strings.NewReader(`{}`))
	req.Header.Set("X-User-Id", "header-user")
	req.Header.Set("X-Session-Id", "header-sess")
	req.Header.Set("X-Channel", "header-chan")

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()

	select {
	case e := <-got:
		if e.UserID != "auth-user" {
			t.Errorf("UserID = %q want auth-user", e.UserID)
		}
		if e.SessionID != "auth-sess" {
			t.Errorf("SessionID = %q want auth-sess", e.SessionID)
		}
		if e.Channel != "auth-chan" {
			t.Errorf("Channel = %q want auth-chan", e.Channel)
		}
	case <-time.After(time.Second):
		t.Fatal("handler not called")
	}
}

func TestBearerAuthHelper(t *testing.T) {
	auth := httpadapter.BearerAuth(func(token string) (*httpadapter.AuthContext, error) {
		if token != "secret" {
			return nil, errors.New("bad token")
		}
		return &httpadapter.AuthContext{TenantID: "ok"}, nil
	})

	cases := []struct {
		name, header string
		wantErr      bool
		wantTenant   string
	}{
		{"good", "Bearer secret", false, "ok"},
		{"good lowercase scheme", "bearer secret", false, "ok"},
		{"bad token", "Bearer wrong", true, ""},
		{"missing", "", true, ""},
		{"wrong scheme", "Basic c2VjcmV0", true, ""},
		{"empty token", "Bearer ", true, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := http.NewRequest(http.MethodGet, "/", nil)
			if tc.header != "" {
				r.Header.Set("Authorization", tc.header)
			}
			ac, err := auth(r)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if ac == nil || ac.TenantID != tc.wantTenant {
				t.Fatalf("ac = %+v", ac)
			}
		})
	}
}

func TestAuthTenantDrivesSubjectRouting(t *testing.T) {
	// Auth sets TenantID = "auth-tenant"; the underlying bus is configured
	// with the same tenant so the handler subscribes on the matching subject
	// and a header X-Tenant-Id with a different value cannot smuggle a
	// request onto a different tenant.
	srv, b := newAdapterServerOnBus(t, "auth-tenant",
		httpadapter.WithAuth(func(_ *http.Request) (*httpadapter.AuthContext, error) {
			return &httpadapter.AuthContext{TenantID: "auth-tenant"}, nil
		}),
	)

	got := make(chan *event.Envelope, 1)
	if err := b.HandleInvoke("scoped", func(_ context.Context, e *event.Envelope) (any, error) {
		got <- e
		return map[string]any{"ok": true}, nil
	}); err != nil {
		t.Fatalf("HandleInvoke: %v", err)
	}

	req, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/v1/agents/scoped/invoke",
		strings.NewReader(`{}`))
	req.Header.Set("X-Tenant-Id", "spoof-tenant") // must be ignored

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()

	select {
	case e := <-got:
		if e.TenantID != "auth-tenant" {
			t.Errorf("TenantID = %q want auth-tenant", e.TenantID)
		}
	case <-time.After(time.Second):
		t.Fatal("handler not called")
	}
}

func TestUnknownRouteIs404(t *testing.T) {
	srv, _ := newAdapterServer(t)

	resp, err := srv.Client().Get(srv.URL + "/v1/whatever")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestInvalidJSONBodyIs400(t *testing.T) {
	srv, _ := newAdapterServer(t)

	resp, err := srv.Client().Post(
		srv.URL+"/v1/agents/echo/invoke",
		"application/json",
		strings.NewReader(`{not-json`))
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var ep event.ErrorPayload
	if err := json.NewDecoder(resp.Body).Decode(&ep); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if ep.Code != event.CodeInvalidRequest {
		t.Errorf("code = %q want %q", ep.Code, event.CodeInvalidRequest)
	}
	if ep.Retryable {
		t.Errorf("invalid request should not be retryable")
	}
}

// --- SSE parsing helpers --------------------------------------------------

type sseFrame struct {
	event string
	id    string
	data  string
}

// readSSEFrames consumes the SSE response body and returns one frame per
// blank-line-separated event, until EOF or deadline. Required because Go's
// stdlib does not ship an SSE parser.
func readSSEFrames(t *testing.T, r io.Reader, deadline time.Duration) []sseFrame {
	t.Helper()
	done := make(chan []sseFrame, 1)
	go func() {
		var (
			frames []sseFrame
			cur    sseFrame
			data   bytes.Buffer
		)
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
		for sc.Scan() {
			line := sc.Text()
			if line == "" {
				cur.data = strings.TrimRight(data.String(), "\n")
				if cur.event != "" || cur.data != "" || cur.id != "" {
					frames = append(frames, cur)
				}
				cur = sseFrame{}
				data.Reset()
				continue
			}
			switch {
			case strings.HasPrefix(line, "event: "):
				cur.event = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "id: "):
				cur.id = strings.TrimPrefix(line, "id: ")
			case strings.HasPrefix(line, "data: "):
				if data.Len() > 0 {
					data.WriteByte('\n')
				}
				data.WriteString(strings.TrimPrefix(line, "data: "))
			}
		}
		if cur.event != "" || data.Len() > 0 {
			cur.data = strings.TrimRight(data.String(), "\n")
			frames = append(frames, cur)
		}
		done <- frames
	}()
	select {
	case f := <-done:
		return f
	case <-time.After(deadline):
		t.Fatalf("SSE parse timeout after %s", deadline)
		return nil
	}
}

func mustDecodeEnvelope(t *testing.T, data string) *event.Envelope {
	t.Helper()
	var env event.Envelope
	if err := json.Unmarshal([]byte(data), &env); err != nil {
		t.Fatalf("decode envelope: %v (data=%q)", err, data)
	}
	return &env
}
