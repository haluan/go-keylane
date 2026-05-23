// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haluan/go-keylane"
)

func httpTestQueue(t *testing.T) (*keylane.Queue, context.Context) {
	t.Helper()
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas: map[keylane.Lane]int{
			"default": 1,
			"GET":     1,
			"payment": 1,
		},
	}
	q, err := keylane.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := q.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return q, ctx
}

func defaultTestConfig() Config {
	return Config{
		KeyFunc: func(r *http.Request) string {
			if k := r.Header.Get("X-Tenant-ID"); k != "" {
				return k
			}
			return "test-tenant"
		},
		LaneFunc: func(r *http.Request) keylane.Lane {
			return keylane.Lane("default")
		},
	}
}

func doGet(t *testing.T, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("X-Tenant-ID", "test-tenant")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	return resp
}

func TestMiddlewareRunsHandlerThroughKeylane(t *testing.T) {
	q, _ := httpTestQueue(t)
	var ran atomic.Bool

	handler := Middleware(q, defaultTestConfig())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran.Store(true)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp := doGet(t, srv.URL)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "ok" {
		t.Errorf("body = %q, want ok", string(body))
	}
	if !ran.Load() {
		t.Error("inner handler did not run")
	}
}

func TestMiddlewareKeyFuncCalled(t *testing.T) {
	q, _ := httpTestQueue(t)
	var keyFuncCalled atomic.Bool
	var capturedKey string

	cfg := Config{
		KeyFunc: func(r *http.Request) string {
			keyFuncCalled.Store(true)
			return r.Header.Get("X-Tenant-ID")
		},
		LaneFunc: func(r *http.Request) keylane.Lane {
			return keylane.Lane("default")
		},
	}

	handler := Middleware(q, cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedKey = r.Header.Get("X-Tenant-ID")
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	req.Header.Set("X-Tenant-ID", "tenant-42")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()

	if !keyFuncCalled.Load() {
		t.Error("KeyFunc was not called")
	}
	if capturedKey != "tenant-42" {
		t.Errorf("captured key = %q, want tenant-42", capturedKey)
	}
}

func TestMiddlewareLaneFuncCalled(t *testing.T) {
	q, _ := httpTestQueue(t)

	var laneFuncCalled atomic.Bool
	var usedLane keylane.Lane
	cfg := Config{
		KeyFunc: func(r *http.Request) string {
			return "k"
		},
		LaneFunc: func(r *http.Request) keylane.Lane {
			laneFuncCalled.Store(true)
			usedLane = keylane.Lane("payment")
			return usedLane
		},
	}

	handler := Middleware(q, cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	req.Header.Set("X-Tenant-ID", "k")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()

	if !laneFuncCalled.Load() {
		t.Error("LaneFunc was not called")
	}
	if usedLane != "payment" {
		t.Errorf("lane = %q, want payment", usedLane)
	}
}

func TestMiddlewareEmptyKey(t *testing.T) {
	q, _ := httpTestQueue(t)
	var ran atomic.Bool

	cfg := Config{
		KeyFunc: func(r *http.Request) string {
			return ""
		},
		LaneFunc: func(r *http.Request) keylane.Lane {
			return keylane.Lane("default")
		},
	}

	handler := Middleware(q, cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran.Store(true)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp := doGet(t, srv.URL)
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
	if ran.Load() {
		t.Error("handler ran with empty key")
	}
}

func TestMiddlewareEmptyLane(t *testing.T) {
	q, _ := httpTestQueue(t)
	var ran atomic.Bool

	cfg := Config{
		KeyFunc: func(r *http.Request) string {
			return "k"
		},
		LaneFunc: func(r *http.Request) keylane.Lane {
			return keylane.Lane("")
		},
	}

	handler := Middleware(q, cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran.Store(true)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	req.Header.Set("X-Tenant-ID", "k")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
	if ran.Load() {
		t.Error("handler ran with empty lane")
	}
}

func TestMiddlewareQueueFull(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 1,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, err := keylane.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Do not start workers — queue accepts jobs but only one slot per lane.

	_ = q.Submit(context.Background(), keylane.Job{
		Key: "fill", Lane: "default",
		Run: func(ctx context.Context) error { return nil },
	})

	var ran atomic.Bool
	handler := Middleware(q, defaultTestConfig())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran.Store(true)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	req.Header.Set("X-Tenant-ID", "k2")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
	if ran.Load() {
		t.Error("handler ran when queue full")
	}
}

func TestMiddlewareCancelledBeforeRun(t *testing.T) {
	q, _ := httpTestQueue(t)
	var ran atomic.Bool

	handler := Middleware(q, defaultTestConfig())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran.Store(true)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	req.Header.Set("X-Tenant-ID", "k")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// Client may error on cancel; handler must not run
		if !ran.Load() {
			return
		}
		t.Fatalf("unexpected client error with handler run: %v", err)
	}
	resp.Body.Close()

	if ran.Load() {
		t.Error("handler ran with cancelled context")
	}
}

func TestMiddlewareCancelledWhileRunning(t *testing.T) {
	q, _ := httpTestQueue(t)

	handler := Middleware(q, defaultTestConfig())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
		req.Header.Set("X-Tenant-ID", "k")
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		close(done)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()
	<-done
}

func TestMiddlewareCustomErrorHandler(t *testing.T) {
	q, _ := httpTestQueue(t)

	cfg := Config{
		KeyFunc: func(r *http.Request) string {
			return ""
		},
		LaneFunc: func(r *http.Request) keylane.Lane {
			return keylane.Lane("default")
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			http.Error(w, "teapot", http.StatusTeapot)
		},
	}

	handler := Middleware(q, cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp := doGet(t, srv.URL)
	resp.Body.Close()

	if resp.StatusCode != http.StatusTeapot {
		t.Errorf("status = %d, want 418", resp.StatusCode)
	}
}

func TestMiddlewareNilQueueConfig(t *testing.T) {
	handler := Middleware(nil, defaultTestConfig())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler should not run")
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Tenant-ID", "k")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}

func TestMiddlewareMissingKeyFunc(t *testing.T) {
	q, _ := httpTestQueue(t)
	handler := Middleware(q, Config{
		LaneFunc: func(r *http.Request) keylane.Lane { return keylane.Lane("default") },
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler should not run")
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}

func TestMiddlewareInvalidAdmissionStatusCode(t *testing.T) {
	q, _ := httpTestQueue(t)
	cfg := defaultTestConfig()
	cfg.Admission = AdmissionConfig{
		Enabled:          true,
		RejectAboveRatio: 0.90,
		RejectStatusCode: 600,
	}
	handler := Middleware(q, cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler should not run with invalid admission config")
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Tenant-ID", "test-tenant")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500 for invalid RejectStatusCode", rec.Code)
	}
}

func TestStatusCodeForError(t *testing.T) {
	tests := []struct {
		err    error
		status int
	}{
		{keylane.ErrInvalidKey, http.StatusBadRequest},
		{keylane.ErrQueueFull, http.StatusServiceUnavailable},
		{context.Canceled, 499},
		{context.DeadlineExceeded, http.StatusGatewayTimeout},
		{ErrMissingKeyFunc, http.StatusInternalServerError},
		{keylane.ErrAdmissionRejected, http.StatusServiceUnavailable},
	}
	admission := AdmissionConfig{RejectStatusCode: http.StatusServiceUnavailable}
	for _, tt := range tests {
		if got := statusCodeForError(tt.err, admission); got != tt.status {
			t.Errorf("statusCodeForError(%v) = %d, want %d", tt.err, got, tt.status)
		}
	}
}

func TestMiddlewareUsesRequestContext(t *testing.T) {
	q, _ := httpTestQueue(t)
	started := make(chan struct{})
	finished := make(chan struct{})
	var gotCtxErr error

	handler := Middleware(q, defaultTestConfig())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		gotCtxErr = r.Context().Err()
		close(finished)
	}))

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Tenant-ID", "test-tenant")
	req = req.WithContext(ctx)

	go func() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}()

	<-started
	cancel()
	<-finished

	if !errors.Is(gotCtxErr, context.Canceled) {
		t.Errorf("handler context err = %v, want Canceled", gotCtxErr)
	}
}
