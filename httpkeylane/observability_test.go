// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/haluan/go-keylane"
)

func TestMiddlewareObserveTransportAndOperation(t *testing.T) {
	var (
		httpMeta HTTPRequestMetadata
		obs      keylane.RequestObservation
	)
	q, _ := httpTestQueue(t)

	handler := Middleware(q, Config{
		KeyFunc:  func(*http.Request) string { return "tenant" },
		LaneFunc: func(*http.Request) keylane.Lane { return "default" },
		OperationFunc: func(r *http.Request) string {
			return "api.get"
		},
		Observe: func(h HTTPRequestMetadata, o keylane.RequestObservation) {
			httpMeta = h
			obs = o
		},
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp := doGet(t, srv.URL+"/items")
	resp.Body.Close()

	if httpMeta.Method != http.MethodGet || httpMeta.Path != "/items" {
		t.Errorf("HTTP meta = %+v", httpMeta)
	}
	if httpMeta.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want 200", httpMeta.StatusCode)
	}
	if obs.Transport != TransportHTTP || obs.Operation != "api.get" {
		t.Errorf("obs transport/operation = %q/%q, want http/api.get", obs.Transport, obs.Operation)
	}
	if obs.Outcome != keylane.RequestOutcomeCompleted {
		t.Errorf("Outcome = %q, want completed", obs.Outcome)
	}
}

func TestMiddlewareObserveStatus201(t *testing.T) {
	var status int
	q, _ := httpTestQueue(t)

	handler := Middleware(q, Config{
		KeyFunc:  func(*http.Request) string { return "k" },
		LaneFunc: func(*http.Request) keylane.Lane { return "default" },
		Observe: func(h HTTPRequestMetadata, _ keylane.RequestObservation) {
			status = h.StatusCode
		},
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp := doGet(t, srv.URL)
	resp.Body.Close()

	if status != http.StatusCreated {
		t.Errorf("StatusCode = %d, want 201", status)
	}
}

func TestMiddlewareObserveStatus400EmptyKey(t *testing.T) {
	var status int
	q, _ := httpTestQueue(t)

	handler := Middleware(q, Config{
		KeyFunc:  func(*http.Request) string { return "" },
		LaneFunc: func(*http.Request) keylane.Lane { return "default" },
		Observe: func(h HTTPRequestMetadata, o keylane.RequestObservation) {
			status = h.StatusCode
			if o.Outcome != keylane.RequestOutcomeFailed {
				t.Errorf("Outcome = %q, want failed for invalid key", o.Outcome)
			}
		},
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler should not run")
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp := doGet(t, srv.URL)
	resp.Body.Close()

	if status != http.StatusBadRequest {
		t.Errorf("StatusCode = %d, want 400", status)
	}
}

func TestMiddlewareObserveCancelled(t *testing.T) {
	var outcome keylane.RequestOutcome
	observed := make(chan struct{}, 1)
	q, _ := httpTestQueue(t)

	handler := Middleware(q, Config{
		KeyFunc:  func(*http.Request) string { return "k" },
		LaneFunc: func(*http.Request) keylane.Lane { return "default" },
		Observe: func(_ HTTPRequestMetadata, obs keylane.RequestObservation) {
			outcome = obs.Outcome
			select {
			case observed <- struct{}{}:
			default:
			}
		},
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))

	req := httptest.NewRequest(http.MethodGet, "/cancel", nil)
	req.Header.Set("X-Tenant-ID", "k")
	ctx, cancel := context.WithCancel(req.Context())
	req = req.WithContext(ctx)

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	select {
	case <-observed:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Observe")
	}

	if outcome != keylane.RequestOutcomeCancelled {
		t.Errorf("Outcome = %q, want cancelled", outcome)
	}
}

func TestMiddlewareRequestHooksTiming(t *testing.T) {
	runDone := make(chan time.Duration, 1)
	obs := keylane.DefaultObservabilityConfig()
	obs.EnableHooks = true
	obs.Hooks.Request.OnCompleted = func(o keylane.RequestObservation) {
		runDone <- o.Run
	}
	q, _ := httpTestQueueWithConfig(t, keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
		Observability:    obs,
	})

	handler := Middleware(q, defaultTestConfig())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Millisecond)
		w.Write([]byte("ok"))
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp := doGet(t, srv.URL)
	resp.Body.Close()

	select {
	case runDur := <-runDone:
		if runDur <= 0 {
			t.Errorf("Run = %v, want > 0 from request hooks", runDur)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for OnCompleted hook")
	}
}

func TestMiddlewareObserveAdmissionRejected(t *testing.T) {
	var (
		status  int
		outcome keylane.RequestOutcome
	)
	q := admissionPressureQueue(t)

	handler := Middleware(q, Config{
		KeyFunc:  func(*http.Request) string { return "k" },
		LaneFunc: func(*http.Request) keylane.Lane { return "default" },
		Admission: AdmissionConfig{
			Enabled:          true,
			RejectAboveRatio: 0.90,
		},
		Observe: func(h HTTPRequestMetadata, o keylane.RequestObservation) {
			status = h.StatusCode
			outcome = o.Outcome
		},
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler should not run")
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp := doGet(t, srv.URL)
	resp.Body.Close()

	if status != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", status)
	}
	if outcome != keylane.RequestOutcomeAdmissionRejected {
		t.Errorf("Outcome = %q, want admission_rejected", outcome)
	}
}

func httpTestQueueWithConfig(t *testing.T, cfg keylane.Config) (*keylane.Queue, context.Context) {
	t.Helper()
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

func admissionPressureQueue(t *testing.T) *keylane.Queue {
	t.Helper()
	q, err := keylane.New(keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for i := 0; i < 9; i++ {
		if err := q.Submit(context.Background(), keylane.Job{
			Key:  "fill",
			Lane: "default",
			Run:  func(context.Context) error { return nil },
		}); err != nil {
			t.Fatalf("Submit %d: %v", i, err)
		}
	}
	return q
}
