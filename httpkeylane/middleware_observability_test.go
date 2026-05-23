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

func observeConfig(q *keylane.Queue, observe ObserveFunc, extra ...func(*Config)) Config {
	cfg := Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
		Observe:  observe,
	}
	for _, fn := range extra {
		fn(&cfg)
	}
	return cfg
}

func TestHTTPObservabilityPropagatesRequestID(t *testing.T) {
	var obs keylane.RequestObservation
	q, _ := newTestQueue(t)

	handler := Middleware(q, observeConfig(q, func(_ HTTPRequestMetadata, o keylane.RequestObservation) {
		obs = o
	}))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	req.Header.Set("X-Request-ID", "req-abc")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()

	if obs.RequestID != "req-abc" {
		t.Errorf("RequestID = %q, want req-abc", obs.RequestID)
	}
}

func TestHTTPObservabilitySetsTransport(t *testing.T) {
	var obs keylane.RequestObservation
	q, _ := newTestQueue(t)

	handler := Middleware(q, observeConfig(q, func(_ HTTPRequestMetadata, o keylane.RequestObservation) {
		obs = o
	}))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	if obs.Transport != TransportHTTP {
		t.Errorf("Transport = %q, want %q", obs.Transport, TransportHTTP)
	}
}

func TestHTTPObservabilityUsesOperationFunc(t *testing.T) {
	var obs keylane.RequestObservation
	q, _ := newTestQueue(t)

	cfg := observeConfig(q, func(_ HTTPRequestMetadata, o keylane.RequestObservation) {
		obs = o
	})
	cfg.OperationFunc = func(r *http.Request) string {
		return "POST /payments"
	}

	handler := Middleware(q, cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/payments", "text/plain", nil)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	resp.Body.Close()

	if obs.Operation != "POST /payments" {
		t.Errorf("Operation = %q, want POST /payments", obs.Operation)
	}
}

func TestHTTPObservabilityEmitsKeyLaneShard(t *testing.T) {
	var obs keylane.RequestObservation
	q, _ := newTestQueue(t)

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("tenant-7"),
		LaneFunc: StaticLane(keylane.Lane("default")),
		Observe: func(_ HTTPRequestMetadata, o keylane.RequestObservation) {
			obs = o
		},
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	if obs.Key != "tenant-7" {
		t.Errorf("Key = %q, want tenant-7", obs.Key)
	}
	if obs.Lane != "default" {
		t.Errorf("Lane = %q, want default", obs.Lane)
	}
	if obs.ShardID != q.ShardIDForKey("tenant-7") {
		t.Errorf("ShardID = %d, want %d", obs.ShardID, q.ShardIDForKey("tenant-7"))
	}
}

func TestHTTPObservabilityOutcomeCompleted(t *testing.T) {
	var outcome keylane.RequestOutcome
	q, _ := newTestQueue(t)

	handler := Middleware(q, observeConfig(q, func(_ HTTPRequestMetadata, o keylane.RequestObservation) {
		outcome = o.Outcome
	}))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	if outcome != keylane.RequestOutcomeCompleted {
		t.Errorf("Outcome = %q, want completed", outcome)
	}
}

func TestHTTPObservabilityOutcomeCancelled(t *testing.T) {
	var outcome keylane.RequestOutcome
	observed := make(chan struct{}, 1)
	handlerStarted := make(chan struct{})
	q, _ := newTestQueue(t)

	handler := Middleware(q, observeConfig(q, func(_ HTTPRequestMetadata, o keylane.RequestObservation) {
		outcome = o.Outcome
		select {
		case observed <- struct{}{}:
		default:
		}
	}))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(handlerStarted)
		<-r.Context().Done()
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx, cancel := context.WithCancel(req.Context())
	req = req.WithContext(ctx)

	mwDone := make(chan struct{})
	go func() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		close(mwDone)
	}()

	<-handlerStarted
	cancel()
	waitDone(t, mwDone, "middleware did not return")
	waitDone(t, observed, "timeout Observe")

	if outcome != keylane.RequestOutcomeCancelled {
		t.Errorf("Outcome = %q, want cancelled", outcome)
	}
}

func TestHTTPObservabilityOutcomeTimedOut(t *testing.T) {
	hold := make(chan struct{})
	var outcome keylane.RequestOutcome
	q, _ := newTestQueue(t, withWorkerCount(1))
	blockWorker(t, q, hold)

	handler := Middleware(q, observeConfig(q, func(_ HTTPRequestMetadata, o keylane.RequestObservation) {
		outcome = o.Outcome
	}))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler should not run")
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	close(hold)

	if outcome != keylane.RequestOutcomeTimedOut && outcome != keylane.RequestOutcomeCancelled {
		t.Errorf("Outcome = %q, want timed_out or cancelled", outcome)
	}
}

func TestHTTPObservabilityOutcomeAdmissionRejected(t *testing.T) {
	var outcome keylane.RequestOutcome
	q := admissionHTTPQueue(t)

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
		Admission: AdmissionConfig{
			Enabled:          true,
			RejectAboveRatio: 0.90,
		},
		Observe: func(_ HTTPRequestMetadata, o keylane.RequestObservation) {
			outcome = o.Outcome
		},
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler should not run")
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	if outcome != keylane.RequestOutcomeAdmissionRejected {
		t.Errorf("Outcome = %q, want admission_rejected", outcome)
	}
}

func TestHTTPObservabilityOutcomeQueueRejected(t *testing.T) {
	var outcome keylane.RequestOutcome
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
	_ = q.Submit(context.Background(), keylane.Job{
		Key: "fill", Lane: "default",
		Run: func(context.Context) error { return nil },
	})

	handler := Middleware(q, observeConfig(q, func(_ HTTPRequestMetadata, o keylane.RequestObservation) {
		outcome = o.Outcome
	}))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler should not run")
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	handler.ServeHTTP(rec, req)

	if outcome != keylane.RequestOutcomeRejected {
		t.Errorf("Outcome = %q, want rejected", outcome)
	}
}
