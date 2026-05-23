// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"

	"github.com/haluan/go-keylane"
)

func TestHTTPRuntimeNoGoroutineLeakOnQueuedCancellation(t *testing.T) {
	before := runtime.NumGoroutine()
	hold := make(chan struct{})
	obs := newTestRequestObserver()
	q, _ := newTestQueue(t, withWorkerCount(1), withRequestHooks(obs.hooks()))
	blockWorker(t, q, hold)

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(ctx)

	done := make(chan struct{})
	go func() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		close(done)
	}()

	obs.waitQueued(t, 1)
	cancel()
	close(hold)
	waitDone(t, done, "request did not finish")

	eventuallyNoGoroutineGrowth(t, before, 4)
}

func TestHTTPRuntimeNoGoroutineLeakOnRunningCancellation(t *testing.T) {
	before := runtime.NumGoroutine()
	q, _ := newTestQueue(t)
	handlerStarted := make(chan struct{})
	handlerExited := make(chan struct{})

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(handlerStarted)
		<-r.Context().Done()
		close(handlerExited)
	}))

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(ctx)

	mwDone := make(chan struct{})
	go func() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		close(mwDone)
	}()

	<-handlerStarted
	cancel()
	waitDone(t, handlerExited, "handler did not observe cancellation")
	waitDone(t, mwDone, "middleware did not return after running cancellation")

	eventuallyNoGoroutineGrowth(t, before, 8)
}

func TestHTTPRuntimeNoGoroutineLeakOnAdmissionReject(t *testing.T) {
	before := runtime.NumGoroutine()
	q := admissionPressureQueueStarted(t)

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
		Admission: AdmissionConfig{
			Enabled:          true,
			RejectAboveRatio: 0.90,
		},
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	for i := 0; i < 5; i++ {
		resp, err := http.Get(srv.URL)
		if err == nil {
			resp.Body.Close()
		}
	}
	srv.Close()

	eventuallyNoGoroutineGrowth(t, before, 8)
}

func TestHTTPRuntimeNoGoroutineLeakOnQueueFull(t *testing.T) {
	before := runtime.NumGoroutine()
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

	handler := Middleware(q, observeConfig(q, nil))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler should not run")
	}))

	for i := 0; i < 5; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		handler.ServeHTTP(rec, req)
	}

	eventuallyNoGoroutineGrowth(t, before, 4)
}

func TestHTTPRuntimeNoGoroutineLeakOnConcurrentOverload(t *testing.T) {
	before := runtime.NumGoroutine()
	q := admissionPressureQueueStarted(t)

	handler := Middleware(q, Config{
		KeyFunc:  HeaderKey("X-Tenant-ID"),
		LaneFunc: StaticLane(keylane.Lane("default")),
		Admission: AdmissionConfig{
			Enabled:          true,
			RejectAboveRatio: 0.90,
		},
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	const n = 25
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
			req.Header.Set("X-Tenant-ID", "t")
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()
	srv.Close()

	eventuallyNoGoroutineGrowth(t, before, 10)
}
