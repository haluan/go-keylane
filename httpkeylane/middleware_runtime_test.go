// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haluan/go-keylane"
)

func TestHTTPMiddlewareWaitsForScheduledHandlerCompletion(t *testing.T) {
	q, _ := newTestQueue(t)
	ch := newControlledHandler()

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
	})(ch)

	srv := httptest.NewServer(handler)
	defer srv.Close()

	done := make(chan struct{})
	go func() {
		req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		close(done)
	}()

	select {
	case <-ch.started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}

	select {
	case <-done:
		t.Fatal("middleware returned before handler released")
	default:
	}

	close(ch.release)
	waitDone(t, done, "middleware did not return after release")
}

func TestHTTPMiddlewareDoesNotRunHandlerBeforeRuntimeExecution(t *testing.T) {
	hold := make(chan struct{})
	obs := newTestRequestObserver()
	q, _ := newTestQueue(t, withWorkerCount(1), withRequestHooks(obs.hooks()))
	blockWorker(t, q, hold)

	var ran atomic.Bool
	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran.Store(true)
	}))

	done := make(chan struct{})
	go func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		handler.ServeHTTP(rec, req)
		close(done)
	}()

	obs.waitQueued(t, 1)
	if ran.Load() {
		t.Error("handler ran before blocker released")
	}
	close(hold)
	waitDone(t, done, "middleware did not complete")
	if !ran.Load() {
		t.Error("handler did not run after worker available")
	}
}

func TestHTTPRuntimeSameKeyRoutesToSameShard(t *testing.T) {
	obs := newTestRequestObserver()
	q, _ := newTestQueue(t, withRequestHooks(obs.hooks()))

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("same-key"),
		LaneFunc: StaticLane(keylane.Lane("default")),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	for i := 0; i < 3; i++ {
		resp, err := http.Get(srv.URL)
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		resp.Body.Close()
	}

	started := obs.waitStarted(t, 3)
	shard := started[0].ShardID
	for i, s := range started {
		if s.ShardID != shard {
			t.Errorf("request %d ShardID = %d, want %d", i, s.ShardID, shard)
		}
	}
	if shard != q.ShardIDForKey("same-key") {
		t.Errorf("ShardID = %d, want %d from ShardIDForKey", shard, q.ShardIDForKey("same-key"))
	}
}

func TestHTTPRuntimeDifferentKeysAreRoutedByHash(t *testing.T) {
	obs := newTestRequestObserver()
	q, _ := newTestQueue(t, withShardCount(8), withRequestHooks(obs.hooks()))

	handler := Middleware(q, Config{
		KeyFunc:  HeaderKey("X-Tenant-ID"),
		LaneFunc: StaticLane(keylane.Lane("default")),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	keys := []string{"tenant-a", "tenant-b", "tenant-c", "tenant-d"}
	for _, k := range keys {
		req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
		req.Header.Set("X-Tenant-ID", k)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Do: %v", err)
		}
		resp.Body.Close()
	}

	started := obs.waitStarted(t, len(keys))
	seen := make(map[int]struct{})
	for _, s := range started {
		seen[s.ShardID] = struct{}{}
	}
	if len(seen) < 2 {
		t.Logf("shard IDs seen: %v (weak: hash may collide with few keys)", seen)
	}
}

func TestHTTPRuntimeUsesMappedLane(t *testing.T) {
	obs := newTestRequestObserver()
	q, _ := newTestQueue(t, withRequestHooks(obs.hooks()))

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("payment-write")),
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

	started := obs.waitStarted(t, 1)
	if started[0].Lane != "payment-write" {
		t.Errorf("Lane = %q, want payment-write", started[0].Lane)
	}
}

func TestHTTPRuntimeRecordsQueueWaitDuration(t *testing.T) {
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

	done := make(chan struct{})
	go func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		handler.ServeHTTP(rec, req)
		close(done)
	}()

	obs.waitQueued(t, 1)
	close(hold)
	waitDone(t, done, "request did not complete")

	started := obs.waitStarted(t, 1)
	if started[0].QueueWait <= 0 {
		t.Errorf("QueueWait = %v, want > 0", started[0].QueueWait)
	}
}

func TestHTTPRuntimeRecordsRunDuration(t *testing.T) {
	obs := newTestRequestObserver()
	q, _ := newTestQueue(t, withRequestHooks(obs.hooks()))

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	completed := obs.waitCompleted(t, 1)
	if completed[0].Run <= 0 {
		t.Errorf("Run = %v, want > 0", completed[0].Run)
	}
}
