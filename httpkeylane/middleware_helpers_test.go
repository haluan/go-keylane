// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/haluan/go-keylane"
)

func httpTestQueueWithLanes(t *testing.T, lanes ...keylane.Lane) (*keylane.Queue, context.Context) {
	t.Helper()
	quotas := make(map[keylane.Lane]int)
	for _, lane := range lanes {
		quotas[lane] = 1
	}
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       quotas,
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

func TestMiddlewareWithHeaderKey(t *testing.T) {
	q, _ := httpTestQueueWithLanes(t, keylane.Lane("default"))
	var ran atomic.Bool

	handler := Middleware(q, Config{
		KeyFunc:  HeaderKey("X-Tenant-ID"),
		LaneFunc: StaticLane(keylane.Lane("default")),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran.Store(true)
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("X-Tenant-ID", "tenant-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if !ran.Load() {
		t.Error("handler did not run")
	}
}

func TestMiddlewareWithHeaderKeyMissing(t *testing.T) {
	q, _ := httpTestQueueWithLanes(t, keylane.Lane("default"))
	var ran atomic.Bool

	handler := Middleware(q, Config{
		KeyFunc:  HeaderKey("X-Tenant-ID"),
		LaneFunc: StaticLane(keylane.Lane("default")),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran.Store(true)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
	if ran.Load() {
		t.Error("handler ran with missing key")
	}
}

func TestMiddlewareWithMethodLaneMapper(t *testing.T) {
	q, _ := httpTestQueueWithLanes(t, LaneRead, LaneWrite)

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: MethodLaneMapper(),
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
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET status = %d, want 200", resp.StatusCode)
	}

	resp2, err := http.Post(srv.URL, "text/plain", nil)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("POST status = %d, want 200", resp2.StatusCode)
	}
}

func TestMiddlewareWithRouteLaneMapper(t *testing.T) {
	q, _ := httpTestQueueWithLanes(t, keylane.Lane("payment-write"), LaneRead)
	var ran atomic.Bool

	handler := Middleware(q, Config{
		KeyFunc: StaticKey("k"),
		LaneFunc: RouteLaneMapper([]LaneRule{
			{Method: http.MethodPost, PathPrefix: "/payments", Lane: keylane.Lane("payment-write")},
		}, MethodLaneMapper()),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran.Store(true)
		w.WriteHeader(http.StatusOK)
	}))

	mux := http.NewServeMux()
	mux.Handle("/payments", handler)
	mux.Handle("/payments/", handler)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/payments", "text/plain", nil)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if !ran.Load() {
		t.Error("handler did not run")
	}
}
