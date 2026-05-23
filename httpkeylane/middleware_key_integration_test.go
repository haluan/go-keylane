// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/haluan/go-keylane"
)

func TestHTTPMiddlewareUsesQueryKey(t *testing.T) {
	q, _ := newTestQueue(t)
	var ran atomic.Bool

	handler := Middleware(q, Config{
		KeyFunc:  QueryKey("tenant_id"),
		LaneFunc: StaticLane(keylane.Lane("default")),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran.Store(true)
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "?tenant_id=tenant-42")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if !ran.Load() {
		t.Error("handler did not run")
	}
}

func TestHTTPMiddlewareUsesFirstNonEmptyKeyFallback(t *testing.T) {
	q, _ := newTestQueue(t)
	var ran atomic.Bool

	handler := Middleware(q, Config{
		KeyFunc: FirstNonEmptyKey(
			HeaderKey("X-Tenant-ID"),
			QueryKey("tenant_id"),
		),
		LaneFunc: StaticLane(keylane.Lane("default")),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran.Store(true)
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "?tenant_id=from-query")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	if !ran.Load() {
		t.Error("handler did not run with query fallback key")
	}
}

func TestHTTPMiddlewareUsesCompositeKey(t *testing.T) {
	q, _ := newTestQueue(t)
	var ran atomic.Bool

	handler := Middleware(q, Config{
		KeyFunc: CompositeKey(
			HeaderKey("X-Tenant-ID"),
			QueryKey("customer_id"),
		),
		LaneFunc: StaticLane(keylane.Lane("default")),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran.Store(true)
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"?customer_id=cust-9", nil)
	req.Header.Set("X-Tenant-ID", "tenant-42")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()

	if !ran.Load() {
		t.Error("handler did not run with composite key")
	}

	// Same inputs should succeed again (stable routing key).
	ran.Store(false)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do second: %v", err)
	}
	io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()
	if !ran.Load() {
		t.Error("handler did not run on second request with same composite key")
	}
}
