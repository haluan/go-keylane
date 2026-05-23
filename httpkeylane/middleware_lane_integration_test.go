// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/haluan/go-keylane"
)

func TestHTTPMiddlewareMapsGETToReadLane(t *testing.T) {
	obs := newTestRequestObserver()
	q, _ := newTestQueue(t, withRequestHooks(obs.hooks()))

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

	started := obs.waitStarted(t, 1)
	if started[0].Lane != LaneRead {
		t.Errorf("Lane = %q, want %q", started[0].Lane, LaneRead)
	}
}

func TestHTTPMiddlewareMapsPOSTToWriteLane(t *testing.T) {
	obs := newTestRequestObserver()
	q, _ := newTestQueue(t, withRequestHooks(obs.hooks()))

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: MethodLaneMapper(),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Post(srv.URL, "text/plain", nil)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	resp.Body.Close()

	started := obs.waitStarted(t, 1)
	if started[0].Lane != LaneWrite {
		t.Errorf("Lane = %q, want %q", started[0].Lane, LaneWrite)
	}
}

func TestHTTPMiddlewareRouteLaneMapperFirstMatchWins(t *testing.T) {
	obs := newTestRequestObserver()
	q, _ := newTestQueue(t, withRequestHooks(obs.hooks()))

	handler := Middleware(q, Config{
		KeyFunc: StaticKey("k"),
		LaneFunc: RouteLaneMapper(
			[]LaneRule{
				{Method: http.MethodPost, PathPrefix: "/payments", Lane: keylane.Lane("payment-write")},
				{Method: http.MethodPost, PathPrefix: "/payments/refunds", Lane: keylane.Lane("refund-write")},
			},
			MethodLaneMapper(),
		),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/payments/refunds", "text/plain", nil)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	resp.Body.Close()

	started := obs.waitStarted(t, 1)
	if started[0].Lane != "payment-write" {
		t.Errorf("Lane = %q, want payment-write (first rule wins)", started[0].Lane)
	}
}

func TestHTTPMiddlewareRouteLaneMapperSpecificRuleFirst(t *testing.T) {
	obs := newTestRequestObserver()
	q, _ := newTestQueue(t, withRequestHooks(obs.hooks()))

	handler := Middleware(q, Config{
		KeyFunc: StaticKey("k"),
		LaneFunc: RouteLaneMapper(
			[]LaneRule{
				{Method: http.MethodPost, PathPrefix: "/payments/refunds", Lane: keylane.Lane("refund-write")},
				{Method: http.MethodPost, PathPrefix: "/payments", Lane: keylane.Lane("payment-write")},
			},
			MethodLaneMapper(),
		),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/payments/refunds", "text/plain", nil)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	resp.Body.Close()

	started := obs.waitStarted(t, 1)
	if started[0].Lane != "refund-write" {
		t.Errorf("Lane = %q, want refund-write", started[0].Lane)
	}
}

func TestHTTPMiddlewareRouteLaneMapperFallback(t *testing.T) {
	obs := newTestRequestObserver()
	q, _ := newTestQueue(t, withRequestHooks(obs.hooks()))

	handler := Middleware(q, Config{
		KeyFunc: StaticKey("k"),
		LaneFunc: RouteLaneMapper(
			[]LaneRule{
				{Method: http.MethodPost, PathPrefix: "/payments", Lane: keylane.Lane("payment-write")},
			},
			MethodLaneMapper(),
		),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/other")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	started := obs.waitStarted(t, 1)
	if started[0].Lane != LaneRead {
		t.Errorf("Lane = %q, want %q fallback read", started[0].Lane, LaneRead)
	}
}
