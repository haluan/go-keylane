// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/haluan/go-keylane"
)

func TestHTTPStatusCaptureImplicitOK(t *testing.T) {
	var status int
	q, _ := newTestQueue(t)

	handler := Middleware(q, observeConfig(q, func(h HTTPRequestMetadata, _ keylane.RequestObservation) {
		status = h.StatusCode
	}))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	if status != http.StatusOK {
		t.Errorf("StatusCode = %d, want 200", status)
	}
}

func TestHTTPStatusCaptureExplicitStatus(t *testing.T) {
	var status int
	q, _ := newTestQueue(t)

	handler := Middleware(q, observeConfig(q, func(h HTTPRequestMetadata, _ keylane.RequestObservation) {
		status = h.StatusCode
	}))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	if status != http.StatusCreated {
		t.Errorf("StatusCode = %d, want 201", status)
	}
}

func TestHTTPStatusCaptureMiddlewareError(t *testing.T) {
	var status int
	q, _ := newTestQueue(t)

	handler := Middleware(q, Config{
		KeyFunc:  func(*http.Request) string { return "" },
		LaneFunc: StaticLane(keylane.Lane("default")),
		Observe: func(h HTTPRequestMetadata, _ keylane.RequestObservation) {
			status = h.StatusCode
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

	if status != http.StatusBadRequest {
		t.Errorf("StatusCode = %d, want 400", status)
	}
}

func TestHTTPStatusCaptureFirstWriteHeaderWins(t *testing.T) {
	var status int
	q, _ := newTestQueue(t)

	handler := Middleware(q, observeConfig(q, func(h HTTPRequestMetadata, _ keylane.RequestObservation) {
		status = h.StatusCode
	}))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.WriteHeader(http.StatusInternalServerError)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	if status != http.StatusCreated {
		t.Errorf("StatusCode = %d, want 201", status)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("response status = %d, want 201", resp.StatusCode)
	}
}
