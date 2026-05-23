// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/haluan/go-keylane"
)

func admissionHTTPQueue(t *testing.T) *keylane.Queue {
	t.Helper()
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, err := keylane.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for i := 0; i < 9; i++ {
		_ = q.Submit(context.Background(), keylane.Job{
			Key:  "key",
			Lane: "default",
			Run:  func(context.Context) error { return nil },
		})
	}
	return q
}

func TestMiddlewareAdmissionDisabled(t *testing.T) {
	q := admissionHTTPQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := q.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var ran atomic.Bool
	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran.Store(true)
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
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if !ran.Load() {
		t.Error("handler did not run with admission disabled")
	}
}

func TestMiddlewareAdmissionRejects(t *testing.T) {
	q := admissionHTTPQueue(t)
	var ran atomic.Bool

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
		Admission: AdmissionConfig{
			Enabled:          true,
			RejectAboveRatio: 0.90,
		},
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran.Store(true)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
	if ran.Load() {
		t.Error("handler ran when admission rejected")
	}
}

func TestMiddlewareAdmissionLaneDepthReturns429(t *testing.T) {
	q, err := keylane.New(keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = q.UpdateAdmissionPolicy(keylane.AdmissionPolicy{
		DefaultClass:            keylane.LaneNormal,
		DefaultRejectAboveRatio: 0.99,
		DefaultMaxQueueDepth:    2,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		_ = q.Submit(context.Background(), keylane.Job{
			Key: "k", Lane: "default",
			Run: func(context.Context) error { return nil },
		})
	}

	var ran atomic.Bool
	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
		Admission: AdmissionConfig{
			Enabled: true,
		},
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran.Store(true)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429 for lane depth rejection", resp.StatusCode)
	}
	if ran.Load() {
		t.Error("handler ran when lane depth admission rejected")
	}
}

func TestMiddlewareAdmissionStatus429(t *testing.T) {
	q := admissionHTTPQueue(t)

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
		Admission: AdmissionConfig{
			Enabled:          true,
			RejectAboveRatio: 0.90,
			RejectStatusCode: http.StatusTooManyRequests,
		},
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", resp.StatusCode)
	}
}

func TestMiddlewareAdmissionCustomErrorHandler(t *testing.T) {
	q := admissionHTTPQueue(t)
	var gotErr error

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
		Admission: AdmissionConfig{
			Enabled:          true,
			RejectAboveRatio: 0.90,
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			gotErr = err
			w.WriteHeader(http.StatusTeapot)
		},
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusTeapot {
		t.Errorf("status = %d, want 418", resp.StatusCode)
	}
	if !errors.Is(gotErr, keylane.ErrAdmissionRejected) {
		t.Errorf("ErrorHandler err = %v, want ErrAdmissionRejected", gotErr)
	}
}

func TestMiddlewareMissingKeyBeforeAdmission(t *testing.T) {
	q := admissionHTTPQueue(t)

	handler := Middleware(q, Config{
		KeyFunc:  HeaderKey("X-Tenant-ID"),
		LaneFunc: StaticLane(keylane.Lane("default")),
		Admission: AdmissionConfig{
			Enabled:          true,
			RejectAboveRatio: 0.90,
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

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for missing key under pressure", resp.StatusCode)
	}
}

func TestHTTPAdmissionRejectionCounter(t *testing.T) {
	q := admissionHTTPQueue(t)

	beforeQueued := q.StatsGCPressure().TotalQueued
	var admissionBefore uint64
	for _, ln := range q.StatsGCPressure().Lanes {
		if ln.Name == "default" {
			admissionBefore = ln.Counters.AdmissionRejected
		}
	}

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
		Admission: AdmissionConfig{
			Enabled:          true,
			RejectAboveRatio: 0.90,
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

	afterQueued := q.StatsGCPressure().TotalQueued
	if afterQueued != beforeQueued {
		t.Errorf("TotalQueued = %d, want %d (no enqueue on admission reject)", afterQueued, beforeQueued)
	}

	var admissionAfter uint64
	for _, ln := range q.StatsGCPressure().Lanes {
		if ln.Name == "default" {
			admissionAfter = ln.Counters.AdmissionRejected
		}
	}
	if admissionAfter != admissionBefore+1 {
		t.Errorf("AdmissionRejected = %d, want %d", admissionAfter, admissionBefore+1)
	}
}

func TestHTTPValidateAdmissionConfigZeroRatioDefaults(t *testing.T) {
	err := ValidateAdmissionConfig(AdmissionConfig{Enabled: true, RejectAboveRatio: 0})
	if err != nil {
		t.Fatalf("ValidateAdmissionConfig = %v, want nil (zero defaults to 0.90)", err)
	}
}

// TestHTTPValidateAdmissionConfigInvalidRejectStatusCode covers the spec invalid-config
// case: RejectStatusCode outside [100, 599] when admission is enabled.
func TestHTTPValidateAdmissionConfigInvalidRejectStatusCode(t *testing.T) {
	for _, status := range []int{99, 600} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			err := ValidateAdmissionConfig(AdmissionConfig{
				Enabled:          true,
				RejectAboveRatio: 0.90,
				RejectStatusCode: status,
			})
			if !errors.Is(err, keylane.ErrInvalidConfig) {
				t.Fatalf("ValidateAdmissionConfig(status=%d) = %v, want ErrInvalidConfig", status, err)
			}
		})
	}
}
