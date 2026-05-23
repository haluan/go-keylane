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
	"time"

	"github.com/haluan/go-keylane"
)

func overloadHTTPQueue(t *testing.T) *keylane.Queue {
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
	_, err = q.UpdateOverloadPolicy(keylane.OverloadPolicy{
		Default: keylane.LaneOverloadPolicy{
			Class: keylane.LaneNormal, RejectAboveRatio: 0.90, MaxQueueDepth: 100,
		},
	})
	if err != nil {
		t.Fatalf("UpdateOverloadPolicy: %v", err)
	}
	fillQueueJobs(t, q, 9, "key", "default")
	return q
}

func TestMiddlewareOverloadRejectLaneDepthReturns429(t *testing.T) {
	q, err := keylane.New(keylane.Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[keylane.Lane]int{"default": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = q.UpdateOverloadPolicy(keylane.OverloadPolicy{
		Default: keylane.LaneOverloadPolicy{
			Class: keylane.LaneNormal, RejectAboveRatio: 0.90, MaxQueueDepth: 1,
		},
	})
	_ = q.Submit(context.Background(), keylane.Job{
		Key: "k", Lane: "default", Run: func(context.Context) error { return nil },
	})

	cfg := Config{
		KeyFunc:  func(*http.Request) string { return "k" },
		LaneFunc: func(*http.Request) keylane.Lane { return "default" },
		Overload: OverloadConfig{Enabled: true},
	}
	handler := Middleware(q, cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429 (lane depth)", rec.Code)
	}
}

func TestMiddlewareOverloadRejectReturns503(t *testing.T) {
	q := overloadHTTPQueue(t)
	var ran atomic.Bool

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
		Overload: OverloadConfig{Enabled: true},
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
		t.Errorf("status = %d, want 503 (pressure reject)", resp.StatusCode)
	}
	if ran.Load() {
		t.Error("handler ran when overload rejected")
	}
}

func TestMiddlewareOverloadKeepNoRetryAfter(t *testing.T) {
	q, err := keylane.New(keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = q.UpdateOverloadPolicy(keylane.OverloadPolicy{
		Default: keylane.LaneOverloadPolicy{
			Class: keylane.LaneNormal, RejectAboveRatio: 0.90, MaxQueueDepth: 100,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
		Overload: OverloadConfig{
			Enabled: true,
			HTTP:    OverloadHTTPConfig{EnableRetryAfter: true},
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

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 (keep)", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") != "" {
		t.Errorf("Retry-After = %q, want absent on keep", resp.Header.Get("Retry-After"))
	}
}

func TestMiddlewareOverloadShedRetryAfter(t *testing.T) {
	q, err := keylane.New(keylane.Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[keylane.Lane]int{"default": 2, "best": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = q.UpdateOverloadPolicy(keylane.OverloadPolicy{
		Default: keylane.LaneOverloadPolicy{Class: keylane.LaneNormal, RejectAboveRatio: 0.90, MaxQueueDepth: 100},
		Lanes: []keylane.LaneOverloadPolicy{
			{Lane: "best", Class: keylane.LaneBestEffort, RejectAboveRatio: 0.75, ShedAboveRatio: 0.50,
				MaxQueueDepth: 100, RetryAfter: 2 * time.Second},
		},
	})
	for i := 0; i < 14; i++ {
		_ = q.Submit(context.Background(), keylane.Job{
			Key: "k", Lane: "default", Run: func(context.Context) error { return nil },
		})
	}

	err = keylane.CheckOverload(q, keylane.OverloadConfig{Enabled: true}, keylane.RequestMeta{
		Key: "k", Lane: "best",
	})
	if err == nil {
		t.Fatal("want overload shed error")
	}

	cfg := OverloadConfig{Enabled: true, HTTP: OverloadHTTPConfig{EnableRetryAfter: true}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	handleOverloadHTTP(rec, req, err, cfg)

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("expected Retry-After header")
	}
}

func TestMiddlewareOverloadDegradeHandler(t *testing.T) {
	q, err := keylane.New(keylane.Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[keylane.Lane]int{"deg": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = q.UpdateOverloadPolicy(keylane.OverloadPolicy{
		Default: keylane.LaneOverloadPolicy{Class: keylane.LaneNormal, RejectAboveRatio: 0.90, MaxQueueDepth: 100},
		Lanes: []keylane.LaneOverloadPolicy{
			{Lane: "deg", Class: keylane.LaneNormal, RejectAboveRatio: 0.95, ShedAboveRatio: 1.0,
				DegradeAboveRatio: 0.01, MaxQueueDepth: 100},
		},
	})
	_ = q.Submit(context.Background(), keylane.Job{
		Key: "fill", Lane: "deg", Run: func(context.Context) error { return nil },
	})

	err = keylane.CheckOverload(q, keylane.OverloadConfig{Enabled: true}, keylane.RequestMeta{
		Key: "k", Lane: "deg",
	})
	if err == nil {
		t.Fatal("want degrade error")
	}

	var degraded bool
	cfg := OverloadConfig{
		Enabled: true,
		DegradeHandler: func(w http.ResponseWriter, r *http.Request, decision keylane.OverloadDecision) {
			degraded = true
			if decision.Action != keylane.OverloadDegrade {
				t.Errorf("action = %q", decision.Action)
			}
			w.WriteHeader(http.StatusTeapot)
		},
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if !handleOverloadHTTP(rec, req, err, cfg) {
		t.Fatal("handler did not process degrade")
	}
	if !degraded {
		t.Fatal("degrade handler not called")
	}
	if rec.Code != http.StatusTeapot {
		t.Errorf("status = %d, want 418", rec.Code)
	}
}

func TestMiddlewareOverloadDegradeDefaultResponse(t *testing.T) {
	q, err := keylane.New(keylane.Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[keylane.Lane]int{"deg": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = q.UpdateOverloadPolicy(keylane.OverloadPolicy{
		Default: keylane.LaneOverloadPolicy{Class: keylane.LaneNormal, RejectAboveRatio: 0.90, MaxQueueDepth: 100},
		Lanes: []keylane.LaneOverloadPolicy{
			{Lane: "deg", Class: keylane.LaneNormal, RejectAboveRatio: 0.95, ShedAboveRatio: 1.0,
				DegradeAboveRatio: 0.01, MaxQueueDepth: 100},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = q.Submit(context.Background(), keylane.Job{
		Key: "fill", Lane: "deg", Run: func(context.Context) error { return nil },
	})

	var ran atomic.Bool
	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("deg")),
		Overload: OverloadConfig{Enabled: true},
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
		t.Errorf("status = %d, want 503 (default degrade)", resp.StatusCode)
	}
	if ran.Load() {
		t.Error("handler ran when overload degraded without custom handler")
	}
}

func TestHTTPOverloadRejectionDoesNotEnqueue(t *testing.T) {
	q := overloadHTTPQueue(t)

	beforeQueued := q.StatsGCPressure().TotalQueued

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
		Overload: OverloadConfig{Enabled: true},
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

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}

	afterQueued := q.StatsGCPressure().TotalQueued
	if afterQueued != beforeQueued {
		t.Errorf("TotalQueued = %d, want %d (no enqueue on overload reject)", afterQueued, beforeQueued)
	}
}
