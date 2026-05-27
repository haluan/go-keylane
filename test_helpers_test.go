// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// testTimeout provides a standard context with a 2-second timeout.
func testTimeout(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// newTestConfig returns a standardized minimal config suitable for reliable testing.
func newTestConfig() Config {
	return Config{
		ShardCount:       4,
		WorkerCount:      2,
		QueueSizePerLane: 100,
		LaneQuotas: map[Lane]int{
			"default": 1,
			"payment": 2,
			"audit":   1,
			"webhook": 1,
		},
	}
}

// newStartedTestQueue returns a running Queue initialized with testing config.
func newStartedTestQueue(t *testing.T, ctx context.Context) *Queue {
	t.Helper()
	cfg := newTestConfig()
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatalf("failed to start queue: %v", err)
	}
	return q
}

// waitForSignal waits up to 2 seconds for a signal on the provided channel, or fails the test.
func waitForSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for signal")
	}
}

func stopTestQueue(t *testing.T, q *Queue) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := q.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func backendLaneInFlight(q *Queue, resource BackendResourceName, lane BackendLane) int {
	for _, res := range q.DebugSnapshot().BackendResources {
		if res.Resource != resource {
			continue
		}
		for _, l := range res.Lanes {
			if l.Lane == lane {
				return l.InFlight
			}
		}
	}
	return 0
}

func assertBackendLaneInFlightZero(t *testing.T, q *Queue, resource BackendResourceName, lane BackendLane) {
	t.Helper()
	if n := backendLaneInFlight(q, resource, lane); n != 0 {
		t.Fatalf("backend inflight for %s/%s = %d, want 0", resource, lane, n)
	}
}

func eventuallyNoGoroutineGrowth(t *testing.T, before int, tolerance int) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		now := runtime.NumGoroutine()
		if now <= before+tolerance {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("goroutines = %d, before = %d, tolerance = %d", now, before, tolerance)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// waitForN waits up to 2 seconds for N signals on the provided channel, or fails the test.
func waitForN(t *testing.T, ch <-chan struct{}, n int) {
	t.Helper()
	timeout := time.After(2 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-ch:
			// received signal
		case <-timeout:
			t.Fatalf("timeout waiting for signal %d/%d", i+1, n)
		}
	}
}
