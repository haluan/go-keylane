// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"context"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haluan/go-keylane"
)

type testQueueOption func(*keylane.Config)

func withShardCount(n int) testQueueOption {
	return func(cfg *keylane.Config) {
		cfg.ShardCount = n
	}
}

func withWorkerCount(n int) testQueueOption {
	return func(cfg *keylane.Config) {
		cfg.WorkerCount = n
	}
}

func withQueueSizePerLane(n int) testQueueOption {
	return func(cfg *keylane.Config) {
		cfg.QueueSizePerLane = n
	}
}

func withLaneQuotas(quotas map[keylane.Lane]int) testQueueOption {
	return func(cfg *keylane.Config) {
		cfg.LaneQuotas = quotas
	}
}

func withObservability(obs keylane.ObservabilityConfig) testQueueOption {
	return func(cfg *keylane.Config) {
		cfg.Observability = obs
	}
}

func withRequestHooks(h keylane.RequestHooks) testQueueOption {
	return func(cfg *keylane.Config) {
		obs := keylane.DefaultObservabilityConfig()
		obs.Hooks.Request = h
		cfg.Observability = obs
	}
}

func defaultLaneQuotas() map[keylane.Lane]int {
	return map[keylane.Lane]int{
		"default":       2,
		LaneRead:        2,
		LaneWrite:       2,
		"payment-write": 1,
		"refund-write":  1,
	}
}

// newTestQueue starts a queue with cleanup cancel and optional Stop(drain).
func newTestQueue(t *testing.T, opts ...testQueueOption) (*keylane.Queue, context.Context) {
	t.Helper()
	cfg := keylane.Config{
		ShardCount:       4,
		WorkerCount:      2,
		QueueSizePerLane: 10,
		LaneQuotas:       defaultLaneQuotas(),
		Observability:    keylane.DefaultObservabilityConfig(),
	}
	for _, opt := range opts {
		opt(&cfg)
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
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(func() {
		stopCancel()
		_ = q.Stop(stopCtx, keylane.WithDrain(true))
	})
	return q, ctx
}

type controlledHandler struct {
	started chan struct{}
	release chan struct{}
	ran     atomic.Bool
}

func newControlledHandler() *controlledHandler {
	return &controlledHandler{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (h *controlledHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.ran.Store(true)
	close(h.started)
	<-h.release
	w.WriteHeader(http.StatusOK)
}

type testRequestObserver struct {
	mu        sync.Mutex
	queued    []keylane.RequestMeta
	started   []keylane.RequestObservation
	completed []keylane.RequestObservation
	rejected  []keylane.RequestObservation
}

func newTestRequestObserver() *testRequestObserver {
	return &testRequestObserver{}
}

func (o *testRequestObserver) hooks() keylane.RequestHooks {
	return keylane.RequestHooks{
		OnQueued: func(m keylane.RequestMeta) {
			o.mu.Lock()
			o.queued = append(o.queued, m)
			o.mu.Unlock()
		},
		OnStarted: func(obs keylane.RequestObservation) {
			o.mu.Lock()
			o.started = append(o.started, obs)
			o.mu.Unlock()
		},
		OnCompleted: func(obs keylane.RequestObservation) {
			o.mu.Lock()
			o.completed = append(o.completed, obs)
			o.mu.Unlock()
		},
		OnRejected: func(obs keylane.RequestObservation) {
			o.mu.Lock()
			o.rejected = append(o.rejected, obs)
			o.mu.Unlock()
		},
	}
}

func (o *testRequestObserver) waitQueued(t *testing.T, n int) []keylane.RequestMeta {
	t.Helper()
	return o.waitMeta(t, n, func() int {
		o.mu.Lock()
		defer o.mu.Unlock()
		return len(o.queued)
	}, func() []keylane.RequestMeta {
		o.mu.Lock()
		defer o.mu.Unlock()
		return append([]keylane.RequestMeta(nil), o.queued...)
	})
}

func (o *testRequestObserver) waitStarted(t *testing.T, n int) []keylane.RequestObservation {
	t.Helper()
	return o.waitObs(t, n, func() int {
		o.mu.Lock()
		defer o.mu.Unlock()
		return len(o.started)
	}, func() []keylane.RequestObservation {
		o.mu.Lock()
		defer o.mu.Unlock()
		out := append([]keylane.RequestObservation(nil), o.started...)
		return out
	})
}

func (o *testRequestObserver) waitCompleted(t *testing.T, n int) []keylane.RequestObservation {
	t.Helper()
	return o.waitObs(t, n, func() int {
		o.mu.Lock()
		defer o.mu.Unlock()
		return len(o.completed)
	}, func() []keylane.RequestObservation {
		o.mu.Lock()
		defer o.mu.Unlock()
		out := append([]keylane.RequestObservation(nil), o.completed...)
		return out
	})
}

func (o *testRequestObserver) waitRejected(t *testing.T, n int) []keylane.RequestObservation {
	t.Helper()
	return o.waitObs(t, n, func() int {
		o.mu.Lock()
		defer o.mu.Unlock()
		return len(o.rejected)
	}, func() []keylane.RequestObservation {
		o.mu.Lock()
		defer o.mu.Unlock()
		out := append([]keylane.RequestObservation(nil), o.rejected...)
		return out
	})
}

func (o *testRequestObserver) waitMeta(t *testing.T, n int, count func() int, snap func() []keylane.RequestMeta) []keylane.RequestMeta {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		if count() >= n {
			return snap()
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for %d queued events, got %d", n, count())
			return nil
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (o *testRequestObserver) waitObs(t *testing.T, n int, count func() int, snap func() []keylane.RequestObservation) []keylane.RequestObservation {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		if count() >= n {
			return snap()
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for %d observations, got %d", n, count())
			return nil
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func blockWorker(t *testing.T, q *keylane.Queue, hold chan struct{}) {
	t.Helper()
	if err := q.Submit(context.Background(), keylane.Job{
		Key:  "block-worker",
		Lane: "default",
		Run: func(context.Context) error {
			<-hold
			return nil
		},
	}); err != nil {
		t.Fatalf("Submit blocker: %v", err)
	}
}

func fillQueueJobs(t *testing.T, q *keylane.Queue, n int, key string, lane keylane.Lane) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := q.Submit(context.Background(), keylane.Job{
			Key:  key,
			Lane: lane,
			Run:  func(context.Context) error { return nil },
		}); err != nil {
			t.Fatalf("Submit fill %d: %v", i, err)
		}
	}
}

func admissionPressureQueueStarted(t *testing.T) *keylane.Queue {
	t.Helper()
	q, err := keylane.New(keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
		Observability:    keylane.DefaultObservabilityConfig(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	fillQueueJobs(t, q, 9, "fill", "default")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := q.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(func() {
		stopCancel()
		_ = q.Stop(stopCtx, keylane.WithDrain(true))
	})
	return q
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

func waitDone(t *testing.T, done <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal(msg)
	}
}

func laneCounter(t *testing.T, q *keylane.Queue, name keylane.Lane) uint64 {
	t.Helper()
	snap := q.StatsGCPressure()
	for _, ln := range snap.Lanes {
		if ln.Name == string(name) {
			return ln.Counters.Accepted
		}
	}
	return 0
}
