// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type requestHookSpy struct {
	queued    chan RequestMeta
	started   chan RequestObservation
	completed chan RequestObservation
	rejected  chan RequestObservation
}

func newRequestHookSpy() *requestHookSpy {
	return &requestHookSpy{
		queued:    make(chan RequestMeta, 8),
		started:   make(chan RequestObservation, 8),
		completed: make(chan RequestObservation, 8),
		rejected:  make(chan RequestObservation, 8),
	}
}

func (s *requestHookSpy) hooks() RequestHooks {
	return RequestHooks{
		OnQueued:    func(m RequestMeta) { s.queued <- m },
		OnStarted:   func(o RequestObservation) { s.started <- o },
		OnCompleted: func(o RequestObservation) { s.completed <- o },
		OnRejected:  func(o RequestObservation) { s.rejected <- o },
	}
}

func startedQueueWithRequestHooks(t *testing.T, hooks RequestHooks) (*Queue, context.Context) {
	t.Helper()
	ctx := testTimeout(t)
	cfg := newTestConfig()
	cfg.Observability = DefaultObservabilityConfig()
	cfg.Observability.Hooks.Request = hooks
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { stopTestQueue(t, q) })
	return q, ctx
}

func TestClassifyRequestOutcome(t *testing.T) {
	meta := RequestMeta{Key: "k", Lane: "default"}
	tests := []struct {
		name string
		err  error
		want RequestOutcome
	}{
		{"nil", nil, RequestOutcomeCompleted},
		{"admission", ErrAdmissionRejected, RequestOutcomeAdmissionRejected},
		{"queue full", ErrQueueFull, RequestOutcomeRejected},
		{"stopped", ErrStopped, RequestOutcomeRejected},
		{"deadline", context.DeadlineExceeded, RequestOutcomeTimedOut},
		{"cancelled", context.Canceled, RequestOutcomeCancelled},
		{"failed", errors.New("boom"), RequestOutcomeFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ObservationForError(nil, meta, tt.err).Outcome
			if got != tt.want {
				t.Errorf("Outcome = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRequestHooksTimingMatchesOnJobTiming(t *testing.T) {
	var jobTiming JobTimingEvent
	var completed RequestObservation
	jobDone := make(chan struct{}, 1)
	completedDone := make(chan struct{}, 1)

	obs := DefaultObservabilityConfig()
	obs.EnableHooks = true
	obs.Hooks.OnJobTiming = func(ev JobTimingEvent) {
		jobTiming = ev
		jobDone <- struct{}{}
	}
	obs.Hooks.Request.OnCompleted = func(o RequestObservation) {
		completed = o
		completedDone <- struct{}{}
	}

	ctx := testTimeout(t)
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[Lane]int{"default": 1},
		Observability:    obs,
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	future, err := SubmitRequest(ctx, q, Request[struct{}, struct{}]{
		Meta:  RequestMeta{Key: "k", Lane: "default"},
		Input: struct{}{},
		Handle: func(ctx context.Context, _ struct{}) (struct{}, error) {
			time.Sleep(5 * time.Millisecond)
			return struct{}{}, nil
		},
	})
	if err != nil {
		t.Fatalf("SubmitRequest: %v", err)
	}
	if _, err := future.Await(ctx); err != nil {
		t.Fatalf("Await: %v", err)
	}

	waitChan(t, jobDone)
	waitChan(t, completedDone)

	if completed.QueueWait != jobTiming.QueueWait {
		t.Errorf("request QueueWait = %v, job QueueWait = %v", completed.QueueWait, jobTiming.QueueWait)
	}
	if timingDiff(completed.Run, jobTiming.RunDuration) > time.Millisecond {
		t.Errorf("request Run = %v, job RunDuration = %v", completed.Run, jobTiming.RunDuration)
	}
}

func timingDiff(a, b time.Duration) time.Duration {
	if a > b {
		return a - b
	}
	return b - a
}

func waitChan(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for hook")
	}
}

func TestRequestHooksQueuedStartedCompleted(t *testing.T) {
	spy := newRequestHookSpy()
	q, ctx := startedQueueWithRequestHooks(t, spy.hooks())

	meta := RequestMeta{
		RequestID: "rid-1",
		Key:       "tenant-a",
		Lane:      "default",
		Transport: "worker",
		Operation: "sum",
	}

	future, err := SubmitRequest(ctx, q, Request[struct{}, struct{}]{
		Meta:  meta,
		Input: struct{}{},
		Handle: func(ctx context.Context, _ struct{}) (struct{}, error) {
			time.Sleep(5 * time.Millisecond)
			return struct{}{}, nil
		},
	})
	if err != nil {
		t.Fatalf("SubmitRequest: %v", err)
	}
	if _, err := future.Await(ctx); err != nil {
		t.Fatalf("Await: %v", err)
	}

	queued := waitRequestMeta(t, spy.queued)
	assertRequestMetaEqual(t, queued, meta)

	started := waitRequestObservation(t, spy.started)
	assertObservationRouting(t, started, meta.Key, meta.Lane)
	if started.ShardID != q.ShardIDForKey(meta.Key) {
		t.Errorf("ShardID = %d, want %d", started.ShardID, q.ShardIDForKey(meta.Key))
	}
	if started.QueueWait <= 0 {
		t.Errorf("QueueWait = %v, want > 0", started.QueueWait)
	}

	completed := waitRequestObservation(t, spy.completed)
	if completed.Outcome != RequestOutcomeCompleted {
		t.Errorf("Outcome = %q, want completed", completed.Outcome)
	}
	if completed.QueueWait <= 0 {
		t.Errorf("QueueWait = %v, want > 0", completed.QueueWait)
	}
	if completed.Run <= 0 {
		t.Errorf("Run = %v, want > 0", completed.Run)
	}
}

func TestRequestHooksHandlerFailed(t *testing.T) {
	spy := newRequestHookSpy()
	q, ctx := startedQueueWithRequestHooks(t, spy.hooks())
	handlerErr := errors.New("handler failed")

	future, err := SubmitRequest(ctx, q, Request[struct{}, struct{}]{
		Meta:  RequestMeta{Key: "k", Lane: "default"},
		Input: struct{}{},
		Handle: func(context.Context, struct{}) (struct{}, error) {
			return struct{}{}, PermanentFailure(handlerErr)
		},
	})
	if err != nil {
		t.Fatalf("SubmitRequest: %v", err)
	}
	if _, awaitErr := future.Await(ctx); awaitErr == nil {
		t.Fatal("Await err = nil, want permanent failure")
	}
	fail, ok := FailureFromFuture(future)
	if !ok || fail.Kind != FailurePermanent {
		t.Fatalf("failure = %+v ok=%v", fail, ok)
	}

	completed := waitRequestObservation(t, spy.completed)
	if completed.Outcome != RequestOutcomeFailed {
		t.Errorf("Outcome = %q, want failed", completed.Outcome)
	}
	if completed.FailureKind != FailurePermanent {
		t.Errorf("FailureKind = %q, want permanent", completed.FailureKind)
	}
	if !errors.Is(completed.Err, handlerErr) {
		t.Errorf("Err = %v, want %v", completed.Err, handlerErr)
	}
}

func TestRequestHooksCancelledBeforeRun(t *testing.T) {
	spy := newRequestHookSpy()
	q, ctx := startedQueueWithRequestHooks(t, spy.hooks())

	reqCtx, cancel := context.WithCancel(ctx)
	cancel()

	future, err := SubmitRequest(reqCtx, q, Request[struct{}, struct{}]{
		Meta:  RequestMeta{Key: "k", Lane: "default"},
		Input: struct{}{},
		Handle: func(context.Context, struct{}) (struct{}, error) {
			return struct{}{}, nil
		},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SubmitRequest err = %v, want context.Canceled", err)
	}
	if _, awaitErr := future.Await(ctx); !errors.Is(awaitErr, context.Canceled) {
		t.Fatalf("Await err = %v", awaitErr)
	}

	rejected := waitRequestObservation(t, spy.rejected)
	if rejected.Outcome != RequestOutcomeCancelled {
		t.Errorf("Outcome = %q, want cancelled", rejected.Outcome)
	}
	if !errors.Is(rejected.Err, context.Canceled) {
		t.Errorf("Err = %v, want context.Canceled", rejected.Err)
	}
}

func TestRequestHooksTimedOutInHandler(t *testing.T) {
	spy := newRequestHookSpy()
	q, ctx := startedQueueWithRequestHooks(t, spy.hooks())

	reqCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()

	future, err := SubmitRequest(reqCtx, q, Request[struct{}, struct{}]{
		Meta:  RequestMeta{Key: "k", Lane: "default"},
		Input: struct{}{},
		Handle: func(ctx context.Context, _ struct{}) (struct{}, error) {
			<-ctx.Done()
			return struct{}{}, ctx.Err()
		},
	})
	if err != nil {
		t.Fatalf("SubmitRequest: %v", err)
	}
	if _, awaitErr := future.Await(ctx); !errors.Is(awaitErr, context.DeadlineExceeded) {
		t.Fatalf("Await err = %v, want deadline exceeded", awaitErr)
	}

	completed := waitRequestObservation(t, spy.completed)
	if completed.Outcome != RequestOutcomeTimedOut {
		t.Errorf("Outcome = %q, want timed_out", completed.Outcome)
	}
	if !errors.Is(completed.Err, context.DeadlineExceeded) {
		t.Errorf("Err = %v, want context.DeadlineExceeded", completed.Err)
	}
}

func TestRequestHooksAdmissionRejected(t *testing.T) {
	spy := newRequestHookSpy()
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[Lane]int{"default": 1},
		Observability:    DefaultObservabilityConfig(),
	}
	cfg.Observability.EnableHooks = true
	cfg.Observability.Hooks.Request = spy.hooks()
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	fillQueueDepth(t, q, 9)

	_, err = SubmitRequest(context.Background(), q, Request[struct{}, struct{}]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Admission: AdmissionConfig{
			Enabled:          true,
			RejectAboveRatio: 0.90,
		},
		Input: struct{}{},
		Handle: func(context.Context, struct{}) (struct{}, error) {
			return struct{}{}, nil
		},
	})
	if !errors.Is(err, ErrAdmissionRejected) {
		t.Fatalf("SubmitRequest err = %v, want ErrAdmissionRejected", err)
	}

	rejected := waitRequestObservation(t, spy.rejected)
	if rejected.Outcome != RequestOutcomeAdmissionRejected {
		t.Errorf("Outcome = %q, want admission_rejected", rejected.Outcome)
	}
	assertZeroTiming(t, rejected)
}

func TestRequestHooksQueueRejected(t *testing.T) {
	spy := newRequestHookSpy()
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 1,
		LaneQuotas:       map[Lane]int{"default": 1},
		Observability:    DefaultObservabilityConfig(),
	}
	cfg.Observability.EnableHooks = true
	cfg.Observability.Hooks.Request = spy.hooks()
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_ = q.Submit(context.Background(), Job{
		Key:  "fill",
		Lane: "default",
		Run:  func(context.Context) error { return nil },
	})

	_, err = SubmitRequest(context.Background(), q, Request[struct{}, struct{}]{
		Meta:  RequestMeta{Key: "k2", Lane: "default"},
		Input: struct{}{},
		Handle: func(context.Context, struct{}) (struct{}, error) {
			return struct{}{}, nil
		},
	})
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("SubmitRequest err = %v, want ErrQueueFull", err)
	}

	rejected := waitRequestObservation(t, spy.rejected)
	if rejected.Outcome != RequestOutcomeRejected {
		t.Errorf("Outcome = %q, want rejected", rejected.Outcome)
	}
}

func TestRequestHooksDisabledWhenEnableHooksFalse(t *testing.T) {
	spy := newRequestHookSpy()
	ctx := testTimeout(t)
	cfg := newTestConfig()
	cfg.Observability = DefaultObservabilityConfig()
	cfg.Observability.EnableHooks = false
	cfg.Observability.Hooks.Request = spy.hooks()
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	_, err = SubmitRequest(ctx, q, Request[struct{}, struct{}]{
		Meta:  RequestMeta{Key: "k", Lane: "default"},
		Input: struct{}{},
		Handle: func(context.Context, struct{}) (struct{}, error) {
			return struct{}{}, nil
		},
	})
	if err != nil {
		t.Fatalf("SubmitRequest: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	assertNoRequestHookSignals(t, spy)
}

func waitRequestMeta(t *testing.T, ch <-chan RequestMeta) RequestMeta {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for request meta hook")
		return RequestMeta{}
	}
}

func waitRequestObservation(t *testing.T, ch <-chan RequestObservation) RequestObservation {
	t.Helper()
	select {
	case o := <-ch:
		return o
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for request observation hook")
		return RequestObservation{}
	}
}

func assertRequestMetaEqual(t *testing.T, got, want RequestMeta) {
	t.Helper()
	if got.RequestID != want.RequestID {
		t.Errorf("RequestID = %q, want %q", got.RequestID, want.RequestID)
	}
	if got.Key != want.Key {
		t.Errorf("Key = %q, want %q", got.Key, want.Key)
	}
	if got.Lane != want.Lane {
		t.Errorf("Lane = %q, want %q", got.Lane, want.Lane)
	}
	if got.Transport != want.Transport {
		t.Errorf("Transport = %q, want %q", got.Transport, want.Transport)
	}
	if got.Operation != want.Operation {
		t.Errorf("Operation = %q, want %q", got.Operation, want.Operation)
	}
}

func assertObservationRouting(t *testing.T, obs RequestObservation, key string, lane Lane) {
	t.Helper()
	if obs.Key != key {
		t.Errorf("Key = %q, want %q", obs.Key, key)
	}
	if obs.Lane != lane {
		t.Errorf("Lane = %q, want %q", obs.Lane, lane)
	}
}

func assertZeroTiming(t *testing.T, obs RequestObservation) {
	t.Helper()
	if obs.QueueWait != 0 {
		t.Errorf("QueueWait = %v, want 0", obs.QueueWait)
	}
	if obs.Run != 0 {
		t.Errorf("Run = %v, want 0", obs.Run)
	}
}

func assertNoRequestHookSignals(t *testing.T, spy *requestHookSpy) {
	t.Helper()
	for _, ch := range []<-chan RequestMeta{spy.queued} {
		select {
		case <-ch:
			t.Fatal("unexpected queued hook")
		default:
		}
	}
	for _, ch := range []<-chan RequestObservation{spy.started, spy.completed, spy.rejected} {
		select {
		case <-ch:
			t.Fatal("unexpected request observation hook")
		default:
		}
	}
}

func TestLowAllocationSubmitRequestNilRequestHooksAllocs(t *testing.T) {
	q, ctx, _ := startQueue(t, lowAllocTestConfig())

	var wg sync.WaitGroup
	drain := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = q.Submit(ctx, Job{
				Key:  "k",
				Lane: "default",
				Run:  func(context.Context) error { return nil },
			})
		}()
	}

	submitJob := func() {
		drain()
		wg.Wait()
	}

	submitRequest := func() {
		drain()
		f, err := SubmitRequest(ctx, q, Request[struct{}, struct{}]{
			Meta:  RequestMeta{Key: "k", Lane: "default"},
			Input: struct{}{},
			Handle: func(context.Context, struct{}) (struct{}, error) {
				return struct{}{}, nil
			},
		})
		if err != nil {
			t.Fatalf("SubmitRequest: %v", err)
		}
		if _, err := f.Await(ctx); err != nil {
			t.Fatalf("Await: %v", err)
		}
		wg.Wait()
	}

	// Warm up allocator state so AllocsPerRun is stable under -race after other package tests.
	for i := 0; i < 20; i++ {
		_ = testing.AllocsPerRun(1, submitJob)
		_ = testing.AllocsPerRun(1, submitRequest)
	}

	jobAllocs := testing.AllocsPerRun(20, submitJob)
	reqAllocs := testing.AllocsPerRun(20, submitRequest)
	if reqAllocs > submitRequestAllocSlack(jobAllocs) {
		t.Errorf("SubmitRequest allocs = %v, Submit job allocs = %v (expected within %v)", reqAllocs, jobAllocs, submitRequestAllocSlack(jobAllocs)-jobAllocs)
	}
}
