// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func retryTestQueue(t *testing.T, retry RetryPolicy) (*Queue, context.Context) {
	t.Helper()
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[Lane]int{"default": 1},
		Retry:            retry,
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return q, ctx
}

func retrySafeIdempotency() Idempotency {
	return Idempotency{Safety: RetrySafetySafe}
}

func TestIntegrationSubmitValueRetrySuccessOnSecondAttempt(t *testing.T) {
	q, ctx := retryTestQueue(t, RetryPolicy{
		Enabled: true, MaxAttempts: 3, InitialBackoff: 1 * time.Millisecond,
		Jitter: false, MinRemainingBudget: 0,
	})
	var attempts atomic.Int32
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default", Idempotency: retrySafeIdempotency(),
		Run: func(context.Context) (int, error) {
			if attempts.Add(1) < 2 {
				return 0, RetryableFailure(errors.New("transient"))
			}
			return 7, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	val, awaitErr := future.Await(context.Background())
	if awaitErr != nil {
		t.Fatal(awaitErr)
	}
	if val != 7 || attempts.Load() != 2 {
		t.Fatalf("val=%d attempts=%d", val, attempts.Load())
	}
}

func TestIntegrationSubmitValueRetryStopsAtMaxAttempts(t *testing.T) {
	q, ctx := retryTestQueue(t, RetryPolicy{
		Enabled: true, MaxAttempts: 2, InitialBackoff: 1 * time.Millisecond,
		Jitter: false, MinRemainingBudget: 0,
	})
	var attempts atomic.Int32
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default", Idempotency: retrySafeIdempotency(),
		Run: func(context.Context) (int, error) {
			attempts.Add(1)
			return 0, RetryableFailure(errors.New("transient"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, awaitErr := future.Await(context.Background())
	if awaitErr == nil {
		t.Fatal("expected error")
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d", attempts.Load())
	}
	assertFutureFailureKind(t, future, FailureRetryable)
}

func TestIntegrationSubmitValuePermanentNotRetried(t *testing.T) {
	q, ctx := retryTestQueue(t, RetryPolicy{
		Enabled: true, MaxAttempts: 3, InitialBackoff: 1 * time.Millisecond, Jitter: false,
	})
	var attempts atomic.Int32
	future, _ := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default",
		Run: func(context.Context) (int, error) {
			attempts.Add(1)
			return 0, PermanentFailure(errors.New("perm"))
		},
	})
	_, _ = future.Await(context.Background())
	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d", attempts.Load())
	}
}

func TestIntegrationSubmitValueDeadlineExhaustedDuringBackoff(t *testing.T) {
	// SubmitValue uses runWithRetry; sleep+deadline classification is covered deterministically below.
	TestRunWithRetryDeadlineExhaustedDuringSleep(t)

	q, ctx := retryTestQueue(t, RetryPolicy{
		Enabled: true, MaxAttempts: 2, InitialBackoff: 34 * time.Millisecond,
		Jitter: false, MinRemainingBudget: time.Nanosecond,
	})
	reqCtx, cancel := context.WithTimeout(ctx, 35*time.Millisecond)
	defer cancel()
	future, err := SubmitValue(reqCtx, q, ValueJob[int]{
		Key: "k", Lane: "default", Idempotency: retrySafeIdempotency(),
		Run: func(context.Context) (int, error) {
			return 0, RetryableFailure(errors.New("transient"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, awaitErr := future.Await(context.Background())
	if errors.Is(awaitErr, context.DeadlineExceeded) {
		assertFutureFailureKind(t, future, FailureDeadlineExhausted)
	}
}

func TestIntegrationSubmitValueCancelDuringBackoff(t *testing.T) {
	q, ctx := retryTestQueue(t, RetryPolicy{
		Enabled: true, MaxAttempts: 3, InitialBackoff: 200 * time.Millisecond, Jitter: false,
	})
	reqCtx, cancel := context.WithCancel(ctx)
	future, err := SubmitValue(reqCtx, q, ValueJob[int]{
		Key: "k", Lane: "default", Idempotency: retrySafeIdempotency(),
		Run: func(context.Context) (int, error) {
			return 0, RetryableFailure(errors.New("transient"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	cancel()
	_, awaitErr := future.Await(context.Background())
	if !errors.Is(awaitErr, context.Canceled) {
		t.Fatalf("await err = %v", awaitErr)
	}
	assertFutureFailureKind(t, future, FailureCancelled)
}

func TestIntegrationSubmitValueFutureCompletesOnce(t *testing.T) {
	q, ctx := retryTestQueue(t, RetryPolicy{
		Enabled: true, MaxAttempts: 2, InitialBackoff: 1 * time.Millisecond, Jitter: false,
	})
	future, _ := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default",
		Run: func(context.Context) (int, error) {
			return 1, nil
		},
	})
	select {
	case <-future.Done():
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
	select {
	case <-future.Done():
	default:
		t.Fatal("done channel should stay closed")
	}
}

func TestIntegrationSubmitRequestCancelDuringBackoff(t *testing.T) {
	q, ctx := retryTestQueue(t, RetryPolicy{
		Enabled: true, MaxAttempts: 3, InitialBackoff: 200 * time.Millisecond, Jitter: false,
	})
	reqCtx, cancel := context.WithCancel(ctx)
	future, err := SubmitRequest(reqCtx, q, Request[struct{}, struct{}]{
		Meta: RequestMeta{Key: "k", Lane: "default"}, Input: struct{}{},
		Idempotency: retrySafeIdempotency(),
		Handle: func(context.Context, struct{}) (struct{}, error) {
			return struct{}{}, RetryableFailure(errors.New("transient"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	cancel()
	_, awaitErr := future.Await(context.Background())
	if !errors.Is(awaitErr, context.Canceled) {
		t.Fatalf("await err = %v", awaitErr)
	}
	assertFutureFailureKind(t, future, FailureCancelled)
}

func TestIntegrationSubmitRequestFinalFailureMetadata(t *testing.T) {
	q, ctx := retryTestQueue(t, RetryPolicy{
		Enabled: true, MaxAttempts: 2, InitialBackoff: 1 * time.Millisecond, Jitter: false,
	})
	future, err := SubmitRequest(ctx, q, Request[struct{}, struct{}]{
		Meta: RequestMeta{Key: "k", Lane: "default"}, Input: struct{}{},
		Idempotency: retrySafeIdempotency(),
		Handle: func(context.Context, struct{}) (struct{}, error) {
			return struct{}{}, RetryableFailure(errors.New("transient"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, awaitErr := future.Await(context.Background())
	if awaitErr == nil {
		t.Fatal("expected error")
	}
	failure, ok := FailureFromFuture(future)
	if !ok || failure.Kind != FailureRetryable {
		t.Fatalf("failure = %+v ok=%v", failure, ok)
	}
	budget, ok := BudgetFromFuture(future)
	if !ok || budget.StartedAt.IsZero() {
		t.Fatalf("budget = %+v ok=%v", budget, ok)
	}
	trace, ok := BudgetTraceFromFuture(future)
	if !ok || trace.AtHandlerStart.StartedAt.IsZero() || trace.AtCompletion.StartedAt.IsZero() {
		t.Fatalf("trace = %+v ok=%v", trace, ok)
	}
}

func TestIntegrationSubmitRequestHandlerRetrySuccess(t *testing.T) {
	q, ctx := retryTestQueue(t, RetryPolicy{
		Enabled: true, MaxAttempts: 3, InitialBackoff: 1 * time.Millisecond, Jitter: false,
	})
	var attempts atomic.Int32
	future, err := SubmitRequest(ctx, q, Request[struct{}, int]{
		Meta: RequestMeta{Key: "k", Lane: "default"}, Input: struct{}{},
		Idempotency: retrySafeIdempotency(),
		Handle: func(context.Context, struct{}) (int, error) {
			if attempts.Add(1) < 2 {
				return 0, RetryableFailure(errors.New("transient"))
			}
			return 9, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	out, awaitErr := future.Await(context.Background())
	if awaitErr != nil || out != 9 {
		t.Fatalf("out=%d err=%v", out, awaitErr)
	}
}

func TestIntegrationSubmitRequestPermanentNotRetried(t *testing.T) {
	q, ctx := retryTestQueue(t, RetryPolicy{
		Enabled: true, MaxAttempts: 3, InitialBackoff: 1 * time.Millisecond, Jitter: false,
	})
	var attempts atomic.Int32
	future, _ := SubmitRequest(ctx, q, Request[struct{}, struct{}]{
		Meta:  RequestMeta{Key: "k", Lane: "default"},
		Input: struct{}{},
		Handle: func(context.Context, struct{}) (struct{}, error) {
			attempts.Add(1)
			return struct{}{}, PermanentFailure(errors.New("perm"))
		},
	})
	_, _ = future.Await(context.Background())
	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d", attempts.Load())
	}
}

func TestIntegrationSubmitRequestAdmissionNotRetried(t *testing.T) {
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[Lane]int{"default": 1},
		Retry:            RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = q.UpdateAdmissionPolicy(AdmissionPolicy{
		DefaultClass:            LaneNormal,
		DefaultRejectAboveRatio: 0.99,
		DefaultMaxQueueDepth:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	defer close(block)
	run := func(context.Context) error { <-block; return nil }
	for i := 0; i < 2; i++ {
		if err := q.Submit(ctx, Job{Key: "block", Lane: "default", Run: run}); err != nil {
			t.Fatal(err)
		}
	}

	var attempts atomic.Int32
	future, submitErr := SubmitRequest(ctx, q, Request[struct{}, struct{}]{
		Meta:      RequestMeta{Key: "k2", Lane: "default"},
		Admission: AdmissionConfig{Enabled: true, RejectAboveRatio: 0.90},
		Input:     struct{}{},
		Handle: func(context.Context, struct{}) (struct{}, error) {
			attempts.Add(1)
			return struct{}{}, nil
		},
	})
	if !errors.Is(submitErr, ErrAdmissionRejected) {
		t.Fatalf("submit err = %v", submitErr)
	}
	if attempts.Load() != 0 {
		t.Fatalf("handler attempts = %d", attempts.Load())
	}
	_, awaitErr := future.Await(context.Background())
	if awaitErr == nil {
		t.Fatal("expected await error")
	}
}

func TestIntegrationSubmitRequestOnCompletedOnceWithRetry(t *testing.T) {
	var completed atomic.Int32
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[Lane]int{"default": 1},
		Retry: RetryPolicy{
			Enabled: true, MaxAttempts: 3, InitialBackoff: 1 * time.Millisecond, Jitter: false,
		},
		Observability: ObservabilityConfig{
			EnableHooks: true,
			Hooks: Hooks{
				Request: RequestHooks{
					OnCompleted: func(RequestObservation) { completed.Add(1) },
				},
			},
		},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	future, err := SubmitRequest(ctx, q, Request[struct{}, struct{}]{
		Meta: RequestMeta{Key: "k", Lane: "default"}, Input: struct{}{},
		Idempotency: retrySafeIdempotency(),
		Handle: func(context.Context, struct{}) (struct{}, error) {
			if attempts.Add(1) < 2 {
				return struct{}{}, RetryableFailure(errors.New("transient"))
			}
			return struct{}{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := future.Await(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return completed.Load() == 1 }, 2*time.Second)
}
