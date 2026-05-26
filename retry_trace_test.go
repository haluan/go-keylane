// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryTraceFromFutureSuccessAfterRetry(t *testing.T) {
	now := time.Now()
	clock := &testRetryClock{now: now}
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	var n int
	res := runWithRetry(context.Background(), FailurePolicy{}, p, runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetySafe}}, NewDeadlineBudget(context.Background(), now), clock, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
		n++
		if n < 2 {
			return 0, RetryableFailure(errors.New("t"))
		}
		return 9, nil
	})
	future := newResultFuture[int]()
	future.setRetryOutcome(res.retryAttempts, res.retryFinal, res.retryTracked)
	future.complete(9, nil, FailurePolicy{}, res.budget, false)

	trace, ok := RetryTraceFromFuture(future)
	if !ok {
		t.Fatal("expected trace")
	}
	if !trace.Final.Succeeded {
		t.Fatalf("final = %+v", trace.Final)
	}
	if len(trace.Attempts) != 1 {
		t.Fatalf("attempts = %+v", trace.Attempts)
	}
}

func TestRetryTraceFromFutureSafetySuppressed(t *testing.T) {
	now := time.Now()
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	res := runWithRetry(context.Background(), FailurePolicy{}, p, runWithRetryOpts{
		Idempotency: Idempotency{Safety: RetrySafetyUnsafe},
	}, NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
		return 0, RetryableFailure(errors.New("t"))
	})
	future := newResultFuture[int]()
	future.setRetryOutcome(res.retryAttempts, res.retryFinal, res.retryTracked)
	future.complete(0, res.err, FailurePolicy{}, res.budget, false)

	trace, ok := RetryTraceFromFuture(future)
	if !ok {
		t.Fatal("expected trace")
	}
	if trace.Final.SafetyReason != RetrySafetyDecisionUnsafe {
		t.Fatalf("final = %+v", trace.Final)
	}
}

func TestRetryTraceFromFuturePressureSuppressed(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 8,
		LaneQuotas:       map[Lane]int{"default": 1},
		Retry:            RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0},
		RetrySuppression: RetrySuppressionPolicy{Enabled: true, SuppressWhenOverloaded: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	opts := buildRunWithRetryOpts(q, "k", "default", 0, Idempotency{Safety: RetrySafetySafe}, nil)
	opts.Snapshot = func(string, Lane, int) RetrySuppressionSnapshot {
		return RetrySuppressionSnapshot{Pressure: overloadedPressure()}
	}
	res := runWithRetry(context.Background(), FailurePolicy{}, q.retryPolicy, opts, NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
		return 0, RetryableFailure(errors.New("t"))
	})
	trace := RetryTrace{Attempts: res.retryAttempts, Final: res.retryFinal}
	if !trace.HadSuppression(RetrySuppressionGlobalOverload) {
		t.Fatalf("trace = %+v", trace)
	}
	if trace.Final.SuppressionReason != RetrySuppressionGlobalOverload {
		t.Fatalf("final = %+v", trace.Final)
	}
}

func TestRetryTraceFromFutureExhausted(t *testing.T) {
	now := time.Now()
	p := RetryPolicy{Enabled: true, MaxAttempts: 2, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	res := runWithRetry(context.Background(), FailurePolicy{}, p, runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetySafe}}, NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
		return 0, RetryableFailure(errors.New("transient"))
	})
	future := newResultFuture[int]()
	future.setRetryOutcome(res.retryAttempts, res.retryFinal, res.retryTracked)
	future.complete(0, res.err, FailurePolicy{}, res.budget, false)

	trace, ok := RetryTraceFromFuture(future)
	if !ok || !trace.Final.Exhausted {
		t.Fatalf("ok=%v final=%+v", ok, trace.Final)
	}
	if trace.Final.StoppedReason != RetryDecisionMaxAttempts {
		t.Fatalf("stopped = %q", trace.Final.StoppedReason)
	}
	traceAny, ok := RetryTraceFromFutureAny(future)
	if !ok || !traceAny.Final.Exhausted {
		t.Fatalf("any ok=%v final=%+v", ok, traceAny.Final)
	}
}

func TestRetryTraceCopyImmutable(t *testing.T) {
	future := newResultFuture[int]()
	future.setRetryOutcome([]RetryAttempt{{Attempt: 1, Lane: "default"}}, RetryFinalState{FailureKind: FailureRetryable}, true)
	future.complete(0, RetryableFailure(errors.New("x")), FailurePolicy{}, DeadlineBudget{}, false)

	trace, ok := RetryTraceFromFuture(future)
	if !ok {
		t.Fatal("expected trace")
	}
	trace.Attempts[0].Lane = "mutated"
	trace2, _ := RetryTraceFromFuture(future)
	if trace2.Attempts[0].Lane != "default" {
		t.Fatalf("mutation leaked: %+v", trace2.Attempts[0])
	}
}

func TestRetryTracePermanentFailureWithoutAttempts(t *testing.T) {
	now := time.Now()
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	res := runWithRetry(context.Background(), FailurePolicy{}, p, runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetySafe}}, NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
		return 0, PermanentFailure(errors.New("perm"))
	})
	if len(res.retryAttempts) != 0 {
		t.Fatalf("attempts = %+v", res.retryAttempts)
	}
	if !res.retryTracked || res.retryFinal.StoppedReason != RetryDecisionPermanentFailure {
		t.Fatalf("final = %+v tracked=%v", res.retryFinal, res.retryTracked)
	}
}
