// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"testing"
	"time"
)

func BenchmarkClassifyFailureObservable(b *testing.B) {
	err := RetryableFailure(errors.New("transient"))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = classifyFailureWithPolicy(err, FailurePolicy{})
	}
}

func BenchmarkRetryDecisionObservable(b *testing.B) {
	policy := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: 10 * time.Millisecond}
	state := RetryState{
		Attempt: 1,
		Failure: RetryableFailure(errors.New("t")),
		Budget:  NewDeadlineBudget(context.Background(), time.Now()),
		Now:     time.Now(),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DecideRetry(policy, state, fixedJitterSource(0.5))
	}
}

func BenchmarkRetrySafetyObservable(b *testing.B) {
	check := RetrySafetyCheck{
		Attempt: 1, Failure: RetryableFailure(errors.New("t")),
		Retry: RetryPolicy{Enabled: true}, Idempotency: Idempotency{Safety: RetrySafetySafe},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DecideRetrySafety(context.Background(), check, IdempotencyPolicy{})
	}
}

func BenchmarkRetrySuppressionObservable(b *testing.B) {
	check := RetrySuppressionCheck{
		Attempt: 1, Failure: RetryableFailure(errors.New("t")),
		Retry: RetryPolicy{Enabled: true}, Pressure: overloadedPressure(), LaneClass: LaneNormal,
	}
	policy := RetrySuppressionPolicy{Enabled: true}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DecideRetrySuppression(context.Background(), policy, check)
	}
}

func BenchmarkRetryFailureSnapshot(b *testing.B) {
	q, _ := New(newTestConfig())
	q.recordFailureKind(FailureRetryable)
	q.recordFailureKind(FailurePermanent)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.RetryFailureSnapshot()
	}
}

func BenchmarkRetryTraceFromFuture(b *testing.B) {
	future := newResultFuture[int]()
	future.setRetryOutcome([]RetryAttempt{{Attempt: 1, Lane: "default"}}, RetryFinalState{Succeeded: true}, true)
	future.complete(1, nil, FailurePolicy{}, DeadlineBudget{}, false)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = RetryTraceFromFuture(future)
	}
}

func BenchmarkRetryEventHookDisabled(b *testing.B) {
	q, _ := New(newTestConfig())
	now := time.Now()
	p := RetryPolicy{Enabled: true, MaxAttempts: 2, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	opts := runWithRetryOpts{
		Idempotency: Idempotency{Safety: RetrySafetySafe},
		Observer:    q.retryObserver(),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = runWithRetry(context.Background(), FailurePolicy{}, p, opts, NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), func(int) (int, error) {
			return 0, PermanentFailure(errors.New("x"))
		})
	}
}

func BenchmarkRetryEventHookEnabled(b *testing.B) {
	cfg := newTestConfig()
	cfg.Observability.EnableHooks = true
	cfg.Observability.Hooks.Retry.OnRetryEvent = func(RetryEvent) {}
	q, _ := New(cfg)
	now := time.Now()
	p := RetryPolicy{Enabled: true, MaxAttempts: 2, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	opts := runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetySafe}, Observer: q.retryObserver()}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = runWithRetry(context.Background(), FailurePolicy{}, p, opts, NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), func(int) (int, error) {
			return 0, PermanentFailure(errors.New("x"))
		})
	}
}

func BenchmarkSubmitValueRetryObservability(b *testing.B) {
	cfg := newTestConfig()
	cfg.Retry = RetryPolicy{Enabled: true, MaxAttempts: 2, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	q, _ := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)
	_ = q.Start(ctx)
	b.Cleanup(func() { _ = q.Stop(context.Background()) })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		future, err := SubmitValue(ctx, q, ValueJob[int]{
			Key: "k", Lane: "default", Idempotency: retrySafeIdempotency(),
			Run: func(context.Context) (int, error) {
				return 0, PermanentFailure(errors.New("x"))
			},
		})
		if err == nil {
			_, _ = future.Await(context.Background())
			_, _ = RetryTraceFromFuture(future)
		}
	}
}

func BenchmarkSubmitRequestRetryObservability(b *testing.B) {
	cfg := newTestConfig()
	cfg.Retry = RetryPolicy{Enabled: true, MaxAttempts: 2, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	q, _ := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)
	_ = q.Start(ctx)
	b.Cleanup(func() { _ = q.Stop(context.Background()) })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		future, err := SubmitRequest(ctx, q, Request[struct{}, int]{
			Meta: RequestMeta{Key: "k", Lane: "default"}, Input: struct{}{},
			Idempotency: retrySafeIdempotency(),
			Handle: func(context.Context, struct{}) (int, error) {
				return 0, PermanentFailure(errors.New("x"))
			},
		})
		if err == nil {
			_, _ = future.Await(context.Background())
		}
	}
}

func BenchmarkRetryStormSuppressedWithObservability(b *testing.B) {
	q, _ := New(newTestConfig())
	now := time.Now()
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	suppression := RetrySuppressionPolicy{Enabled: true, SuppressWhenOverloaded: true}
	opts := runWithRetryOpts{
		Idempotency:       Idempotency{Safety: RetrySafetySafe},
		Observer:          q.retryObserver(),
		SuppressionPolicy: suppression,
		Snapshot: func(string, Lane, int) RetrySuppressionSnapshot {
			return RetrySuppressionSnapshot{Pressure: overloadedPressure(), LaneClass: LaneNormal}
		},
	}
	budget := NewDeadlineBudget(context.Background(), now)
	clock := &testRetryClock{now: now}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = runWithRetry(context.Background(), FailurePolicy{}, p, opts, budget, clock, fixedJitterSource(0.5), func(int) (int, error) {
			return 0, RetryableFailure(errors.New("t"))
		})
	}
}
