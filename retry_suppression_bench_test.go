// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"testing"
	"time"
)

func BenchmarkDecideRetrySuppressionHealthy(b *testing.B) {
	check := RetrySuppressionCheck{
		Attempt:   1,
		Failure:   RetryableFailure(errors.New("transient")),
		Retry:     RetryPolicy{Enabled: true},
		Pressure:  Pressure{TotalDepthRatio: 0.1, IsHealthy: true},
		LaneClass: LaneNormal,
	}
	policy := RetrySuppressionPolicy{Enabled: true}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DecideRetrySuppression(ctx, policy, check)
	}
}

func BenchmarkDecideRetrySuppressionOverloaded(b *testing.B) {
	check := RetrySuppressionCheck{
		Attempt:   1,
		Failure:   RetryableFailure(errors.New("transient")),
		Retry:     RetryPolicy{Enabled: true},
		Pressure:  Pressure{TotalDepthRatio: OverloadedDepthRatio, IsOverloaded: true},
		LaneClass: LaneNormal,
	}
	policy := RetrySuppressionPolicy{Enabled: true}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DecideRetrySuppression(ctx, policy, check)
	}
}

func BenchmarkRetrySuppressionSnapshot(b *testing.B) {
	cfg := Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 100,
		LaneQuotas: map[Lane]int{"default": 1},
	}
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.RetrySuppressionSnapshot("k", "default", 0)
	}
}

func benchSuppressionUnderPressureOpts() runWithRetryOpts {
	policy := RetrySuppressionPolicy{Enabled: true}
	NormalizeRetrySuppressionPolicy(&policy)
	return runWithRetryOpts{
		Idempotency:       Idempotency{Safety: RetrySafetySafe},
		SuppressionPolicy: policy,
		Snapshot: func(string, Lane, int) RetrySuppressionSnapshot {
			return RetrySuppressionSnapshot{
				Pressure:  Pressure{TotalDepthRatio: OverloadedDepthRatio, IsOverloaded: true},
				LaneClass: LaneNormal,
			}
		},
	}
}

func BenchmarkRunWithRetrySuppressedUnderPressure(b *testing.B) {
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Nanosecond, Jitter: false}
	now := time.Now()
	opts := benchSuppressionUnderPressureOpts()
	budget := NewDeadlineBudget(context.Background(), now)
	clock := &testRetryClock{now: now}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = runWithRetry(context.Background(), FailurePolicy{}, p, opts, budget, clock, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
			return 0, RetryableFailure(errors.New("transient"))
		})
	}
}

func BenchmarkRunWithRetrySuppressionTrace(b *testing.B) {
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Nanosecond, Jitter: false}
	now := time.Now()
	opts := benchSuppressionUnderPressureOpts()
	budget := NewDeadlineBudget(context.Background(), now)
	clock := &testRetryClock{now: now}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := runWithRetry(context.Background(), FailurePolicy{}, p, opts, budget, clock, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
			return 0, RetryableFailure(errors.New("transient"))
		})
		trace := RetryTrace{Attempts: res.retryAttempts}
		_ = trace.HadSuppression(RetrySuppressionGlobalOverload)
	}
}

func BenchmarkRunWithRetrySuppressionDisabled(b *testing.B) {
	p := RetryPolicy{Enabled: true, MaxAttempts: 2, InitialBackoff: time.Nanosecond, Jitter: false}
	now := time.Now()
	opts := runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetySafe}}
	budget := NewDeadlineBudget(context.Background(), now)
	clock := &testRetryClock{now: now}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = runWithRetry(context.Background(), FailurePolicy{}, p, opts, budget, clock, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
			return 1, nil
		})
	}
}
