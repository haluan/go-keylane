// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"testing"
	"time"
)

func BenchmarkDecideRetrySafetySafe(b *testing.B) {
	check := RetrySafetyCheck{
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: Idempotency{Safety: RetrySafetySafe},
	}
	policy := IdempotencyPolicy{}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DecideRetrySafety(ctx, check, policy)
	}
}

func BenchmarkDecideRetrySafetyUnsafe(b *testing.B) {
	check := RetrySafetyCheck{
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: Idempotency{Safety: RetrySafetyUnsafe},
	}
	policy := IdempotencyPolicy{}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DecideRetrySafety(ctx, check, policy)
	}
}

func BenchmarkDecideRetrySafetyHook(b *testing.B) {
	check := RetrySafetyCheck{
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: Idempotency{Safety: RetrySafetyRequiresCheck, Key: "k"},
	}
	policy := IdempotencyPolicy{
		Hook: func(context.Context, RetrySafetyCheck) RetrySafetyDecision {
			return RetrySafetyDecision{Allow: true, Reason: RetrySafetyDecisionHookAllowed}
		},
	}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DecideRetrySafety(ctx, check, policy)
	}
}

func BenchmarkSubmitValueRetryDisabled(b *testing.B) {
	q, cancel := setupQueue(1, 1, 100, map[Lane]int{"default": 1})
	defer cancel()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, _ := SubmitValue(ctx, q, ValueJob[int]{
			Key: "k", Lane: "default",
			Run: func(context.Context) (int, error) { return 1, nil },
		})
		_, _ = f.Await(ctx)
	}
}

func BenchmarkSubmitValueRetryEnabledSafe(b *testing.B) {
	cfg := Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 100,
		LaneQuotas: map[Lane]int{"default": 1},
		Retry: RetryPolicy{
			Enabled: true, MaxAttempts: 2, InitialBackoff: time.Nanosecond, Jitter: false,
		},
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
		f, _ := SubmitValue(ctx, q, ValueJob[int]{
			Key: "k", Lane: "default",
			Idempotency: Idempotency{Safety: RetrySafetySafe},
			Run:         func(context.Context) (int, error) { return 1, nil },
		})
		_, _ = f.Await(ctx)
	}
}

func BenchmarkSubmitRequestRetryEnabledSafe(b *testing.B) {
	cfg := Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 100,
		LaneQuotas: map[Lane]int{"default": 1},
		Retry: RetryPolicy{
			Enabled: true, MaxAttempts: 2, InitialBackoff: time.Nanosecond, Jitter: false,
		},
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
		f, _ := SubmitRequest(ctx, q, Request[struct{}, int]{
			Meta:        RequestMeta{Key: "k", Lane: "default"},
			Input:       struct{}{},
			Idempotency: Idempotency{Safety: RetrySafetySafe},
			Handle:      func(context.Context, struct{}) (int, error) { return 1, nil },
		})
		_, _ = f.Await(ctx)
	}
}

func BenchmarkRunWithRetryUnsafeSuppress(b *testing.B) {
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Nanosecond, Jitter: false}
	now := time.Now()
	opts := runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetyUnsafe}}
	budget := NewDeadlineBudget(context.Background(), now)
	clock := &testRetryClock{now: now}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = runWithRetry(context.Background(), FailurePolicy{}, p, opts, budget, clock, fixedJitterSource(0.5), func(int) (int, error) {
			return 0, RetryableFailure(errors.New("transient"))
		})
	}
}
