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

func idempotencyTestQueue(t *testing.T, idem IdempotencyPolicy) (*Queue, context.Context) {
	t.Helper()
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[Lane]int{"default": 1},
		Retry: RetryPolicy{
			Enabled: true, MaxAttempts: 3, InitialBackoff: 1 * time.Millisecond,
			Jitter: false, MinRemainingBudget: 0,
		},
		Idempotency: idem,
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

func TestIntegrationIdempotencySubmitValueSafeRetries(t *testing.T) {
	q, ctx := idempotencyTestQueue(t, IdempotencyPolicy{})
	var sideEffects atomic.Int32
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default",
		Idempotency: Idempotency{Safety: RetrySafetySafe},
		Run: func(context.Context) (int, error) {
			if sideEffects.Add(1) < 3 {
				return 0, RetryableFailure(errors.New("transient"))
			}
			return 1, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, awaitErr := future.Await(context.Background()); awaitErr != nil {
		t.Fatal(awaitErr)
	}
	if sideEffects.Load() != 3 {
		t.Fatalf("side effects = %d", sideEffects.Load())
	}
}

func TestIntegrationIdempotencySubmitValueUnsafeSingleSideEffect(t *testing.T) {
	q, ctx := idempotencyTestQueue(t, IdempotencyPolicy{})
	var sideEffects atomic.Int32
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default",
		Idempotency: Idempotency{Safety: RetrySafetyUnsafe},
		Run: func(context.Context) (int, error) {
			sideEffects.Add(1)
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
	if sideEffects.Load() != 1 {
		t.Fatalf("side effects = %d", sideEffects.Load())
	}
}

func TestIntegrationIdempotencySubmitValueRequiresCheckHookAllow(t *testing.T) {
	q, ctx := idempotencyTestQueue(t, IdempotencyPolicy{
		Hook: func(context.Context, RetrySafetyCheck) RetrySafetyDecision {
			return RetrySafetyDecision{Allow: true, Reason: RetrySafetyDecisionHookAllowed}
		},
	})
	var sideEffects atomic.Int32
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default",
		Idempotency: Idempotency{Safety: RetrySafetyRequiresCheck, Key: "idem"},
		Run: func(context.Context) (int, error) {
			if sideEffects.Add(1) < 2 {
				return 0, RetryableFailure(errors.New("transient"))
			}
			return 1, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, awaitErr := future.Await(context.Background()); awaitErr != nil {
		t.Fatal(awaitErr)
	}
	if sideEffects.Load() != 2 {
		t.Fatalf("side effects = %d", sideEffects.Load())
	}
}

func TestIntegrationIdempotencySubmitValueRequiresCheckHookReject(t *testing.T) {
	q, ctx := idempotencyTestQueue(t, IdempotencyPolicy{
		Hook: func(context.Context, RetrySafetyCheck) RetrySafetyDecision {
			return RetrySafetyDecision{Allow: false, Reason: RetrySafetyDecisionHookRejected}
		},
	})
	var sideEffects atomic.Int32
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default",
		Idempotency: Idempotency{Safety: RetrySafetyRequiresCheck, Key: "idem"},
		Run: func(context.Context) (int, error) {
			sideEffects.Add(1)
			return 0, RetryableFailure(errors.New("transient"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, awaitErr := future.Await(context.Background()); awaitErr == nil {
		t.Fatal("expected error")
	}
	if sideEffects.Load() != 1 {
		t.Fatalf("side effects = %d", sideEffects.Load())
	}
}

func TestIntegrationIdempotencySubmitValueRequireForRetrySafeEmptyKey(t *testing.T) {
	q, ctx := idempotencyTestQueue(t, IdempotencyPolicy{RequireForRetry: true})
	var sideEffects atomic.Int32
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default",
		Idempotency: Idempotency{Safety: RetrySafetySafe},
		Run: func(context.Context) (int, error) {
			sideEffects.Add(1)
			return 0, RetryableFailure(errors.New("transient"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, awaitErr := future.Await(context.Background()); awaitErr == nil {
		t.Fatal("expected error")
	}
	if sideEffects.Load() != 1 {
		t.Fatalf("side effects = %d", sideEffects.Load())
	}
}

func TestIntegrationIdempotencySubmitValueRequireForRetryMissingKey(t *testing.T) {
	q, ctx := idempotencyTestQueue(t, IdempotencyPolicy{RequireForRetry: true})
	var sideEffects atomic.Int32
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default",
		Idempotency: Idempotency{Safety: RetrySafetyRequiresCheck},
		Run: func(context.Context) (int, error) {
			sideEffects.Add(1)
			return 0, RetryableFailure(errors.New("transient"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, awaitErr := future.Await(context.Background()); awaitErr == nil {
		t.Fatal("expected error")
	}
	if sideEffects.Load() != 1 {
		t.Fatalf("side effects = %d", sideEffects.Load())
	}
}

func TestIntegrationIdempotencySubmitRequestSafeRetrySuccess(t *testing.T) {
	q, ctx := idempotencyTestQueue(t, IdempotencyPolicy{})
	var sideEffects atomic.Int32
	future, err := SubmitRequest(ctx, q, Request[struct{}, int]{
		Meta:        RequestMeta{Key: "k", Lane: "default"},
		Input:       struct{}{},
		Idempotency: Idempotency{Safety: RetrySafetySafe},
		Handle: func(context.Context, struct{}) (int, error) {
			if sideEffects.Add(1) < 2 {
				return 0, RetryableFailure(errors.New("transient"))
			}
			return 5, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	out, awaitErr := future.Await(context.Background())
	if awaitErr != nil || out != 5 {
		t.Fatalf("out=%d err=%v", out, awaitErr)
	}
}

func TestIntegrationIdempotencySubmitRequestUnsafeNoDuplicateSideEffects(t *testing.T) {
	q, ctx := idempotencyTestQueue(t, IdempotencyPolicy{})
	var sideEffects atomic.Int32
	future, err := SubmitRequest(ctx, q, Request[struct{}, struct{}]{
		Meta:        RequestMeta{Key: "k", Lane: "default"},
		Input:       struct{}{},
		Idempotency: Idempotency{Safety: RetrySafetyUnsafe},
		Handle: func(context.Context, struct{}) (struct{}, error) {
			sideEffects.Add(1)
			return struct{}{}, RetryableFailure(errors.New("transient"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, awaitErr := future.Await(context.Background()); awaitErr == nil {
		t.Fatal("expected error")
	}
	if sideEffects.Load() != 1 {
		t.Fatalf("side effects = %d", sideEffects.Load())
	}
}

func TestIntegrationIdempotencyExplicitUnsafeRetryObservable(t *testing.T) {
	q, ctx := idempotencyTestQueue(t, IdempotencyPolicy{})
	var sideEffects atomic.Int32
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default",
		Idempotency: Idempotency{Safety: RetrySafetyUnsafe, AllowUnsafeRetry: true},
		Run: func(context.Context) (int, error) {
			if sideEffects.Add(1) < 2 {
				return 0, RetryableFailure(errors.New("transient"))
			}
			return 1, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, awaitErr := future.Await(context.Background()); awaitErr != nil {
		t.Fatal(awaitErr)
	}
	trace, ok := RetryTraceFromFuture(future)
	if !ok {
		t.Fatal("RetryTraceFromFuture: not available")
	}
	if !trace.HadExplicitUnsafeRetry() {
		t.Fatalf("retry trace = %+v", trace)
	}
}

func TestIntegrationIdempotencySubmitRequestHookReceivesContext(t *testing.T) {
	var captured RetrySafetyCheck
	q, ctx := idempotencyTestQueue(t, IdempotencyPolicy{
		Hook: func(_ context.Context, check RetrySafetyCheck) RetrySafetyDecision {
			captured = check
			return RetrySafetyDecision{Allow: false}
		},
	})
	future, err := SubmitRequest(ctx, q, Request[struct{}, struct{}]{
		Meta:        RequestMeta{Key: "route-key", Lane: "default"},
		Input:       struct{}{},
		Idempotency: Idempotency{Safety: RetrySafetyRequiresCheck, Key: "idem-key"},
		Handle: func(context.Context, struct{}) (struct{}, error) {
			return struct{}{}, RetryableFailure(errors.New("transient"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = future.Await(context.Background())
	if captured.Key != "route-key" {
		t.Fatalf("key = %q", captured.Key)
	}
	if captured.Lane != "default" {
		t.Fatalf("lane = %q", captured.Lane)
	}
	if captured.ShardID < 0 {
		t.Fatalf("shardID = %d", captured.ShardID)
	}
	if captured.Attempt != 1 {
		t.Fatalf("attempt = %d", captured.Attempt)
	}
	if captured.Idempotency.Key != "idem-key" {
		t.Fatalf("idem key = %q", captured.Idempotency.Key)
	}
	if captured.Failure.Kind != FailureRetryable {
		t.Fatalf("failure kind = %s", captured.Failure.Kind)
	}
	if !captured.Retry.Enabled {
		t.Fatal("expected retry enabled in check")
	}
}
