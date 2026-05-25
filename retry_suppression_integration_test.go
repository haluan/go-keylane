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

func suppressionIntegrationQueue(t *testing.T) (*Queue, context.Context) {
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
		RetrySuppression: RetrySuppressionPolicy{Enabled: true},
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

func fillQueueForOverload(t *testing.T, q *Queue, ctx context.Context, hold <-chan struct{}, n int) {
	t.Helper()
	run := func(context.Context) error { <-hold; return nil }
	for i := 0; i < n; i++ {
		if err := q.Submit(ctx, Job{Key: "fill", Lane: "default", Run: run}); err != nil {
			t.Fatalf("fill %d: %v", i, err)
		}
	}
}

func TestIntegrationRetrySuppressionSubmitValueHealthyRetries(t *testing.T) {
	q, ctx := suppressionIntegrationQueue(t)
	var sideEffects atomic.Int32
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default",
		Idempotency: Idempotency{Safety: RetrySafetySafe},
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

func TestIntegrationRetrySuppressionSubmitValueOverloadedSingleSideEffect(t *testing.T) {
	q, ctx := suppressionIntegrationQueue(t)
	hold := make(chan struct{})
	defer close(hold)

	var sideEffects atomic.Int32
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k2", Lane: "default",
		Idempotency: Idempotency{Safety: RetrySafetySafe},
		Run: func(context.Context) (int, error) {
			if sideEffects.Add(1) == 1 {
				fillQueueForOverload(t, q, ctx, hold, 9)
			}
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
	trace, ok := RetryTraceFromFuture(future)
	if !ok {
		t.Fatal("missing retry trace")
	}
	if !trace.HadSuppression(RetrySuppressionGlobalOverload) {
		t.Fatalf("trace = %+v", trace)
	}
}

func TestIntegrationRetrySuppressionSubmitRequestPreservesFailure(t *testing.T) {
	q, ctx := suppressionIntegrationQueue(t)
	hold := make(chan struct{})
	defer close(hold)

	future, err := SubmitRequest(ctx, q, Request[struct{}, struct{}]{
		Meta:        RequestMeta{Key: "k2", Lane: "default"},
		Input:       struct{}{},
		Idempotency: Idempotency{Safety: RetrySafetySafe},
		Handle: func(context.Context, struct{}) (struct{}, error) {
			fillQueueForOverload(t, q, ctx, hold, 9)
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
	trace, ok := RetryTraceFromFuture(future)
	if !ok || !trace.HadSuppression(RetrySuppressionGlobalOverload) {
		t.Fatalf("trace ok=%v %+v", ok, trace)
	}
}

func TestIntegrationRetrySuppressionDisabledPreservesRetries(t *testing.T) {
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[Lane]int{"default": 1},
		Retry: RetryPolicy{
			Enabled: true, MaxAttempts: 3, InitialBackoff: 1 * time.Millisecond,
			Jitter: false, MinRemainingBudget: 0,
		},
		RetrySuppression: RetrySuppressionPolicy{Enabled: false},
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
	var sideEffects atomic.Int32
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default",
		Idempotency: Idempotency{Safety: RetrySafetySafe},
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
