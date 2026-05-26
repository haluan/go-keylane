// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"sync/atomic"
	"testing"
)

func newContinuationRetryQueue(t *testing.T, ctx context.Context, retry RetryPolicy) *Queue {
	t.Helper()
	cfg := newTestConfig()
	cfg.Continuation = ContinuationConfig{Enabled: true, MaxPending: 10}
	cfg.Retry = retry
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return q
}

func TestPipelineContinuationPermanentFailureDoesNotRetry(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationRetryQueue(t, ctx, RetryPolicy{Enabled: true, MaxAttempts: 3})

	var runCount int32
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					atomic.AddInt32(&runCount, 1)
					cont, c := NewContinuation[pState](context.Background())
					go func() { c.Fail(errPermanentForTest) }()
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, awaitErr := future.Await(context.Background())
	if awaitErr == nil {
		t.Fatal("expected error")
	}
	assertFutureFailureKind(t, future, FailurePermanent)

	if n := atomic.LoadInt32(&runCount); n != 1 {
		t.Fatalf("pipeline ran %d times, want 1 (permanent failure should not retry)", n)
	}
}

func TestPipelineContinuationRetryableFailureUsesRetryPolicy(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationRetryQueue(t, ctx, RetryPolicy{Enabled: true, MaxAttempts: 3})

	var runCount int32
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta:        RequestMeta{Key: "k", Lane: "default"},
		Idempotency: retrySafeIdempotency(),
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					n := atomic.AddInt32(&runCount, 1)
					cont, c := NewContinuation[pState](context.Background())
					if n < 3 {
						// Fail with retryable error on attempts 1 and 2.
						go func() { c.Fail(RetryableFailure(errContinuationTestSentinel{})) }()
					} else {
						// Succeed on attempt 3.
						go func() { c.Complete(pState{Val: int(n)}) }()
					}
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}

	out, awaitErr := future.Await(context.Background())
	if awaitErr != nil {
		t.Fatalf("Await: %v", awaitErr)
	}
	if n := atomic.LoadInt32(&runCount); n != 3 {
		t.Fatalf("pipeline ran %d times, want 3", n)
	}
	if out.Sum != 3 {
		t.Fatalf("output = %d, want 3", out.Sum)
	}
}

func TestPipelineContinuationRetryExhaustedCompletesWithFailure(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationRetryQueue(t, ctx, RetryPolicy{Enabled: true, MaxAttempts: 2})

	var runCount int32
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta:        RequestMeta{Key: "k", Lane: "default"},
		Idempotency: retrySafeIdempotency(),
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					atomic.AddInt32(&runCount, 1)
					cont, c := NewContinuation[pState](context.Background())
					go func() { c.Fail(RetryableFailure(errContinuationTestSentinel{})) }()
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, awaitErr := future.Await(context.Background())
	if awaitErr == nil {
		t.Fatal("expected error after retry exhaustion")
	}

	if n := atomic.LoadInt32(&runCount); n != 2 {
		t.Fatalf("pipeline ran %d times, want 2 (MaxAttempts=2)", n)
	}
}

func testPipelineContinuationUnsafeMutationRetrySuppressed(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationRetryQueue(t, ctx, RetryPolicy{Enabled: true, MaxAttempts: 3})

	var runCount int32
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta:        RequestMeta{Key: "k", Lane: "default"},
		Idempotency: Idempotency{Safety: RetrySafetyUnsafe},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					atomic.AddInt32(&runCount, 1)
					cont, c := NewContinuation[pState](context.Background())
					go func() { c.Fail(RetryableFailure(errContinuationTestSentinel{})) }()
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, awaitErr := future.Await(context.Background()); awaitErr == nil {
		t.Fatal("expected error")
	}
	if n := atomic.LoadInt32(&runCount); n != 1 {
		t.Fatalf("pipeline ran %d times, want 1 (unsafe idempotency suppresses retry)", n)
	}
	trace, ok := RetryTraceFromFuture(future)
	if !ok {
		t.Fatal("expected retry trace on continuation failure")
	}
	if trace.Final.SafetyReason != RetrySafetyDecisionUnsafe {
		t.Fatalf("final safety reason = %v, want unsafe", trace.Final.SafetyReason)
	}
}

func TestPipelineContinuationUnsafeMutationRetrySuppressed(t *testing.T) {
	testPipelineContinuationUnsafeMutationRetrySuppressed(t)
}

func TestPipelineContinuationRetrySuppressedWhenIdempotencyUnsafe(t *testing.T) {
	testPipelineContinuationUnsafeMutationRetrySuppressed(t)
}

func TestPipelineContinuationRetrySuppressedByPolicy(t *testing.T) {
	ctx := testTimeout(t)
	cfg := newTestConfig()
	cfg.ShardCount = 1
	cfg.WorkerCount = 1
	cfg.QueueSizePerLane = 10
	cfg.LaneQuotas = map[Lane]int{"default": 1}
	cfg.Continuation = ContinuationConfig{Enabled: true, MaxPending: 10}
	cfg.Retry = RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: 1, Jitter: false, MinRemainingBudget: 0}
	cfg.RetrySuppression = RetrySuppressionPolicy{Enabled: true, SuppressWhenOverloaded: true}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Stop(context.Background()) })

	hold := make(chan struct{})
	defer close(hold)

	var runCount int32
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta:        RequestMeta{Key: "k", Lane: "default"},
		Idempotency: retrySafeIdempotency(),
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					if atomic.AddInt32(&runCount, 1) == 1 {
						fillQueueForOverload(t, q, ctx, hold, 9)
					}
					cont, c := NewContinuation[pState](context.Background())
					go func() { c.Fail(RetryableFailure(errContinuationTestSentinel{})) }()
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, awaitErr := future.Await(context.Background()); awaitErr == nil {
		t.Fatal("expected error")
	}
	if n := atomic.LoadInt32(&runCount); n != 1 {
		t.Fatalf("pipeline ran %d times, want 1 (suppression blocks retry)", n)
	}
	trace, ok := RetryTraceFromFuture(future)
	if !ok {
		t.Fatal("missing retry trace")
	}
	if !trace.HadSuppression(RetrySuppressionGlobalOverload) &&
		!trace.HadSuppression(RetrySuppressionLanePressure) {
		t.Fatalf("retry trace = %+v", trace)
	}
}

func testPipelineContinuationRetryTraceIncludesFailure(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationRetryQueue(t, ctx, RetryPolicy{
		Enabled: true, MaxAttempts: 3, InitialBackoff: 1, Jitter: false,
	})

	var runCount int32
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta:        RequestMeta{Key: "k", Lane: "default"},
		Idempotency: retrySafeIdempotency(),
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					n := atomic.AddInt32(&runCount, 1)
					cont, c := NewContinuation[pState](context.Background())
					if n < 2 {
						go func() { c.Fail(RetryableFailure(errContinuationTestSentinel{})) }()
					} else {
						go func() { c.Complete(pState{Val: 2}) }()
					}
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := future.Await(context.Background()); err != nil {
		t.Fatal(err)
	}
	trace, ok := RetryTraceFromFuture(future)
	if !ok || len(trace.Attempts) != 1 {
		t.Fatalf("retry trace = %+v ok=%v", trace, ok)
	}
	if !trace.Final.Succeeded {
		t.Fatalf("final state = %+v", trace.Final)
	}
}

func TestPipelineContinuationRetryTraceIncludesFailure(t *testing.T) {
	testPipelineContinuationRetryTraceIncludesFailure(t)
}

func TestPipelineContinuationRetryableFailureRecordsRetryTrace(t *testing.T) {
	testPipelineContinuationRetryTraceIncludesFailure(t)
}
