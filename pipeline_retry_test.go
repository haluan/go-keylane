// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestSubmitPipelineRequestLevelRetryRerunsAllStages(t *testing.T) {
	q, ctx := retryTestQueue(t, RetryPolicy{
		Enabled: true, MaxAttempts: 3, InitialBackoff: 1, Jitter: false, MinRemainingBudget: 0,
	})

	var stageRuns atomic.Int32
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta:        RequestMeta{Key: "k", Lane: "default"},
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: retrySafeIdempotency(),
		Stages: []PipelineStage[pState]{
			validPipelineStage(StageValidate, func(*pState) { stageRuns.Add(1) }),
			retryTransientBusinessStage(&stageRuns),
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	out, awaitErr := future.Await(context.Background())
	if awaitErr != nil {
		t.Fatal(awaitErr)
	}
	if out.Sum != 9 {
		t.Fatalf("sum = %d", out.Sum)
	}
	// Two stages per attempt; business fails once then succeeds on attempt 2.
	if stageRuns.Load() != 4 {
		t.Fatalf("stage runs = %d, want 4 (2 stages x 2 attempts)", stageRuns.Load())
	}
}

func TestSubmitPipelineRetryPermanentPreservesStageFailure(t *testing.T) {
	q, ctx := retryTestQueue(t, RetryPolicy{
		Enabled: true, MaxAttempts: 3, InitialBackoff: 1, Jitter: false, MinRemainingBudget: 0,
	})

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta:        RequestMeta{Key: "k", Lane: "default"},
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: retrySafeIdempotency(),
		Stages: []PipelineStage[pState]{
			permanentFailStage(StageBusiness, errors.New("bad input")),
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

	sf, ok := AsStageFailure(awaitErr)
	if !ok {
		t.Fatalf("expected StageFailure, got %T", awaitErr)
	}
	if sf.Stage.Name != StageBusiness {
		t.Fatalf("stage = %q, want business", sf.Stage.Name)
	}
	assertFutureFailureKind(t, future, FailurePermanent)
}

func TestSubmitPipelineRetryExhaustedPreservesStageFailure(t *testing.T) {
	q, ctx := retryTestQueue(t, RetryPolicy{
		Enabled: true, MaxAttempts: 2, InitialBackoff: 1, Jitter: false, MinRemainingBudget: 0,
	})

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta:        RequestMeta{Key: "k", Lane: "default"},
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: retrySafeIdempotency(),
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageExternalAPI},
				Run: func(_ context.Context, st pState) (pState, error) {
					return st, RetryableFailure(errors.New("transient"))
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

	sf, ok := AsStageFailure(awaitErr)
	if !ok {
		t.Fatalf("expected StageFailure, got %T", awaitErr)
	}
	if sf.Stage.Name != StageExternalAPI {
		t.Fatalf("stage = %q, want external_api", sf.Stage.Name)
	}
	assertFutureFailureKind(t, future, FailureRetryable)
}

func retryTransientBusinessStage(runs *atomic.Int32) PipelineStage[pState] {
	return PipelineStage[pState]{
		Meta: StageMeta{Name: StageBusiness},
		Run: func(_ context.Context, st pState) (pState, error) {
			n := runs.Add(1)
			if n < 4 {
				return st, RetryableFailure(errors.New("transient"))
			}
			st.Val = 9
			return st, nil
		},
	}
}
