// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSubmitPipelineStageFailurePreservesFailureKind(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
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
		t.Fatalf("stage = %q", sf.Stage.Name)
	}
	if sf.Execution.Stage.Name != StageBusiness {
		t.Fatalf("execution stage = %q", sf.Execution.Stage.Name)
	}

	assertFutureFailureKind(t, future, FailurePermanent)
}

func TestSubmitPipelineDeadlineExhaustedBeforeSecondStage(t *testing.T) {
	q, _ := retryTestQueue(t, RetryPolicy{Enabled: false})
	reqCtx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	var stage2Runs bool
	future, err := SubmitPipeline(reqCtx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				Run: func(_ context.Context, st pState) (pState, error) {
					time.Sleep(60 * time.Millisecond)
					return st, nil
				},
			},
			{
				Meta: StageMeta{Name: StageDBRead},
				Run: func(_ context.Context, st pState) (pState, error) {
					stage2Runs = true
					return st, nil
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
	if stage2Runs {
		t.Fatal("stage 2 should not run when budget exhausted before it starts")
	}

	sf, ok := AsStageFailure(awaitErr)
	if !ok {
		t.Fatalf("expected StageFailure, got %T", awaitErr)
	}
	if sf.Stage.Name != StageDBRead {
		t.Fatalf("skipped stage = %q, want %q", sf.Stage.Name, StageDBRead)
	}
	if !sf.Execution.Deadline.BudgetExhausted {
		t.Fatal("expected BudgetExhausted on skipped stage execution")
	}

	assertFutureFailureKind(t, future, FailureDeadlineExhausted)
	if !errors.Is(awaitErr, context.DeadlineExceeded) {
		t.Fatalf("expected errors.Is(awaitErr, context.DeadlineExceeded), got %T: %v", awaitErr, awaitErr)
	}
}

func TestSubmitPipelineTypedDeadlineExhaustedDuringStageIsTimeout(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageBusiness},
				Run: func(_ context.Context, st pState) (pState, error) {
					return st, DeadlineExhaustedFailure(context.DeadlineExceeded)
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
	if _, ok := AsStageFailure(awaitErr); !ok {
		t.Fatalf("expected StageFailure, got %T", awaitErr)
	}
	assertFutureFailureKind(t, future, FailureTimeout)
}

func TestSubmitPipelineStageContextTimeoutMatchesRequestClassification(t *testing.T) {
	policy := FailurePolicy{
		Classifier: func(err error) Failure {
			if errors.Is(err, context.DeadlineExceeded) {
				return PermanentFailure(err)
			}
			return Failure{}
		},
	}
	q, _ := retryTestQueueWithFailurePolicy(t, policy)

	waitOnCtx := func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}

	reqCtx, reqCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer reqCancel()
	reqFuture, err := SubmitRequest(reqCtx, q, Request[sumInput, sumOutput]{
		Meta:  RequestMeta{Key: "k", Lane: "default"},
		Input: sumInput{},
		Handle: func(ctx context.Context, _ sumInput) (sumOutput, error) {
			return sumOutput{}, waitOnCtx(ctx)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = reqFuture.Await(context.Background())
	assertFutureFailureKind(t, reqFuture, FailureTimeout)

	pipeCtx, pipeCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer pipeCancel()
	pipeFuture, err := SubmitPipeline(pipeCtx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k2", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageBusiness},
				Run: func(ctx context.Context, st pState) (pState, error) {
					return st, waitOnCtx(ctx)
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, pipeErr := pipeFuture.Await(context.Background())
	if pipeErr == nil {
		t.Fatal("expected pipeline error")
	}
	if _, ok := AsStageFailure(pipeErr); !ok {
		t.Fatalf("expected StageFailure, got %T", pipeErr)
	}
	assertFutureFailureKind(t, pipeFuture, FailureTimeout)
}

func retryTestQueueWithFailurePolicy(t *testing.T, policy FailurePolicy) (*Queue, context.Context) {
	t.Helper()
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[Lane]int{"default": 1},
		FailurePolicy:    policy,
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

func TestNewStageFailureUnwrap(t *testing.T) {
	root := errors.New("root")
	wrapped := NewStageFailure(StageMeta{Name: StageValidate}, root)
	if !errors.Is(wrapped, root) {
		t.Fatal("unwrap failed")
	}
	sf, ok := AsStageFailure(wrapped)
	if !ok || sf.Stage.Name != StageValidate {
		t.Fatalf("AsStageFailure = %+v ok=%v", sf, ok)
	}
}
