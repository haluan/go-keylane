// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"strings"
	"testing"
)

func TestSubmitPipelineRunStagePanicClassifiedPermanent(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				Run: func(_ context.Context, st pState) (pState, error) {
					panic("sync run")
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	awaitErr := futureAwaitErr(t, future, ctx)
	sf, ok := AsStageFailure(awaitErr)
	if !ok {
		t.Fatalf("expected StageFailure, got %T: %v", awaitErr, awaitErr)
	}
	if sf.Stage.Name != StageValidate {
		t.Fatalf("stage = %q", sf.Stage.Name)
	}
	if !strings.Contains(awaitErr.Error(), "stage panic") {
		t.Fatalf("error = %v", awaitErr)
	}
	assertFutureFailureKind(t, future, FailurePermanent)
}

func TestSubmitPipelineRunContinuationStagePanicClassifiedPermanent(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 8)

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					panic("run continuation")
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	awaitErr := futureAwaitErr(t, future, ctx)
	sf, ok := AsStageFailure(awaitErr)
	if !ok {
		t.Fatalf("expected StageFailure, got %T: %v", awaitErr, awaitErr)
	}
	if sf.Stage.Name != StageValidate {
		t.Fatalf("stage = %q", sf.Stage.Name)
	}
	if !strings.Contains(awaitErr.Error(), "stage panic") {
		t.Fatalf("error = %v", awaitErr)
	}
	assertFutureFailureKind(t, future, FailurePermanent)
}

func TestSubmitPipelineStagePanicTerminalHooksOnce(t *testing.T) {
	spy := newPipelineHookSpy()
	q, ctx := startedQueueWithRequestHooks(t, spy.hooks())

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				Run: func(_ context.Context, st pState) (pState, error) {
					panic("hook test panic")
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := future.Await(context.Background()); err == nil {
		t.Fatal("expected error")
	}

	<-spy.queued
	waitRequestObservation(t, spy.started)
	waitRequestObservation(t, spy.completed)

	waitStageObservation(t, spy.stageStarted)
	failed := waitStageObservation(t, spy.stageFailed)
	if failed.Stage != StageValidate {
		t.Errorf("stage = %q", failed.Stage)
	}
	if failed.FailureKind != FailurePermanent {
		t.Errorf("failure kind = %q", failed.FailureKind)
	}

	select {
	case <-spy.started:
		t.Fatal("extra request OnStarted")
	default:
	}
	select {
	case <-spy.completed:
		t.Fatal("extra request OnCompleted")
	default:
	}
	select {
	case <-spy.stageStarted:
		t.Fatal("extra OnStageStarted")
	default:
	}
	select {
	case <-spy.stageCompleted:
		t.Fatal("unexpected OnStageCompleted after stage panic")
	default:
	}
	select {
	case <-spy.stageFailed:
		t.Fatal("extra OnStageFailed")
	default:
	}
}

func futureAwaitErr[T any](t *testing.T, f Future[T], ctx context.Context) error {
	t.Helper()
	_, err := f.Await(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
	return err
}
