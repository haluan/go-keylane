// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPipelineContinuationRequestCancellationWhileYielded(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 10)

	reqCtx, reqCancel := context.WithCancel(context.Background())
	defer reqCancel()

	var completer ContinuationCompleter[pState]
	yielded := make(chan struct{})

	future, err := SubmitPipeline(reqCtx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](context.Background())
					completer = c
					close(yielded)
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
			{
				Meta: StageMeta{Name: StageBusiness},
				Run: func(_ context.Context, st pState) (pState, error) {
					t.Error("stage 2 should not run when request is cancelled while yielded")
					return st, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for the yield, then cancel the request context.
	waitForSignal(t, yielded)
	reqCancel()

	_, awaitErr := future.Await(ctx)
	if awaitErr == nil {
		t.Fatal("expected error after request cancellation while yielded")
	}

	// Verify it's a cancellation error.
	if !errors.Is(awaitErr, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %T: %v", awaitErr, awaitErr)
	}

	// Late Complete after cancellation must not panic or change future result.
	waitUntil(t, func() bool { return completer != nil }, 2*time.Second)
	if completer.Complete(pState{Val: 99}) {
		t.Fatal("late Complete should return false after cancellation resolved continuation")
	}
	snap := q.DebugSnapshot().Continuation
	if snap.LateCompletions != 1 {
		t.Fatalf("LateCompletions = %d, want 1", snap.LateCompletions)
	}

	// Future remains cancelled.
	_, awaitErr2 := future.Await(ctx)
	if !errors.Is(awaitErr2, context.Canceled) {
		t.Fatalf("second Await: expected context.Canceled, got %v", awaitErr2)
	}
}

func TestPipelineContinuationCompleteAfterCancelIgnored(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 10)

	reqCtx, reqCancel := context.WithCancel(context.Background())
	defer reqCancel()

	yielded := make(chan struct{})
	var completer ContinuationCompleter[pState]

	future, err := SubmitPipeline(reqCtx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](context.Background())
					completer = c
					close(yielded)
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
			{
				Meta: StageMeta{Name: StageBusiness},
				Run: func(_ context.Context, st pState) (pState, error) {
					t.Error("resume stage must not run when completion after cancel is ignored")
					return st, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}

	waitForSignal(t, yielded)
	reqCancel()

	waitUntil(t, func() bool {
		select {
		case <-future.Done():
			return true
		default:
			return false
		}
	}, 2*time.Second)

	if completer.Complete(pState{Val: 99}) {
		t.Fatal("Complete after request cancel must return false")
	}
	snap := q.DebugSnapshot().Continuation
	if snap.LateCompletions < 1 {
		t.Fatalf("LateCompletions = %d, want at least 1", snap.LateCompletions)
	}

	_, awaitErr := future.Await(ctx)
	if !errors.Is(awaitErr, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", awaitErr)
	}
}

func TestPipelineContinuationCompleteAcceptedBeforeCancelNotLate(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 10)

	reqCtx, reqCancel := context.WithCancel(context.Background())
	defer reqCancel()

	yielded := make(chan struct{})
	resumed := make(chan struct{})
	unblockResume := make(chan struct{})
	var completer ContinuationCompleter[pState]

	future, err := SubmitPipeline(reqCtx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(stageCtx context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](stageCtx)
					completer = c
					close(yielded)
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
			{
				Meta: StageMeta{Name: StageBusiness},
				Run: func(ctx context.Context, st pState) (pState, error) {
					close(resumed)
					<-unblockResume
					if err := ctx.Err(); err != nil {
						return st, err
					}
					return st, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}

	waitForSignal(t, yielded)
	waitUntil(t, func() bool { return q.DebugSnapshot().Continuation.Pending > 0 }, 2*time.Second)
	lateBefore := q.DebugSnapshot().Continuation.LateCompletions

	if !completer.Complete(pState{Val: 5}) {
		t.Fatal("Complete must return true when continuation accepts the outcome")
	}
	waitUntil(t, func() bool {
		return q.DebugSnapshot().Continuation.LateCompletions == lateBefore
	}, 2*time.Second)

	waitForSignal(t, resumed)
	reqCancel()
	close(unblockResume)

	_, awaitErr := future.Await(ctx)
	if !errors.Is(awaitErr, context.Canceled) {
		t.Fatalf("expected context.Canceled after resume, got %v", awaitErr)
	}
	if q.DebugSnapshot().Continuation.LateCompletions != lateBefore {
		t.Fatalf("LateCompletions = %d after cancel-on-resume, want %d (accepted completion is not late)", q.DebugSnapshot().Continuation.LateCompletions, lateBefore)
	}
}

func TestPipelineContinuationAwaitCancellationDoesNotCancelRequest(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 10)

	yielded := make(chan struct{})
	var completer ContinuationCompleter[pState]

	// reqCtx is NOT cancelled — only the Await context is.
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](context.Background())
					completer = c
					close(yielded)
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}

	waitForSignal(t, yielded)

	// Cancel just the Await context, not the request context.
	awaitCtx, awaitCancel := context.WithCancel(context.Background())
	awaitCancel()

	_, awaitErr := future.Await(awaitCtx)
	if !errors.Is(awaitErr, context.Canceled) {
		t.Fatalf("await cancel: expected context.Canceled, got %v", awaitErr)
	}

	// The continuation should still be pending (future not yet completed).
	select {
	case <-future.Done():
		t.Fatal("future should still be pending: Await cancellation must not cancel the continuation")
	default:
	}

	// Completing the continuation now should still succeed.
	waitUntil(t, func() bool { return completer != nil }, 2*time.Second)
	completer.Complete(pState{Val: 5})

	out, awaitErr2 := future.Await(context.Background())
	if awaitErr2 != nil {
		t.Fatalf("after continuation complete: %v", awaitErr2)
	}
	if out.Sum != 5 {
		t.Fatalf("output = %d, want 5", out.Sum)
	}
}

func TestPipelineContinuationCancelBeforeYieldDoesNotRegister(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 10)

	reqCtx, reqCancel := context.WithCancel(context.Background())
	defer reqCancel()

	stageStarted := make(chan struct{})
	var completer ContinuationCompleter[pState]

	future, err := SubmitPipeline(reqCtx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					close(stageStarted)
					reqCancel()
					cont, c := NewContinuation[pState](context.Background())
					completer = c
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}

	waitForSignal(t, stageStarted)

	_, awaitErr := future.Await(ctx)
	if !errors.Is(awaitErr, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", awaitErr)
	}

	snap := q.DebugSnapshot().Continuation
	if snap.Pending != 0 {
		t.Fatalf("Pending = %d, want 0 (continuation must not register)", snap.Pending)
	}
	if snap.LateCompletions != 0 {
		t.Fatalf("LateCompletions = %d, want 0", snap.LateCompletions)
	}
	if completer.Complete(pState{Val: 1}) {
		t.Fatal("Complete after cancel-before-yield should return false")
	}
}
