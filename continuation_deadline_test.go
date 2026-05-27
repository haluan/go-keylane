// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPipelineContinuationDeadlineWhileYielded(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 10)

	// 80ms deadline: stage runs quickly but continuation never completes before deadline.
	reqCtx, reqCancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer reqCancel()

	yielded := make(chan struct{})

	future, err := SubmitPipeline(reqCtx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, _ := NewContinuation[pState](context.Background())
					// Never complete — let deadline fire.
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

	_, awaitErr := future.Await(ctx)
	if awaitErr == nil {
		t.Fatal("expected error when deadline fires while yielded")
	}

	// Must be deadline/cancellation related.
	if !errors.Is(awaitErr, context.DeadlineExceeded) && !errors.Is(awaitErr, context.Canceled) {
		t.Fatalf("expected deadline/cancel error, got %T: %v", awaitErr, awaitErr)
	}

	waitUntil(t, func() bool {
		return q.DebugSnapshot().Continuation.Pending == 0
	}, 2*time.Second)
	if snap := q.DebugSnapshot().Continuation; snap.Pending != 0 {
		t.Fatalf("pending after deadline = %d, want 0", snap.Pending)
	}
}

func TestPipelineContinuationLateCompleteAfterDeadlineIgnored(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 10)

	reqCtx, reqCancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
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
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}

	waitForSignal(t, yielded)

	// Wait for the deadline to fire.
	<-reqCtx.Done()

	// Give resolution goroutine time to cancel the continuation and complete the future.
	waitUntil(t, func() bool {
		select {
		case <-future.Done():
			return true
		default:
			return false
		}
	}, 2*time.Second)

	// Late completion after deadline should not change the future.
	if completer.Complete(pState{Val: 99}) {
		t.Fatal("late Complete should return false after deadline cancelled continuation")
	}
	snap := q.DebugSnapshot().Continuation
	if snap.LateCompletions != 1 {
		t.Fatalf("LateCompletions = %d, want 1", snap.LateCompletions)
	}

	out, awaitErr := future.Await(ctx)
	if awaitErr == nil {
		t.Fatalf("expected error; got output %v", out)
	}
	if out.Sum == 99 {
		t.Fatal("late Complete after deadline must not change future result")
	}
}

func TestPipelineContinuationDebugSnapshotReflectsPending(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 10)

	yielded := make(chan struct{})
	var completer ContinuationCompleter[pState]

	_, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
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

	// Poll until the registry reflects pending=1.
	waitUntil(t, func() bool {
		snap := q.DebugSnapshot()
		return snap.Continuation.Pending == 1
	}, 2*time.Second)

	snap := q.DebugSnapshot()
	if snap.Continuation.Pending != 1 {
		t.Fatalf("pending = %d, want 1", snap.Continuation.Pending)
	}
	if snap.Continuation.MaxPending != 10 {
		t.Fatalf("max pending = %d, want 10", snap.Continuation.MaxPending)
	}

	// Resolve and verify pending drops to 0.
	completer.Complete(pState{})
	waitUntil(t, func() bool {
		return q.DebugSnapshot().Continuation.Pending == 0
	}, 2*time.Second)

	snap = q.DebugSnapshot()
	if snap.Continuation.Pending != 0 {
		t.Fatalf("after resolve: pending = %d, want 0", snap.Continuation.Pending)
	}
	if snap.Continuation.Completed != 1 {
		t.Fatalf("completed = %d, want 1", snap.Continuation.Completed)
	}
}
