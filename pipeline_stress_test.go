// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestStressContinuationCancelNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 32)

	reqCtx, cancel := context.WithCancel(ctx)
	ready := make(chan struct{})

	future, err := SubmitPipeline(reqCtx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, _ := NewContinuation[pState](context.Background())
					close(ready)
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, ready)
	cancel()
	_, _ = future.Await(ctx)
	stopTestQueue(t, q)
	eventuallyNoGoroutineGrowth(t, before, 12)
}

func TestStressBackendRejectNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	ctx := testTimeout(t)
	q := newBackendTestQueue(t, ctx)

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageDBRead},
				Run: func(ctx context.Context, st pState) (pState, error) {
					l1, err := AcquireBackend(ctx, q, BackendOperation{
						Resource: "primary-db", Lane: BackendLaneDBWrite,
					})
					if err != nil {
						return st, err
					}
					defer l1.Release()
					_, err = AcquireBackend(ctx, q, BackendOperation{
						Resource: "primary-db", Lane: BackendLaneDBWrite,
					})
					return st, err
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = future.Await(ctx)
	stopTestQueue(t, q)
	assertBackendLaneInFlightZero(t, q, "primary-db", BackendLaneDBWrite)
	eventuallyNoGoroutineGrowth(t, before, 12)
}

func TestStressBackendInFlightZeroAfterStageFailure(t *testing.T) {
	ctx := testTimeout(t)
	q := newBackendTestQueue(t, ctx)

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageDBRead},
				Run: func(ctx context.Context, st pState) (pState, error) {
					l1, err := AcquireBackend(ctx, q, BackendOperation{
						Resource: "primary-db", Lane: BackendLaneDBWrite,
					})
					if err != nil {
						return st, err
					}
					defer l1.Release()
					_, err = AcquireBackend(ctx, q, BackendOperation{
						Resource: "primary-db", Lane: BackendLaneDBWrite,
					})
					return st, err
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := future.Await(ctx); err == nil {
		t.Fatal("expected stage failure")
	}
	stopTestQueue(t, q)
	assertBackendLaneInFlightZero(t, q, "primary-db", BackendLaneDBWrite)
}

func TestStressBackendInFlightZeroAfterStagePanic(t *testing.T) {
	ctx := testTimeout(t)
	q := newBackendTestQueue(t, ctx)

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageDBRead},
				Run: func(ctx context.Context, st pState) (pState, error) {
					op := BackendOperationFromStage(ctx, "primary-db", BackendLaneDBRead)
					return WithBackend(ctx, q, op, func(context.Context) (pState, error) {
						panic("stage panic with WithBackend")
					})
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := future.Await(ctx); err == nil {
		t.Fatal("expected stage panic failure")
	}
	stopTestQueue(t, q)
	assertBackendLaneInFlightZero(t, q, "primary-db", BackendLaneDBRead)
}

func TestStressContinuationPendingZeroAfterDeadline(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 10)

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
	if _, err := future.Await(ctx); err == nil {
		t.Fatal("expected deadline error")
	}
	waitUntil(t, func() bool {
		return q.DebugSnapshot().Continuation.Pending == 0
	}, 2*time.Second)
	stopTestQueue(t, q)
	if snap := q.DebugSnapshot().Continuation; snap.Pending != 0 {
		t.Fatalf("pending = %d, want 0", snap.Pending)
	}
}

func TestStressContinuationCapRejectsAndDrains(t *testing.T) {
	before := runtime.NumGoroutine()
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 1)

	yielded := make(chan struct{})
	var completer1 ContinuationCompleter[pState]

	f1, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k1", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](context.Background())
					completer1 = c
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
	waitUntil(t, func() bool {
		return q.DebugSnapshot().Continuation.Pending == 1
	}, 2*time.Second)

	f2, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k2", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, _ := NewContinuation[pState](context.Background())
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, awaitErr := f2.Await(ctx)
	if awaitErr == nil {
		t.Fatal("expected continuation limit error on await")
	}
	if !errors.Is(awaitErr, ErrContinuationLimitExceeded) {
		sf, ok := AsStageFailure(awaitErr)
		if !ok || sf.Err == nil || !errors.Is(sf.Err, ErrContinuationLimitExceeded) {
			t.Fatalf("await err = %v", awaitErr)
		}
	}

	completer1.Complete(pState{})
	_, _ = f1.Await(ctx)
	stopTestQueue(t, q)
	if snap := q.continuationReg.snapshot(); snap.Pending != 0 {
		t.Fatalf("pending = %d", snap.Pending)
	}
	eventuallyNoGoroutineGrowth(t, before, 12)
}

func TestStressPipelineTerminalHooksOnce(t *testing.T) {
	spy := newPipelineHookSpy()
	q, ctx := startedQueueWithRequestHooks(t, spy.hooks())

	var completed atomic.Int32
	spyHooks := spy.hooks()
	spyHooks.OnCompleted = func(RequestObservation) { completed.Add(1) }
	q.config.Observability.Hooks.Request = spyHooks

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			validPipelineStage(StageValidate, func(s *pState) { s.Val = 2 }),
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := future.Await(ctx); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return completed.Load() == 1 }, 2*time.Second)
	if completed.Load() != 1 {
		t.Fatalf("OnCompleted count = %d", completed.Load())
	}
	var stageCompleted atomic.Int32
	stageHooks := spy.hooks()
	stageHooks.OnStageCompleted = func(StageObservation) { stageCompleted.Add(1) }
	q.config.Observability.Hooks.Request = stageHooks

	future2, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k2", Lane: "default"},
		Stages: []PipelineStage[pState]{
			validPipelineStage(StageValidate, func(s *pState) {}),
			validPipelineStage(StageBusiness, func(s *pState) {}),
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := future2.Await(ctx); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return stageCompleted.Load() == 2 }, 2*time.Second)
	if stageCompleted.Load() != 2 {
		t.Fatalf("OnStageCompleted count = %d", stageCompleted.Load())
	}
}
