// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPipelineContinuationNoGoroutineLeak(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 50)

	before := runtime.NumGoroutine()

	const N = 20
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
				Meta: RequestMeta{Key: "k", Lane: "default"},
				Stages: []PipelineStage[pState]{
					{
						Meta: StageMeta{Name: StageValidate},
						RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
							cont, c := NewContinuation[pState](context.Background())
							go func() { c.Complete(st) }()
							return StageResult[pState]{Continuation: cont}, nil
						},
					},
				},
				Complete: validPipelineComplete(),
			})
			if err != nil {
				return
			}
			future.Await(context.Background()) //nolint:errcheck
		}()
	}
	wg.Wait()

	eventuallyNoGoroutineGrowth(t, before, 5)
}

func TestPipelineContinuationCompletionStormRace(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 100)

	const N = 30
	futures := make([]Future[pOutput], N)
	for i := 0; i < N; i++ {
		f, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
			Meta: RequestMeta{Key: "k", Lane: "default"},
			Stages: []PipelineStage[pState]{
				{
					Meta: StageMeta{Name: StageValidate},
					RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
						cont, c := NewContinuation[pState](context.Background())
						go func() { c.Complete(pState{Val: 1}) }()
						return StageResult[pState]{Continuation: cont}, nil
					},
				},
			},
			Complete: validPipelineComplete(),
		})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		futures[i] = f
	}

	for i, f := range futures {
		out, err := f.Await(context.Background())
		if err != nil {
			t.Errorf("future %d: %v", i, err)
			continue
		}
		if out.Sum != 1 {
			t.Errorf("future %d: output = %d, want 1", i, out.Sum)
		}
	}
}

func TestPipelineContinuationCancelDoesNotBlockCompleter(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 50)

	const N = 30
	done := make(chan struct{}, N)
	for i := 0; i < N; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			reqCtx, reqCancel := context.WithCancel(context.Background())
			defer reqCancel()
			yielded := make(chan struct{})
			var completer ContinuationCompleter[pState]
			future, err := SubmitPipeline(reqCtx, q, Pipeline[pState, pOutput]{
				Meta: RequestMeta{Key: "block-k", Lane: "default"},
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
				},
				Complete: validPipelineComplete(),
			})
			if err != nil {
				return
			}
			<-yielded
			deadline := time.After(2 * time.Second)
			for q.DebugSnapshot().Continuation.Pending == 0 {
				select {
				case <-deadline:
					t.Error("continuation not registered")
					return
				case <-time.After(5 * time.Millisecond):
				}
			}
			reqCancel()
			c := completer
			if c.Complete(pState{Val: 1}) {
				t.Error("Complete after request cancel must return false")
			}
			_, awaitErr := future.Await(context.Background())
			if awaitErr != nil && !errors.Is(awaitErr, context.Canceled) {
				t.Errorf("await: %v", awaitErr)
			}
		}()
	}

	timeout := time.After(5 * time.Second)
	for i := 0; i < N; i++ {
		select {
		case <-done:
		case <-timeout:
			t.Fatal("goroutine blocked: cancel + Complete must not deadlock")
		}
	}
}

func TestPipelineContinuationCancelCompleteRace(t *testing.T) {
	ctx := testTimeout(t)

	// DESIGN: when cancellation wins the continuation resolution race, Complete is rejected
	// and counted as late. A canceled future alone is insufficient: Complete may resolve first
	// and resume may observe reqCtx cancellation later (no late completion).
	assertCancelWinsContinuationRace := func(t *testing.T, q *Queue, reqCtx context.Context, reqCancel context.CancelFunc, completer ContinuationCompleter[pState], future Future[pOutput], lateBefore uint64) {
		t.Helper()
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
			t.Fatal("Complete after cancel must return false when cancellation won resolution")
		}
		waitUntil(t, func() bool {
			return q.DebugSnapshot().Continuation.LateCompletions > lateBefore
		}, 2*time.Second)
		_, awaitErr := future.Await(ctx)
		if !errors.Is(awaitErr, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", awaitErr)
		}
	}

	// Deterministic resolution-race path.
	{
		q := newContinuationTestQueue(t, ctx, 4)
		reqCtx, reqCancel := context.WithCancel(context.Background())
		defer reqCancel()
		yielded := make(chan struct{})
		var completer ContinuationCompleter[pState]
		future, err := SubmitPipeline(reqCtx, q, pipelineContinuationYieldOnly(&completer, yielded))
		if err != nil {
			t.Fatal(err)
		}
		<-yielded
		waitUntil(t, func() bool { return q.DebugSnapshot().Continuation.Pending > 0 }, 2*time.Second)
		lateBefore := q.DebugSnapshot().Continuation.LateCompletions
		assertCancelWinsContinuationRace(t, q, reqCtx, reqCancel, completer, future, lateBefore)
	}

	// Concurrent stress: no hang; require late only when Complete was rejected (false), not when
	// the request ends canceled after a successful continuation resolution and resume.
	const N = 30
	var wg sync.WaitGroup
	var canceledRequests int32

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			q := newContinuationTestQueue(t, ctx, 4)
			reqCtx, reqCancel := context.WithCancel(context.Background())
			defer reqCancel()
			yielded := make(chan struct{})
			var completer ContinuationCompleter[pState]

			future, err := SubmitPipeline(reqCtx, q, pipelineContinuationYieldOnly(&completer, yielded))
			if err != nil {
				t.Errorf("submit: %v", err)
				return
			}

			<-yielded
			waitUntil(t, func() bool { return q.DebugSnapshot().Continuation.Pending > 0 }, 2*time.Second)

			lateBefore := q.DebugSnapshot().Continuation.LateCompletions
			var completeOK atomic.Bool
			var raceWG sync.WaitGroup
			raceWG.Add(2)
			go func() {
				defer raceWG.Done()
				completeOK.Store(completer.Complete(pState{Val: 1}))
			}()
			go func() {
				defer raceWG.Done()
				reqCancel()
			}()
			raceWG.Wait()

			_, awaitErr := future.Await(context.Background())
			if errors.Is(awaitErr, context.Canceled) {
				atomic.AddInt32(&canceledRequests, 1)
			}
			if !completeOK.Load() && q.DebugSnapshot().Continuation.LateCompletions <= lateBefore {
				t.Errorf("Complete rejected but LateCompletions stayed %d", lateBefore)
			}
		}()
	}
	wg.Wait()

	if canceledRequests == 0 {
		t.Fatal("expected at least one concurrent iteration to end with request cancellation")
	}
}

func pipelineContinuationYieldOnly(completer *ContinuationCompleter[pState], yielded chan struct{}) Pipeline[pState, pOutput] {
	return Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "race-k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(stageCtx context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](stageCtx)
					*completer = c
					close(yielded)
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	}
}

func TestPipelineContinuationResumeQueueRejectRace(t *testing.T) {
	ctx := testTimeout(t)

	// Use a very small queue to force resume enqueue rejections under load.
	cfg := newTestConfig()
	cfg.Continuation = ContinuationConfig{Enabled: true, MaxPending: 50}
	cfg.QueueSizePerLane = 2
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}

	const N = 20
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			future, submitErr := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
				Meta: RequestMeta{Key: "k", Lane: "default"},
				Stages: []PipelineStage[pState]{
					{
						Meta: StageMeta{Name: StageValidate},
						RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
							cont, c := NewContinuation[pState](context.Background())
							go func() { c.Complete(st) }()
							return StageResult[pState]{Continuation: cont}, nil
						},
					},
				},
				Complete: validPipelineComplete(),
			})
			if submitErr != nil {
				return
			}
			// Result may succeed or fail with resume-rejected; both are acceptable.
			_, _ = future.Await(context.Background())
		}()
	}

	wg.Wait()
	// No panics or deadlocks; test passes.
}
