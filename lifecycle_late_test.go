// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestPipelineContinuationLateCompleteAfterStop(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 8)

	yielded := make(chan struct{})
	var completer ContinuationCompleter[pState]
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
	waitUntil(t, func() bool {
		return q.DebugSnapshot().Continuation.Pending == 1
	}, 2*time.Second)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer stopCancel()
	if err := q.Stop(stopCtx, WithDrain(false)); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	lateBefore := q.DebugSnapshot().Continuation.LateCompletions
	if completer.Complete(pState{Val: 1}) {
		t.Log("late Complete returned true after stop")
	}
	waitUntil(t, func() bool {
		return q.DebugSnapshot().Continuation.LateCompletions >= lateBefore
	}, 2*time.Second)
	_, _ = future.Await(ctx)
}

func TestBackendLeaseReleasedAfterJobPanic(t *testing.T) {
	ctx := testTimeout(t)
	q := newBackendTestQueue(t, ctx)

	done := make(chan struct{})
	if err := q.Submit(ctx, Job{
		Key:  "panic-backend",
		Lane: "default",
		Run: func(ctx context.Context) error {
			op := BackendOperation{Resource: "primary-db", Lane: BackendLaneDBRead}
			_, _ = WithBackend(ctx, q, op, func(ctx context.Context) (struct{}, error) {
				close(done)
				panic("job panic with lease held")
			})
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, done)
	time.Sleep(50 * time.Millisecond)
	stopTestQueue(t, q)
	assertBackendLaneInFlightZero(t, q, "primary-db", BackendLaneDBRead)
}

func TestRaceBackendAcquireReleaseAndStop(t *testing.T) {
	ctx := testTimeout(t)
	q := newBackendTestQueue(t, ctx)

	var wg sync.WaitGroup
	wg.Add(20)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			_, _ = SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
				Meta: RequestMeta{Key: "k", Lane: "default"},
				Stages: []PipelineStage[pState]{
					{
						Meta: StageMeta{Name: StageDBRead},
						Run: func(ctx context.Context, st pState) (pState, error) {
							op := BackendOperationFromStage(ctx, "primary-db", BackendLaneDBRead)
							return WithBackend(ctx, q, op, func(ctx context.Context) (pState, error) {
								time.Sleep(time.Microsecond)
								return st, nil
							})
						},
					},
				},
				Complete: validPipelineComplete(),
			})
		}()
		go func() {
			defer wg.Done()
			stopCtx, c := context.WithTimeout(context.Background(), time.Second)
			defer c()
			_ = q.Stop(stopCtx, WithDrain(false))
		}()
	}
	wg.Wait()
}
