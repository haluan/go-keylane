// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPipelineContinuationBackendLeaseHandoff(t *testing.T) {
	ctx := testTimeout(t)
	cfg := newTestConfig()
	cfg.Continuation = ContinuationConfig{Enabled: true, MaxPending: 10}
	cfg.BackendResources = testBackendResourceConfig()
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}

	yielded := make(chan struct{})
	var completer ContinuationCompleter[pState]

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(stageCtx context.Context, st pState) (StageResult[pState], error) {
					lease, err := AcquireBackend(stageCtx, q, BackendOperationFromStage(stageCtx, "primary-db", BackendLaneDBRead))
					if err != nil {
						return StageResult[pState]{}, err
					}
					cont, c := NewContinuation[pState](stageCtx)
					completer = c
					go func() {
						defer lease.Release()
						time.Sleep(10 * time.Millisecond)
						completer.Complete(pState{Val: 3})
					}()
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
	out, err := future.Await(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if out.Sum != 3 {
		t.Fatalf("sum = %d", out.Sum)
	}
	waitUntil(t, func() bool {
		for _, res := range q.DebugSnapshot().BackendResources {
			if res.Resource != "primary-db" {
				continue
			}
			for _, lane := range res.Lanes {
				if lane.Lane == BackendLaneDBRead && lane.InFlight == 0 {
					return true
				}
			}
		}
		return false
	}, 2*time.Second)
}

func TestPipelineContinuationBackendLeaseReleasedOnFail(t *testing.T) {
	ctx := testTimeout(t)
	cfg := newTestConfig()
	cfg.Continuation = ContinuationConfig{Enabled: true, MaxPending: 10}
	cfg.BackendResources = testBackendResourceConfig()
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}

	yielded := make(chan struct{})
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(stageCtx context.Context, st pState) (StageResult[pState], error) {
					lease, err := AcquireBackend(stageCtx, q, BackendOperationFromStage(stageCtx, "primary-db", BackendLaneDBRead))
					if err != nil {
						return StageResult[pState]{}, err
					}
					cont, c := NewContinuation[pState](stageCtx)
					go func() {
						defer lease.Release()
						c.Fail(errors.New("io failed"))
					}()
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
		t.Fatal("expected pipeline failure")
	}
	waitUntil(t, func() bool {
		for _, res := range q.DebugSnapshot().BackendResources {
			for _, lane := range res.Lanes {
				if lane.Lane == BackendLaneDBRead && lane.InFlight == 0 {
					return true
				}
			}
		}
		return false
	}, 2*time.Second)
}

func TestPipelineContinuationBackendLeaseReleasedOnCancel(t *testing.T) {
	ctx := testTimeout(t)
	cfg := newTestConfig()
	cfg.Continuation = ContinuationConfig{Enabled: true, MaxPending: 10}
	cfg.BackendResources = testBackendResourceConfig()
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}

	reqCtx, reqCancel := context.WithCancel(context.Background())
	yielded := make(chan struct{})
	var lease BackendLease

	future, err := SubmitPipeline(reqCtx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(stageCtx context.Context, st pState) (StageResult[pState], error) {
					var err error
					lease, err = AcquireBackend(stageCtx, q, BackendOperationFromStage(stageCtx, "primary-db", BackendLaneDBRead))
					if err != nil {
						return StageResult[pState]{}, err
					}
					cont, _ := NewContinuation[pState](stageCtx)
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
	reqCancel()
	_, _ = future.Await(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		lease.Release()
	}()
	wg.Wait()
	waitUntil(t, func() bool {
		for _, res := range q.DebugSnapshot().BackendResources {
			for _, lane := range res.Lanes {
				if lane.Lane == BackendLaneDBRead && lane.InFlight == 0 {
					return true
				}
			}
		}
		return false
	}, 2*time.Second)
}
