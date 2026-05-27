// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestPipelineIntegrationMultiStageBackendSnapshot(t *testing.T) {
	ctx := testTimeout(t)
	cfg := newTestConfig()
	cfg.BackendResources = testBackendResourceConfig()
	cfg.Observability.EnableDebugSnapshot = true
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}

	stageReady := make(chan struct{})
	stageContinue := make(chan struct{})
	inflightDuringHold := make(chan int, 1)

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			validPipelineStage(StageValidate, func(s *pState) {}),
			{
				Meta: StageMeta{Name: StageDBRead},
				Run: func(ctx context.Context, st pState) (pState, error) {
					op := BackendOperationFromStage(ctx, "primary-db", BackendLaneDBRead)
					lease, err := AcquireBackend(ctx, q, op)
					if err != nil {
						return st, err
					}
					var inflight int
					for _, res := range q.DebugSnapshot().BackendResources {
						for _, lane := range res.Lanes {
							if lane.Lane == BackendLaneDBRead {
								inflight = lane.InFlight
							}
						}
					}
					inflightDuringHold <- inflight
					close(stageReady)
					<-stageContinue
					st.Val = 5
					lease.Release()
					return st, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}

	<-stageReady
	if inflight := <-inflightDuringHold; inflight != 1 {
		t.Fatalf("expected inflight=1 during hold, got %d", inflight)
	}
	close(stageContinue)
	out, err := future.Await(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if out.Sum != 5 {
		t.Fatalf("sum = %d", out.Sum)
	}
}

func TestPipelineIntegrationBackendPressureInSnapshot(t *testing.T) {
	ctx := testTimeout(t)
	cfg := newTestConfig()
	cfg.Observability.EnableDebugSnapshot = true
	cfg.BackendResources.PressureProviders = []BackendPressureProvider{
		staticPressureProvider{snap: BackendPressureSnapshot{
			Resource: "primary-db", Lane: BackendLaneDBRead,
			InUse: 2, Capacity: 8, Pressure: 0.25,
		}},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			validPipelineStage(StageValidate, func(s *pState) {}),
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := future.Await(ctx); err != nil {
		t.Fatal(err)
	}
	snap := q.DebugSnapshot()
	if len(snap.BackendPressure) != 1 || snap.BackendPressure[0].InUse != 2 {
		t.Fatalf("BackendPressure = %+v", snap.BackendPressure)
	}
}

func TestPipelineIntegrationBackendSaturatedStageFails(t *testing.T) {
	ctx := testTimeout(t)
	q := newBackendTestQueue(t, ctx)
	spy := newPipelineHookSpy()
	q.config.Observability.Hooks.Request = spy.hooks()
	q.config.Observability.EnableHooks = true

	var saturated atomic.Int32
	q.config.Observability.Hooks.Backend.OnBackendAdmission = func(dec BackendAdmissionDecision) {
		if dec.Reason == BackendAdmissionSaturated {
			saturated.Add(1)
		}
	}

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
	_, awaitErr := future.Await(ctx)
	if awaitErr == nil {
		t.Fatal("expected error")
	}
	if saturated.Load() == 0 {
		t.Fatal("expected saturated admission event")
	}
	waitStageObservation(t, spy.stageStarted)
	waitStageObservation(t, spy.stageFailed)
}

func TestPipelineIntegrationRetryIncrementsAttemptInStageContext(t *testing.T) {
	q, ctx := retryTestQueue(t, RetryPolicy{
		Enabled: true, MaxAttempts: 2, InitialBackoff: 1, Jitter: false, MinRemainingBudget: 0,
	})
	var attempts []int
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta:        RequestMeta{Key: "k", Lane: "default"},
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: retrySafeIdempotency(),
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageBusiness},
				Run: func(ctx context.Context, st pState) (pState, error) {
					if exec, ok := StageExecutionFromContext(ctx); ok {
						attempts = append(attempts, exec.Attempt)
					}
					if len(attempts) == 1 {
						return st, RetryableFailure(errors.New("transient"))
					}
					st.Val = 9
					return st, nil
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
	if len(attempts) != 2 || attempts[0] != 1 || attempts[1] != 2 {
		t.Fatalf("attempts = %v", attempts)
	}
}
