// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"testing"
	"time"
)

type pipelineHookSpy struct {
	requestHookSpy
	stageStarted   chan StageObservation
	stageCompleted chan StageObservation
	stageFailed    chan StageObservation
}

func newPipelineHookSpy() *pipelineHookSpy {
	return &pipelineHookSpy{
		requestHookSpy: *newRequestHookSpy(),
		stageStarted:   make(chan StageObservation, 16),
		stageCompleted: make(chan StageObservation, 16),
		stageFailed:    make(chan StageObservation, 16),
	}
}

func (s *pipelineHookSpy) hooks() RequestHooks {
	h := s.requestHookSpy.hooks()
	h.OnStageStarted = func(o StageObservation) { s.stageStarted <- o }
	h.OnStageCompleted = func(o StageObservation) { s.stageCompleted <- o }
	h.OnStageFailed = func(o StageObservation) { s.stageFailed <- o }
	return h
}

func waitStageObservation(t *testing.T, ch <-chan StageObservation) StageObservation {
	t.Helper()
	select {
	case obs := <-ch:
		return obs
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for stage observation")
		return StageObservation{}
	}
}

func TestSubmitPipelineStageHooksFirePerStage(t *testing.T) {
	spy := newPipelineHookSpy()
	q, ctx := startedQueueWithRequestHooks(t, spy.hooks())

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{
			RequestID: "req-pipe-1",
			Key:       "k",
			Lane:      "default",
			Transport: "http",
			Operation: "get-customer",
		},
		Stages: []PipelineStage[pState]{
			validPipelineStage(StageValidate, func(s *pState) {}),
			validPipelineStage(StageDBRead, func(s *pState) { s.Val = 1 }),
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := future.Await(context.Background()); err != nil {
		t.Fatal(err)
	}

	<-spy.queued
	waitRequestObservation(t, spy.started)
	waitRequestObservation(t, spy.completed)

	started1 := waitStageObservation(t, spy.stageStarted)
	completed1 := waitStageObservation(t, spy.stageCompleted)
	started2 := waitStageObservation(t, spy.stageStarted)
	completed2 := waitStageObservation(t, spy.stageCompleted)

	if started1.Stage != StageValidate || completed1.Stage != StageValidate {
		t.Fatalf("stage1: started=%q completed=%q", started1.Stage, completed1.Stage)
	}
	if started2.Stage != StageDBRead || completed2.Stage != StageDBRead {
		t.Fatalf("stage2: started=%q completed=%q", started2.Stage, completed2.Stage)
	}
	if started1.Operation != "get-customer" {
		t.Errorf("operation = %q", started1.Operation)
	}
	if completed1.Outcome != RequestOutcomeCompleted {
		t.Errorf("outcome = %q", completed1.Outcome)
	}
	if completed1.FailureKind != FailureNone {
		t.Errorf("failure kind = %q", completed1.FailureKind)
	}
}

func TestSubmitPipelineStageFailedHook(t *testing.T) {
	spy := newPipelineHookSpy()
	q, ctx := startedQueueWithRequestHooks(t, spy.hooks())

	future, _ := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default", Operation: "op"},
		Stages: []PipelineStage[pState]{
			permanentFailStage(StageExternalAPI, errors.New("upstream")),
		},
		Complete: validPipelineComplete(),
	})
	_, awaitErr := future.Await(context.Background())
	if awaitErr == nil {
		t.Fatal("expected error")
	}

	waitStageObservation(t, spy.stageStarted)
	failed := waitStageObservation(t, spy.stageFailed)

	if failed.Stage != StageExternalAPI {
		t.Errorf("stage = %q", failed.Stage)
	}
	if failed.Outcome != RequestOutcomeFailed {
		t.Errorf("outcome = %q", failed.Outcome)
	}
	if failed.FailureKind != FailurePermanent {
		t.Errorf("failure kind = %q", failed.FailureKind)
	}
}

func TestSubmitPipelineRequestHooksOncePerPipeline(t *testing.T) {
	spy := newPipelineHookSpy()
	q, ctx := startedQueueWithRequestHooks(t, spy.hooks())

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			validPipelineStage(StageValidate, func(s *pState) {}),
			validPipelineStage(StageBusiness, func(s *pState) { s.Val = 2 }),
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := future.Await(context.Background()); err != nil {
		t.Fatal(err)
	}

	<-spy.queued
	waitRequestObservation(t, spy.started)
	waitRequestObservation(t, spy.completed)

	waitStageObservation(t, spy.stageStarted)
	waitStageObservation(t, spy.stageCompleted)
	waitStageObservation(t, spy.stageStarted)
	waitStageObservation(t, spy.stageCompleted)

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
}

func TestStageObservationLowCardinalityStageName(t *testing.T) {
	obs := newStageObservation(
		RequestMeta{Key: "tenant-secret", Operation: "get-customer"},
		StageMeta{Name: StageDBRead},
		0, time.Millisecond, time.Second, time.Second, nil, FailurePolicy{},
	)
	if obs.Stage != StageDBRead {
		t.Fatalf("stage = %q", obs.Stage)
	}
	if obs.Key != "tenant-secret" {
		t.Fatal("key present for debugging but must not be used as metric label")
	}
}
