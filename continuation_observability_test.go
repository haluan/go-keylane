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

type continuationHookSpy struct {
	mu     sync.Mutex
	events []string
}

func (s *continuationHookSpy) record(name string) {
	s.mu.Lock()
	s.events = append(s.events, name)
	s.mu.Unlock()
}

func (s *continuationHookSpy) hooks() ContinuationHooks {
	return ContinuationHooks{
		OnContinuationYielded:   func(ContinuationObservation) { s.record("yielded") },
		OnContinuationResumed:   func(ContinuationObservation) { s.record("resumed") },
		OnContinuationCompleted: func(ContinuationObservation) { s.record("completed") },
		OnContinuationFailed:    func(ContinuationObservation) { s.record("failed") },
		OnContinuationCancelled: func(ContinuationObservation) { s.record("cancelled") },
		OnContinuationLate:      func(ContinuationObservation) { s.record("late") },
	}
}

func (s *continuationHookSpy) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.events))
	copy(out, s.events)
	return out
}

func startedQueueWithContinuationHooks(t *testing.T, cont ContinuationHooks) (*Queue, context.Context) {
	t.Helper()
	ctx := testTimeout(t)
	cfg := newTestConfig()
	cfg.Continuation = ContinuationConfig{Enabled: true, MaxPending: 64}
	cfg.Observability = DefaultObservabilityConfig()
	cfg.Observability.Hooks.Request.Continuation = cont
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopTestQueue(t, q) })
	return q, ctx
}

func TestContinuationHooksYieldResumeCompleted(t *testing.T) {
	spy := &continuationHookSpy{}
	q, ctx := startedQueueWithContinuationHooks(t, spy.hooks())

	ready := make(chan ContinuationCompleter[pState], 1)
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](context.Background())
					ready <- c
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	(<-ready).Complete(pState{Val: 3})
	if _, err := future.Await(ctx); err != nil {
		t.Fatal(err)
	}
	got := spy.snapshot()
	want := []string{"yielded", "completed", "resumed"}
	if len(got) < len(want) {
		t.Fatalf("events = %v", got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("events = %v, want prefix %v", got, want)
		}
	}
}

func TestContinuationHooksFailPath(t *testing.T) {
	spy := &continuationHookSpy{}
	q, ctx := startedQueueWithContinuationHooks(t, spy.hooks())

	ready := make(chan ContinuationCompleter[pState], 1)
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](context.Background())
					ready <- c
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	(<-ready).Fail(PermanentFailure(errors.New("async fail")))
	if _, err := future.Await(ctx); err == nil {
		t.Fatal("expected error")
	}
	got := spy.snapshot()
	if len(got) < 2 || got[0] != "yielded" || got[1] != "failed" {
		t.Fatalf("events = %v", got)
	}
}

func TestContinuationHooksCancelledPath(t *testing.T) {
	spy := &continuationHookSpy{}
	q, ctx := startedQueueWithContinuationHooks(t, spy.hooks())

	reqCtx, cancel := context.WithCancel(ctx)
	ready := make(chan struct{})

	future, err := SubmitPipeline(reqCtx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](context.Background())
					close(ready)
					_ = c
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
	if _, err := future.Await(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("await err = %v", err)
	}
	waitUntil(t, func() bool {
		got := spy.snapshot()
		return len(got) >= 2 && got[0] == "yielded" && got[1] == "cancelled"
	}, 5*time.Second)
	got := spy.snapshot()
	if len(got) < 2 || got[0] != "yielded" || got[1] != "cancelled" {
		t.Fatalf("events = %v", got)
	}
}

func TestContinuationHooksLateCompletion(t *testing.T) {
	spy := &continuationHookSpy{}
	q, ctx := startedQueueWithContinuationHooks(t, spy.hooks())

	ready := make(chan ContinuationCompleter[pState], 1)
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](context.Background())
					ready <- c
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	c := <-ready
	c.Complete(pState{Val: 1})
	if _, err := future.Await(ctx); err != nil {
		t.Fatal(err)
	}
	_ = c.Complete(pState{Val: 99})
	got := spy.snapshot()
	lateCount := 0
	for _, e := range got {
		if e == "late" {
			lateCount++
		}
	}
	if lateCount != 1 {
		t.Fatalf("events = %v, late count = %d", got, lateCount)
	}
	if snap := q.DebugSnapshot().Continuation; snap.LateCompletions != 1 {
		t.Fatalf("LateCompletions = %d", snap.LateCompletions)
	}
}

func TestContinuationHooksDisabled(t *testing.T) {
	ctx := testTimeout(t)
	cfg := newTestConfig()
	cfg.Continuation = ContinuationConfig{Enabled: true, MaxPending: 10}
	cfg.Observability = DefaultObservabilityConfig()
	cfg.Observability.EnableHooks = false
	spy := &continuationHookSpy{}
	cfg.Observability.Hooks.Request.Continuation = spy.hooks()
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}

	ready := make(chan ContinuationCompleter[pState], 1)
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](context.Background())
					ready <- c
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	(<-ready).Complete(pState{Val: 1})
	if _, err := future.Await(ctx); err != nil {
		t.Fatal(err)
	}
	if len(spy.snapshot()) != 0 {
		t.Fatalf("events = %v", spy.snapshot())
	}
}
