// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

type pState struct {
	Log []StageName
	Val int
}

type pOutput struct {
	Sum int
}

func validPipelineComplete() func(context.Context, pState) (pOutput, error) {
	return func(_ context.Context, s pState) (pOutput, error) {
		return pOutput{Sum: s.Val}, nil
	}
}

func cancelAfterFirstStage(cancel context.CancelFunc, done *atomic.Bool) PipelineStage[pState] {
	return PipelineStage[pState]{
		Meta: StageMeta{Name: StageValidate},
		Run: func(_ context.Context, st pState) (pState, error) {
			cancel()
			done.Store(true)
			return st, nil
		},
	}
}

func noopStageThatMustNotRun(t *testing.T, name StageName) PipelineStage[pState] {
	t.Helper()
	return PipelineStage[pState]{
		Meta: StageMeta{Name: name},
		Run: func(_ context.Context, st pState) (pState, error) {
			t.Error("stage should not run")
			return st, nil
		},
	}
}

func permanentFailStage(name StageName, err error) PipelineStage[pState] {
	return PipelineStage[pState]{
		Meta: StageMeta{Name: name},
		Run: func(_ context.Context, _ pState) (pState, error) {
			return pState{}, PermanentFailure(err)
		},
	}
}

func validPipelineStage(name StageName, fn func(*pState)) PipelineStage[pState] {
	return PipelineStage[pState]{
		Meta: StageMeta{Name: name},
		Run: func(_ context.Context, st pState) (pState, error) {
			fn(&st)
			return st, nil
		},
	}
}

func TestSubmitPipelineNilQueue(t *testing.T) {
	f, err := SubmitPipeline(context.Background(), nil, Pipeline[pState, pOutput]{})
	if f == nil {
		t.Fatal("future should not be nil")
	}
	if !errors.Is(err, ErrNilQueue) {
		t.Errorf("got %v, want %v", err, ErrNilQueue)
	}
	_, awaitErr := f.Await(context.Background())
	if !errors.Is(awaitErr, ErrNilQueue) {
		t.Errorf("Await got %v", awaitErr)
	}
}

func TestSubmitPipelineValidation(t *testing.T) {
	q, _ := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 1,
		LaneQuotas: map[Lane]int{"test": 1},
	})
	complete := validPipelineComplete()
	run := func(_ context.Context, _ pState) (pState, error) {
		return pState{}, nil
	}

	tests := []struct {
		name string
		p    Pipeline[pState, pOutput]
		want error
	}{
		{
			name: "empty key",
			p: Pipeline[pState, pOutput]{
				Meta: RequestMeta{Key: "", Lane: "test"},
				Stages: []PipelineStage[pState]{
					{Meta: StageMeta{Name: StageValidate}, Run: run},
				},
				Complete: complete,
			},
			want: ErrInvalidKey,
		},
		{
			name: "empty lane",
			p: Pipeline[pState, pOutput]{
				Meta: RequestMeta{Key: "k", Lane: ""},
				Stages: []PipelineStage[pState]{
					{Meta: StageMeta{Name: StageValidate}, Run: run},
				},
				Complete: complete,
			},
			want: ErrInvalidLane,
		},
		{
			name: "empty stages",
			p: Pipeline[pState, pOutput]{
				Meta:     RequestMeta{Key: "k", Lane: "test"},
				Complete: complete,
			},
			want: ErrEmptyPipelineStages,
		},
		{
			name: "nil stage run",
			p: Pipeline[pState, pOutput]{
				Meta: RequestMeta{Key: "k", Lane: "test"},
				Stages: []PipelineStage[pState]{
					{Meta: StageMeta{Name: StageValidate}},
				},
				Complete: complete,
			},
			want: ErrNilJobRun,
		},
		{
			name: "empty stage name",
			p: Pipeline[pState, pOutput]{
				Meta: RequestMeta{Key: "k", Lane: "test"},
				Stages: []PipelineStage[pState]{
					{Meta: StageMeta{Name: ""}, Run: run},
				},
				Complete: complete,
			},
			want: ErrEmptyStageName,
		},
		{
			name: "nil complete",
			p: Pipeline[pState, pOutput]{
				Meta: RequestMeta{Key: "k", Lane: "test"},
				Stages: []PipelineStage[pState]{
					{Meta: StageMeta{Name: StageValidate}, Run: run},
				},
			},
			want: ErrNilPipelineComplete,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := SubmitPipeline(context.Background(), q, tt.p)
			if !errors.Is(err, tt.want) {
				t.Errorf("got %v, want %v", err, tt.want)
			}
		})
	}
}

func TestSubmitPipelineExecutesStagesInOrder(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			validPipelineStage(StageValidate, func(s *pState) { s.Log = append(s.Log, StageValidate) }),
			validPipelineStage(StageDBRead, func(s *pState) { s.Log = append(s.Log, StageDBRead); s.Val = 3 }),
			validPipelineStage(StageResponse, func(s *pState) { s.Log = append(s.Log, StageResponse) }),
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	out, awaitErr := future.Await(context.Background())
	if awaitErr != nil {
		t.Fatal(awaitErr)
	}
	if out.Sum != 3 {
		t.Fatalf("sum = %d, want 3", out.Sum)
	}
}

func TestSubmitPipelineStopsOnFirstFailedStage(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	var ranComplete atomic.Bool
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			validPipelineStage(StageValidate, func(s *pState) { s.Log = append(s.Log, StageValidate) }),
			permanentFailStage(StageDBRead, errors.New("db down")),
			validPipelineStage(StageResponse, func(s *pState) { s.Log = append(s.Log, StageResponse) }),
		},
		Complete: func(ctx context.Context, s pState) (pOutput, error) {
			ranComplete.Store(true)
			return pOutput{Sum: s.Val}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, awaitErr := future.Await(context.Background())
	if awaitErr == nil {
		t.Fatal("expected error")
	}
	if ranComplete.Load() {
		t.Fatal("Complete ran after stage failure")
	}
	sf, ok := AsStageFailure(awaitErr)
	if !ok {
		t.Fatalf("expected StageFailure, got %T", awaitErr)
	}
	if sf.Stage.Name != StageDBRead {
		t.Fatalf("stage = %q, want db_read", sf.Stage.Name)
	}
}

func TestSubmitPipelineFutureCompletesOnce(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			validPipelineStage(StageBusiness, func(s *pState) { s.Val = 1 }),
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, future.Done())
	_, err1 := future.Await(context.Background())
	_, err2 := future.Await(context.Background())
	if err1 != err2 {
		t.Fatalf("await results differ: %v vs %v", err1, err2)
	}
}

func TestSubmitPipelineContextCancelledBeforeStages(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	reqCtx, cancel := context.WithCancel(ctx)
	cancel()

	var stageRuns atomic.Int32
	future, err := SubmitPipeline(reqCtx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			validPipelineStage(StageValidate, func(*pState) { stageRuns.Add(1) }),
		},
		Complete: validPipelineComplete(),
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	_, awaitErr := future.Await(context.Background())
	if awaitErr != nil && !errors.Is(awaitErr, context.Canceled) {
		if fail, ok := FailureFromFuture(future); !ok || fail.Kind != FailureCancelled {
			t.Fatalf("got %v, want canceled", awaitErr)
		}
	}
	if stageRuns.Load() != 0 {
		t.Fatalf("stages ran %d times, want 0", stageRuns.Load())
	}
}

func TestSubmitPipelineContextCancelledBetweenStages(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	reqCtx, cancel := context.WithCancel(ctx)
	var afterFirst atomic.Bool

	future, err := SubmitPipeline(reqCtx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			cancelAfterFirstStage(cancel, &afterFirst),
			noopStageThatMustNotRun(t, StageDBRead),
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, awaitErr := future.Await(context.Background())
	if !errors.Is(awaitErr, context.Canceled) {
		t.Fatalf("got %v, want canceled", awaitErr)
	}
	if !afterFirst.Load() {
		t.Fatal("first stage did not run")
	}
}

func TestValidateStageMeta(t *testing.T) {
	if err := ValidateStageMeta(StageMeta{Name: StageValidate}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateStageMeta(StageMeta{Name: "custom_stage"}); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(ValidateStageMeta(StageMeta{}), ErrEmptyStageName) {
		t.Fatal("expected ErrEmptyStageName")
	}
}
