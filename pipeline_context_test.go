// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestSubmitPipelineStagesReceiveExecutionContext(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	var stages []StageName
	var mu sync.Mutex

	recordStage := func(name StageName) func(context.Context, pState) (pState, error) {
		return func(c context.Context, st pState) (pState, error) {
			exec, ok := StageExecutionFromContext(c)
			if !ok {
				t.Errorf("stage %s: no execution context", name)
				return st, nil
			}
			mu.Lock()
			stages = append(stages, exec.Stage.Name)
			mu.Unlock()

			if exec.RequestID != "req-ctx-1" {
				t.Errorf("request id = %q", exec.RequestID)
			}
			if exec.Key != "tenant-1" || exec.Lane != "default" {
				t.Errorf("routing: key=%q lane=%q", exec.Key, exec.Lane)
			}
			if exec.Transport != "http" || exec.Operation != "get-item" {
				t.Errorf("transport/op = %s/%s", exec.Transport, exec.Operation)
			}
			if exec.ShardID != q.ShardIDForKey("tenant-1") {
				t.Errorf("shard = %d", exec.ShardID)
			}
			if exec.StageCount != 2 {
				t.Errorf("stage count = %d", exec.StageCount)
			}
			return st, nil
		}
	}

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{
			RequestID: "req-ctx-1",
			Key:       "tenant-1",
			Lane:      "default",
			Transport: "http",
			Operation: "get-item",
		},
		Stages: []PipelineStage[pState]{
			{Meta: StageMeta{Name: StageValidate}, Run: recordStage(StageValidate)},
			{Meta: StageMeta{Name: StageDBRead}, Run: recordStage(StageDBRead)},
		},
		Complete: func(c context.Context, st pState) (pOutput, error) {
			exec, ok := StageExecutionFromContext(c)
			if !ok {
				t.Fatal("complete: no execution context")
			}
			if exec.Stage.Name != StageResponse {
				t.Fatalf("complete stage = %q", exec.Stage.Name)
			}
			if exec.StageIndex != 2 {
				t.Fatalf("complete index = %d", exec.StageIndex)
			}
			return pOutput{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := future.Await(context.Background()); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(stages) != 2 || stages[0] != StageValidate || stages[1] != StageDBRead {
		t.Fatalf("stages = %v", stages)
	}
}

func TestSubmitRequestImplicitBusinessStageContext(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	future, err := SubmitRequest(ctx, q, Request[sumInput, sumOutput]{
		Meta:  RequestMeta{Key: "k", Lane: "default", Operation: "sum"},
		Input: sumInput{A: 1, B: 2},
		Handle: func(c context.Context, in sumInput) (sumOutput, error) {
			exec, ok := StageExecutionFromContext(c)
			if !ok {
				t.Fatal("expected execution context on SubmitRequest")
			}
			if exec.Stage.Name != StageBusiness {
				t.Fatalf("stage = %q", exec.Stage.Name)
			}
			if exec.StageIndex != 0 || exec.StageCount != 1 || exec.Attempt != 1 {
				t.Fatalf("stage index/count/attempt = %d/%d/%d", exec.StageIndex, exec.StageCount, exec.Attempt)
			}
			return sumOutput{Sum: in.A + in.B}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	out, awaitErr := future.Await(context.Background())
	if awaitErr != nil {
		t.Fatal(awaitErr)
	}
	if out.Sum != 3 {
		t.Fatalf("sum = %d", out.Sum)
	}
}

func TestSubmitPipelinePropagatesQueueWaitInExecutionContext(t *testing.T) {
	q, ctx := retryTestQueue(t, RetryPolicy{Enabled: false})

	blockA := make(chan struct{})
	startedA := make(chan struct{})
	_, _ = SubmitValue(ctx, q, ValueJob[struct{}]{
		Key: "queued-pipeline", Lane: "default",
		Run: func(context.Context) (struct{}, error) {
			close(startedA)
			<-blockA
			return struct{}{}, nil
		},
	})
	<-startedA
	time.Sleep(25 * time.Millisecond)

	var firstQueueWait time.Duration
	var firstDeadlineQueueWait time.Duration
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "queued-pipeline", Lane: "default"}, // same key as blocker
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				Run: func(c context.Context, st pState) (pState, error) {
					exec, ok := StageExecutionFromContext(c)
					if !ok {
						t.Fatal("no execution context")
					}
					firstQueueWait = exec.QueueWait
					firstDeadlineQueueWait = exec.Deadline.QueueWait
					return st, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // pipeline stays queued behind blocker
	close(blockA)
	if _, err := future.Await(context.Background()); err != nil {
		t.Fatal(err)
	}
	if firstQueueWait < 10*time.Millisecond {
		t.Fatalf("exec.QueueWait = %v, want >= 10ms", firstQueueWait)
	}
	if firstDeadlineQueueWait < 10*time.Millisecond {
		t.Fatalf("exec.Deadline.QueueWait = %v, want >= 10ms", firstDeadlineQueueWait)
	}
}

func TestSubmitPipelineContextCancelledDuringStagePreservesMetadata(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	reqCtx, cancel := context.WithCancel(ctx)

	future, err := SubmitPipeline(reqCtx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				Run: func(c context.Context, st pState) (pState, error) {
					cancel()
					exec, ok := StageExecutionFromContext(c)
					if !ok || exec.Stage.Name != StageValidate {
						t.Fatalf("exec during cancel = %+v ok=%v", exec, ok)
					}
					return st, context.Canceled
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, awaitErr := future.Await(context.Background())
	if awaitErr == nil {
		t.Fatal("expected error")
	}
}
