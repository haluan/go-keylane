// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSubmitPipelineRetryUpdatesAttemptInContext(t *testing.T) {
	q, ctx := retryTestQueue(t, RetryPolicy{
		Enabled: true, MaxAttempts: 3, InitialBackoff: 1, Jitter: false, MinRemainingBudget: 0,
	})

	var attemptsSeen []int
	var mu sync.Mutex
	var stageRuns atomic.Int32

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta:        RequestMeta{Key: "k", Lane: "default"},
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: retrySafeIdempotency(),
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageBusiness},
				Run: func(c context.Context, st pState) (pState, error) {
					stageRuns.Add(1)
					exec, ok := StageExecutionFromContext(c)
					if !ok {
						t.Fatal("no execution context")
					}
					mu.Lock()
					attemptsSeen = append(attemptsSeen, exec.Attempt)
					mu.Unlock()
					if exec.Attempt < 2 {
						return st, RetryableFailure(errors.New("transient"))
					}
					st.Val = 5
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

	mu.Lock()
	defer mu.Unlock()
	if len(attemptsSeen) != 2 || attemptsSeen[0] != 1 || attemptsSeen[1] != 2 {
		t.Fatalf("attempts seen = %v", attemptsSeen)
	}
	if stageRuns.Load() != 2 {
		t.Fatalf("stage runs = %d", stageRuns.Load())
	}
}

func TestSubmitRequestRetryRefreshesDeadlineRemaining(t *testing.T) {
	q, ctx := retryTestQueue(t, RetryPolicy{
		Enabled: true, MaxAttempts: 3, InitialBackoff: 5 * time.Millisecond,
		Jitter: false, MinRemainingBudget: 0,
	})
	reqCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	var remainingByAttempt []time.Duration
	var runtimeByAttempt []time.Duration
	var mu sync.Mutex

	future, err := SubmitRequest(reqCtx, q, Request[sumInput, sumOutput]{
		Meta:        RequestMeta{Key: "k", Lane: "default"},
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: retrySafeIdempotency(),
		Input:       sumInput{A: 1, B: 1},
		Handle: func(c context.Context, in sumInput) (sumOutput, error) {
			exec, ok := StageExecutionFromContext(c)
			if !ok {
				t.Fatal("no execution context")
			}
			mu.Lock()
			remainingByAttempt = append(remainingByAttempt, exec.Deadline.Remaining)
			runtimeByAttempt = append(runtimeByAttempt, exec.Deadline.Runtime)
			attempt := exec.Attempt
			mu.Unlock()
			if exec.Deadline.BudgetExhausted {
				return sumOutput{}, context.DeadlineExceeded
			}
			if attempt < 2 {
				time.Sleep(15 * time.Millisecond)
				return sumOutput{}, RetryableFailure(errors.New("transient"))
			}
			return sumOutput{Sum: in.A + in.B}, nil
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
	if len(remainingByAttempt) != 2 {
		t.Fatalf("attempts recorded = %d, remaining = %v", len(remainingByAttempt), remainingByAttempt)
	}
	if remainingByAttempt[1] >= remainingByAttempt[0] {
		t.Fatalf("attempt 2 remaining %v should be less than attempt 1 %v after backoff",
			remainingByAttempt[1], remainingByAttempt[0])
	}
	if len(runtimeByAttempt) != 2 {
		t.Fatalf("runtime attempts recorded = %d, runtime = %v", len(runtimeByAttempt), runtimeByAttempt)
	}
	if runtimeByAttempt[1] < 10*time.Millisecond {
		t.Fatalf("attempt 2 runtime_so_far %v should include attempt 1 handler time (>= 10ms)",
			runtimeByAttempt[1])
	}
	if runtimeByAttempt[1] <= runtimeByAttempt[0] {
		t.Fatalf("attempt 2 runtime %v should exceed attempt 1 start runtime %v",
			runtimeByAttempt[1], runtimeByAttempt[0])
	}
}

func TestSubmitPipelineRetryAccumulatesRuntimeAcrossAttempts(t *testing.T) {
	q, ctx := retryTestQueue(t, RetryPolicy{
		Enabled: true, MaxAttempts: 3, InitialBackoff: 5 * time.Millisecond,
		Jitter: false, MinRemainingBudget: 0,
	})

	var runtimeByAttempt []time.Duration
	var mu sync.Mutex
	var stageRuns atomic.Int32

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta:        RequestMeta{Key: "k", Lane: "default"},
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: retrySafeIdempotency(),
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageBusiness},
				Run: func(c context.Context, st pState) (pState, error) {
					stageRuns.Add(1)
					exec, ok := StageExecutionFromContext(c)
					if !ok {
						t.Fatal("no execution context")
					}
					mu.Lock()
					runtimeByAttempt = append(runtimeByAttempt, exec.Deadline.Runtime)
					attempt := exec.Attempt
					mu.Unlock()
					if attempt < 2 {
						time.Sleep(15 * time.Millisecond)
						return st, RetryableFailure(errors.New("transient"))
					}
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
	if stageRuns.Load() != 2 {
		t.Fatalf("stage runs = %d, want 2", stageRuns.Load())
	}

	mu.Lock()
	defer mu.Unlock()
	if len(runtimeByAttempt) != 2 {
		t.Fatalf("runtime attempts recorded = %d, runtime = %v", len(runtimeByAttempt), runtimeByAttempt)
	}
	if runtimeByAttempt[1] < 10*time.Millisecond {
		t.Fatalf("attempt 2 runtime_so_far %v should include attempt 1 pipeline time (>= 10ms)",
			runtimeByAttempt[1])
	}
	if runtimeByAttempt[1] <= runtimeByAttempt[0] {
		t.Fatalf("attempt 2 runtime %v should exceed attempt 1 start runtime %v",
			runtimeByAttempt[1], runtimeByAttempt[0])
	}
}
