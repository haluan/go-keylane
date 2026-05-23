// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func testRequestQueue(t *testing.T) (*Queue, context.Context) {
	t.Helper()
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[Lane]int{"default": 1},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := q.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return q, ctx
}

func TestSubmitRequestCancelledBeforeSubmit(t *testing.T) {
	q, _ := testRequestQueue(t)
	var ran atomic.Bool

	reqCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := SubmitRequest(reqCtx, q, Request[sumInput, sumOutput]{
		Meta:  RequestMeta{Key: "k", Lane: "default"},
		Input: sumInput{},
		Handle: func(ctx context.Context, in sumInput) (sumOutput, error) {
			ran.Store(true)
			return sumOutput{}, nil
		},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SubmitRequest err = %v, want Canceled", err)
	}
	if ran.Load() {
		t.Error("handler ran on cancelled submit context")
	}
}

func TestSubmitRequestDeadlineExceededBeforeSubmit(t *testing.T) {
	q, _ := testRequestQueue(t)
	var ran atomic.Bool

	reqCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	_, err := SubmitRequest(reqCtx, q, Request[sumInput, sumOutput]{
		Meta:  RequestMeta{Key: "k", Lane: "default"},
		Input: sumInput{},
		Handle: func(ctx context.Context, in sumInput) (sumOutput, error) {
			ran.Store(true)
			return sumOutput{}, nil
		},
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SubmitRequest err = %v, want DeadlineExceeded", err)
	}
	if ran.Load() {
		t.Error("handler ran with expired deadline before submit")
	}
}

func TestSubmitRequestCancelledWhileQueued(t *testing.T) {
	q, queueCtx := testRequestQueue(t)
	blocker := make(chan struct{})
	var secondRan atomic.Bool

	_ = q.Submit(queueCtx, Job{
		Key:  "blocker-key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			<-blocker
			return nil
		},
	})

	reqCtx, reqCancel := context.WithCancel(context.Background())
	future, err := SubmitRequest(reqCtx, q, Request[sumInput, sumOutput]{
		Meta:  RequestMeta{Key: "req-key", Lane: "default"},
		Input: sumInput{},
		Handle: func(ctx context.Context, in sumInput) (sumOutput, error) {
			secondRan.Store(true)
			return sumOutput{Sum: 1}, nil
		},
	})
	if err != nil {
		t.Fatalf("SubmitRequest: %v", err)
	}

	reqCancel()
	close(blocker)

	_, awaitErr := future.Await(context.Background())
	if !errors.Is(awaitErr, context.Canceled) {
		t.Fatalf("Await err = %v, want Canceled", awaitErr)
	}
	if secondRan.Load() {
		t.Error("handler ran after request context cancelled while queued")
	}
}

func TestSubmitRequestDeadlineExceededWhileQueued(t *testing.T) {
	q, queueCtx := testRequestQueue(t)
	blocker := make(chan struct{})
	var secondRan atomic.Bool

	_ = q.Submit(queueCtx, Job{
		Key:  "blocker-key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			<-blocker
			return nil
		},
	})

	reqCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	future, err := SubmitRequest(reqCtx, q, Request[sumInput, sumOutput]{
		Meta:  RequestMeta{Key: "req-key", Lane: "default"},
		Input: sumInput{},
		Handle: func(ctx context.Context, in sumInput) (sumOutput, error) {
			secondRan.Store(true)
			return sumOutput{}, nil
		},
	})
	if err != nil {
		t.Fatalf("SubmitRequest: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	close(blocker)

	_, awaitErr := future.Await(context.Background())
	if !errors.Is(awaitErr, context.DeadlineExceeded) {
		t.Fatalf("Await err = %v, want DeadlineExceeded", awaitErr)
	}
	if secondRan.Load() {
		t.Error("handler ran after request deadline exceeded while queued")
	}
}

func TestSubmitRequestCancelledWhileRunning(t *testing.T) {
	q, queueCtx := testRequestQueue(t)
	started := make(chan struct{})
	reqCtx, reqCancel := context.WithCancel(context.Background())

	future, err := SubmitRequest(reqCtx, q, Request[sumInput, sumOutput]{
		Meta:  RequestMeta{Key: "k", Lane: "default"},
		Input: sumInput{},
		Handle: func(ctx context.Context, in sumInput) (sumOutput, error) {
			close(started)
			<-ctx.Done()
			return sumOutput{}, ctx.Err()
		},
	})
	if err != nil {
		t.Fatalf("SubmitRequest: %v", err)
	}

	<-started
	reqCancel()

	_, awaitErr := future.Await(context.Background())
	if !errors.Is(awaitErr, context.Canceled) {
		t.Fatalf("Await err = %v, want Canceled", awaitErr)
	}
	_ = queueCtx
}

func TestSubmitRequestHandlerIgnoresCancellation(t *testing.T) {
	q, _ := testRequestQueue(t)
	reqCtx, reqCancel := context.WithCancel(context.Background())
	started := make(chan struct{})

	future, err := SubmitRequest(reqCtx, q, Request[sumInput, sumOutput]{
		Meta:  RequestMeta{Key: "k", Lane: "default"},
		Input: sumInput{A: 3, B: 4},
		Handle: func(ctx context.Context, in sumInput) (sumOutput, error) {
			close(started)
			time.Sleep(30 * time.Millisecond)
			return sumOutput{Sum: in.A + in.B}, nil
		},
	})
	if err != nil {
		t.Fatalf("SubmitRequest: %v", err)
	}

	<-started
	reqCancel()

	out, awaitErr := future.Await(context.Background())
	if awaitErr != nil {
		t.Fatalf("Await err = %v, want nil (handler ignored cancellation)", awaitErr)
	}
	if out.Sum != 7 {
		t.Errorf("Sum = %d, want 7", out.Sum)
	}
}

func TestSubmitRequestAwaitTimeoutDoesNotCancelRequest(t *testing.T) {
	q, _ := testRequestQueue(t)
	blocker := make(chan struct{})

	requestCtx := context.Background()
	future, err := SubmitRequest(requestCtx, q, Request[sumInput, sumOutput]{
		Meta:  RequestMeta{Key: "k", Lane: "default"},
		Input: sumInput{},
		Handle: func(ctx context.Context, in sumInput) (sumOutput, error) {
			select {
			case <-blocker:
				return sumOutput{Sum: 99}, nil
			case <-ctx.Done():
				return sumOutput{}, ctx.Err()
			}
		},
	})
	if err != nil {
		t.Fatalf("SubmitRequest: %v", err)
	}

	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer awaitCancel()

	_, awaitErr := future.Await(awaitCtx)
	if !errors.Is(awaitErr, context.DeadlineExceeded) {
		t.Fatalf("first Await err = %v, want DeadlineExceeded", awaitErr)
	}

	close(blocker)

	out, awaitErr := future.Await(context.Background())
	if awaitErr != nil {
		t.Fatalf("second Await err = %v", awaitErr)
	}
	if out.Sum != 99 {
		t.Errorf("Sum = %d, want 99", out.Sum)
	}
}

func TestSubmitRequestFutureCompletesAfterAwaitAbandoned(t *testing.T) {
	q, _ := testRequestQueue(t)
	blocker := make(chan struct{})

	future, err := SubmitRequest(context.Background(), q, Request[sumInput, sumOutput]{
		Meta:  RequestMeta{Key: "k", Lane: "default"},
		Input: sumInput{},
		Handle: func(ctx context.Context, in sumInput) (sumOutput, error) {
			<-blocker
			return sumOutput{Sum: 11}, nil
		},
	})
	if err != nil {
		t.Fatalf("SubmitRequest: %v", err)
	}

	shortCtx, shortCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer shortCancel()
	_, _ = future.Await(shortCtx)

	close(blocker)

	select {
	case <-future.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done() not closed after handler completed")
	}

	out, awaitErr := future.Await(context.Background())
	if awaitErr != nil || out.Sum != 11 {
		t.Fatalf("final Await: out=%+v err=%v", out, awaitErr)
	}
}

func TestSubmitRequestDoubleCompletionRace(t *testing.T) {
	q, _ := testRequestQueue(t)
	for i := 0; i < 50; i++ {
		reqCtx, reqCancel := context.WithCancel(context.Background())
		future, err := SubmitRequest(reqCtx, q, Request[sumInput, sumOutput]{
			Meta:  RequestMeta{Key: "k", Lane: "default"},
			Input: sumInput{A: 1, B: 1},
			Handle: func(ctx context.Context, in sumInput) (sumOutput, error) {
				select {
				case <-time.After(time.Millisecond):
					return sumOutput{Sum: in.A + in.B}, nil
				case <-ctx.Done():
					return sumOutput{}, ctx.Err()
				}
			},
		})
		if err != nil {
			t.Fatalf("iteration %d SubmitRequest: %v", i, err)
		}

		go func() {
			time.Sleep(time.Duration(i%3) * time.Millisecond)
			reqCancel()
		}()

		_, _ = future.Await(context.Background())
	}
}

func TestSubmitRequestCancelledWhileQueuedNoCompletedCounter(t *testing.T) {
	q, queueCtx := testRequestQueue(t)
	blocker := make(chan struct{})

	_ = q.Submit(queueCtx, Job{
		Key: "blocker-key", Lane: "default",
		Run: func(ctx context.Context) error { <-blocker; return nil },
	})

	reqCtx, reqCancel := context.WithCancel(context.Background())
	future, err := SubmitRequest(reqCtx, q, Request[sumInput, sumOutput]{
		Meta:  RequestMeta{Key: "req-key", Lane: "default"},
		Input: sumInput{},
		Handle: func(ctx context.Context, in sumInput) (sumOutput, error) {
			return sumOutput{Sum: 1}, nil
		},
	})
	if err != nil {
		t.Fatalf("SubmitRequest: %v", err)
	}

	reqCancel()
	close(blocker)
	_, _ = future.Await(context.Background())

	stats := q.Stats()
	ls, ok := laneStatsInStats(stats, "default")
	if !ok {
		t.Fatal("lane stats not found")
	}
	if ls.CompletedTotal > 1 {
		t.Errorf("CompletedTotal = %d, want at most 1 (blocker only)", ls.CompletedTotal)
	}
}
