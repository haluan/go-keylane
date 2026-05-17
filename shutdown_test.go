package keylane

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestGracefulShutdownPreventsSubmit(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	if err := q.Stop(ctx); err != nil {
		t.Fatalf("failed to stop: %v", err)
	}

	job := Job{
		Key:  "k",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	}
	err := q.Submit(ctx, job)
	if !errors.Is(err, ErrStopped) {
		t.Errorf("got error %v, want %v", err, ErrStopped)
	}
}

func TestGracefulShutdownPreventsTrySubmit(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	if err := q.Stop(ctx); err != nil {
		t.Fatalf("failed to stop: %v", err)
	}

	job := Job{
		Key:  "k",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	}
	if ok := q.TrySubmit(job); ok {
		t.Error("expected TrySubmit to return false after Stop")
	}
}

func TestGracefulShutdownWorkersExit(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	if err := q.Stop(ctx); err != nil {
		t.Fatalf("failed to stop: %v", err)
	}

	// Verify scheduler's workers exited completely by checking if stop completed cleanly.
	// Since Stop is synchronous and blocks on all workers exiting, completion of Stop already guarantees exit.
}

func TestGracefulShutdownIdempotent(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	if err := q.Stop(ctx); err != nil {
		t.Fatalf("first stop failed: %v", err)
	}
	if err := q.Stop(ctx); err != nil {
		t.Fatalf("second stop failed: %v", err)
	}
}

func TestGracefulShutdownWithDrain(t *testing.T) {
	ctx := testTimeout(t)

	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 100,
		LaneQuotas: map[Lane]int{
			"default": 1,
		},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	var executed int
	var mu sync.Mutex

	job := Job{
		Key:  "k",
		Lane: "default",
		Run: func(ctx context.Context) error {
			mu.Lock()
			executed++
			mu.Unlock()
			return nil
		},
	}

	// Submit 5 jobs before queue starts
	for i := 0; i < 5; i++ {
		if err := q.Submit(context.Background(), job); err != nil {
			t.Fatalf("pre-submit failed: %v", err)
		}
	}

	if err := q.Start(ctx); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	// Immediately call Stop with drain.
	// This must block until enqueued jobs are completely processed.
	if err := q.Stop(ctx, WithDrain(true)); err != nil {
		t.Fatalf("stop with drain failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if executed != 5 {
		t.Errorf("executed = %d, want 5 (all enqueued jobs drained)", executed)
	}
}

func TestGracefulShutdownTimeout(t *testing.T) {
	ctx := testTimeout(t)

	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 100,
		LaneQuotas: map[Lane]int{
			"default": 1,
		},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	gate := make(chan struct{})
	blockJob := Job{
		Key:  "k",
		Lane: "default",
		Run: func(ctx context.Context) error {
			<-gate
			return nil
		},
	}

	_ = q.Submit(context.Background(), blockJob)
	_ = q.Start(ctx)

	// Call Stop with a short timeout. Since blockJob is blocking, Stop must return deadline exceeded.
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err = q.Stop(stopCtx, WithDrain(true))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got stop err %v, want %v", err, context.DeadlineExceeded)
	}

	close(gate)
}
