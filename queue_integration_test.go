package keylane_test

import (
	"context"
	"testing"
	"time"

	"github.com/haluan/go-keylane"
)

func TestQueue_Integration_Basic(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       8,
		WorkerCount:      2,
		QueueSizePerLane: 10,
		LaneQuotas: map[keylane.Lane]int{
			"default": 1,
		},
	}

	q, err := keylane.New(cfg)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := q.Start(ctx); err != nil {
		t.Fatalf("failed to start queue: %v", err)
	}

	done := make(chan struct{})
	job := keylane.Job{
		Key:  "test-key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			close(done)
			return nil
		},
	}

	if err := q.Submit(ctx, job); err != nil {
		t.Fatalf("failed to submit job: %v", err)
	}

	select {
	case <-done:
		// success
	case <-time.After(time.Second):
		t.Fatal("job timed out")
	}
}

func TestQueue_Integration_MultipleJobs(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       4,
		WorkerCount:      1,
		QueueSizePerLane: 100,
		LaneQuotas: map[keylane.Lane]int{
			"default": 2,
		},
	}

	q, err := keylane.New(cfg)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := q.Start(ctx); err != nil {
		t.Fatalf("failed to start queue: %v", err)
	}

	const jobCount = 10
	results := make(chan int, jobCount)
	for i := 0; i < jobCount; i++ {
		val := i
		err := q.Submit(ctx, keylane.Job{
			Key:  "k", // same key -> same shard
			Lane: "default",
			Run: func(ctx context.Context) error {
				results <- val
				return nil
			},
		})
		if err != nil {
			t.Fatalf("submit failed: %v", err)
		}
	}

	for i := 0; i < jobCount; i++ {
		select {
		case <-results:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for job %d", i)
		}
	}
}

func TestQueue_Integration_SubmitBeforeStart(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       4,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas: map[keylane.Lane]int{
			"default": 1,
		},
	}

	q, err := keylane.New(cfg)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	done := make(chan struct{})
	job := keylane.Job{
		Key:  "test-key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			close(done)
			return nil
		},
	}

	// Submit before Start
	if err := q.Submit(context.Background(), job); err != nil {
		t.Fatalf("failed to submit job before Start: %v", err)
	}

	// Should not run yet
	select {
	case <-done:
		t.Fatal("job executed before queue was started")
	case <-time.After(100 * time.Millisecond):
		// OK
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := q.Start(ctx); err != nil {
		t.Fatalf("failed to start queue: %v", err)
	}

	// Now it should run
	select {
	case <-done:
		// success
	case <-time.After(time.Second):
		t.Fatal("job not executed after Start")
	}
}

func TestQueue_Integration_Fairness(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 100,
		LaneQuotas: map[keylane.Lane]int{
			"noisy": 1,
			"quiet": 1,
		},
	}

	q, _ := keylane.New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	noisyExec := make(chan int, 10)
	quietDone := make(chan struct{})

	for i := 0; i < 10; i++ {
		val := i
		_ = q.Submit(ctx, keylane.Job{
			Key: "k", Lane: "noisy", Run: func(ctx context.Context) error {
				noisyExec <- val
				return nil
			},
		})
	}
	_ = q.Submit(ctx, keylane.Job{
		Key: "k", Lane: "quiet", Run: func(ctx context.Context) error {
			close(quietDone)
			return nil
		},
	})

	select {
	case <-quietDone:
		// success
	case <-time.After(time.Second):
		t.Fatal("quiet job was starved")
	}
}

func TestQueue_Integration_MultipleShards(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       16,
		WorkerCount:      4,
		QueueSizePerLane: 10,
		LaneQuotas: map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	const count = 20
	results := make(chan int, count)
	for i := 0; i < count; i++ {
		val := i
		_ = q.Submit(ctx, keylane.Job{
			Key:  string(rune('A' + i)), // different keys -> likely different shards
			Lane: "default",
			Run: func(ctx context.Context) error {
				results <- val
				return nil
			},
		})
	}

	for i := 0; i < count; i++ {
		select {
		case <-results:
		case <-time.After(time.Second):
			t.Fatalf("timed out on job %d", i)
		}
	}
}

func TestQueue_Integration_ContextCancel(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	_ = q.Start(ctx)

	cancel() // Stop workers

	done := make(chan struct{})
	_ = q.Submit(context.Background(), keylane.Job{
		Key: "k", Lane: "default", Run: func(ctx context.Context) error {
			close(done)
			return nil
		},
	})

	select {
	case <-done:
		t.Fatal("job should not have been executed after cancel")
	case <-time.After(100 * time.Millisecond):
		// success
	}
}
