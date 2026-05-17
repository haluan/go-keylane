package keylane_test

import (
	"context"
	"testing"
	"time"

	"github.com/haluan/go-keylane"
)

func TestTrySubmitSuccess(t *testing.T) {
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

	if ok := q.TrySubmit(job); !ok {
		t.Fatal("expected TrySubmit to succeed")
	}

	select {
	case <-done:
		// success
	case <-time.After(time.Second):
		t.Fatal("job timed out")
	}
}

func TestTrySubmitRejectsInvalidJob(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       4,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas: map[keylane.Lane]int{
			"default": 1,
		},
	}
	q, _ := keylane.New(cfg)

	job := keylane.Job{
		Key:  "",
		Lane: "default",
		Run:  nil,
	}

	if ok := q.TrySubmit(job); ok {
		t.Fatal("expected TrySubmit to reject invalid job")
	}
}

func TestTrySubmitRejectsUnknownLane(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       4,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas: map[keylane.Lane]int{
			"default": 1,
		},
	}
	q, _ := keylane.New(cfg)

	job := keylane.Job{
		Key:  "key",
		Lane: "unknown",
		Run:  func(ctx context.Context) error { return nil },
	}

	if ok := q.TrySubmit(job); ok {
		t.Fatal("expected TrySubmit to reject unknown lane")
	}
}

func TestTrySubmitReturnsFalseWhenQueueFull(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 1,
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

	blockChan := make(chan struct{})
	defer close(blockChan)

	// First job: will be processed immediately and block the worker
	job1 := keylane.Job{
		Key:  "same-key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			<-blockChan
			return nil
		},
	}

	if ok := q.TrySubmit(job1); !ok {
		t.Fatal("expected first TrySubmit to succeed")
	}

	// Wait slightly to ensure job1 is picked up by the worker and is in-flight
	time.Sleep(10 * time.Millisecond)

	// Second job: will sit in the queue (capacity 1)
	job2 := keylane.Job{
		Key:  "same-key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	}

	if ok := q.TrySubmit(job2); !ok {
		t.Fatal("expected second TrySubmit to succeed")
	}

	// Third job: queue is full, so should return false
	job3 := keylane.Job{
		Key:  "same-key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	}

	if ok := q.TrySubmit(job3); ok {
		t.Fatal("expected third TrySubmit to fail (queue full)")
	}
}

func TestTrySubmitReturnsFalseWhenNotStarted(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 2,
		LaneQuotas: map[keylane.Lane]int{
			"default": 1,
		},
	}
	q, err := keylane.New(cfg)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	job := keylane.Job{
		Key:  "key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	}

	if ok := q.TrySubmit(job); ok {
		t.Fatal("expected TrySubmit to fail when not started")
	}
}

func TestTrySubmitReturnsFalseWhenStopped(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 2,
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

	if err := q.Stop(context.Background()); err != nil {
		t.Fatalf("failed to stop queue: %v", err)
	}

	job := keylane.Job{
		Key:  "key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	}

	if ok := q.TrySubmit(job); ok {
		t.Fatal("expected TrySubmit to fail when stopped")
	}
}

func TestTrySubmitDoesNotBlock(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 1,
		LaneQuotas: map[keylane.Lane]int{
			"default": 1,
		},
	}
	q, _ := keylane.New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	blockChan := make(chan struct{})
	defer close(blockChan)

	// Block the worker
	_ = q.TrySubmit(keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			<-blockChan
			return nil
		},
	})
	time.Sleep(10 * time.Millisecond)

	// Fill the queue
	_ = q.TrySubmit(keylane.Job{Key: "key", Lane: "default", Run: func(ctx context.Context) error { return nil }})

	// Now TrySubmit should instantly return false without blocking
	start := time.Now()
	ok := q.TrySubmit(keylane.Job{Key: "key", Lane: "default", Run: func(ctx context.Context) error { return nil }})
	duration := time.Since(start)

	if ok {
		t.Fatal("expected TrySubmit to fail")
	}
	if duration > 10*time.Millisecond {
		t.Errorf("TrySubmit took too long: %v (expected it not to block)", duration)
	}
}

func TestTrySubmitDoesNotDuplicateReadyShard(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas: map[keylane.Lane]int{
			"default": 1,
		},
	}
	q, err := keylane.New(cfg)
	if err != nil {
		t.Fatalf("failed to create: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	blockChan := make(chan struct{})
	defer close(blockChan)

	// Block the worker
	_ = q.TrySubmit(keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			<-blockChan
			return nil
		},
	})
	time.Sleep(10 * time.Millisecond)

	// Submit job 1 to empty shard -> should mark it ready
	if ok := q.TrySubmit(keylane.Job{Key: "key", Lane: "default", Run: func(ctx context.Context) error { return nil }}); !ok {
		t.Fatal("failed first submit")
	}

	// Submit job 2 to same shard -> should NOT duplicate ready status
	if ok := q.TrySubmit(keylane.Job{Key: "key", Lane: "default", Run: func(ctx context.Context) error { return nil }}); !ok {
		t.Fatal("failed second submit")
	}
}
