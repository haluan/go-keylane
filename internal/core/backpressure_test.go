package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBackpressureScoping(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{
		"laneX": 1,
		"laneY": 1,
	})
	// 2 shards, 1 worker, QueueSizePerLane = 1
	s, err := NewScheduler(2, 1, 1, reg)
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("failed to start scheduler: %v", err)
	}

	// Route keys: Shard 0 and Shard 1
	var keyHash0 uint64 = 0 // routes to Shard 0
	var keyHash1 uint64 = 1 // routes to Shard 1

	blockChan := make(chan struct{})
	defer close(blockChan)

	// Job 1: Block the worker on Shard 0, Lane X
	jobBlock, _ := NewInternalJob(func(ctx context.Context) error {
		<-blockChan
		return nil
	}, keyHash0, 0) // laneX is 0

	shardID, becameReady, err := s.Enqueue(jobBlock)
	if err != nil {
		t.Fatalf("failed to enqueue blocking job: %v", err)
	}
	if becameReady {
		s.ReadyCh <- shardID
	}

	// Wait slightly for the worker to pick up and execute the block job
	time.Sleep(10 * time.Millisecond)

	// Shard 0, Lane X is now running jobBlock, so the queue is empty, but worker is busy.
	// Job 2: Fill the queue of Shard 0, Lane X (capacity 1)
	jobFill, _ := NewInternalJob(func(ctx context.Context) error { return nil }, keyHash0, 0)
	_, _, err = s.Enqueue(jobFill)
	if err != nil {
		t.Fatalf("failed to enqueue fill job: %v", err)
	}

	// Shard 0, Lane X queue is now FULL.
	// Job 3: Attempt to enqueue another job into Shard 0, Lane X -> should fail with ErrQueueFull
	jobExtra, _ := NewInternalJob(func(ctx context.Context) error { return nil }, keyHash0, 0)
	_, _, err = s.Enqueue(jobExtra)
	if !errors.Is(err, ErrQueueFull) {
		t.Errorf("expected Enqueue to return ErrQueueFull, got %v", err)
	}

	// Attempt TryEnqueue into Shard 0, Lane X -> should fail with ErrQueueFull
	_, _, err = s.TryEnqueue(jobExtra)
	if !errors.Is(err, ErrQueueFull) {
		t.Errorf("expected TryEnqueue to return ErrQueueFull, got %v", err)
	}

	// Job 4: Validate that Shard 1, Lane X queue is NOT full
	job1X, _ := NewInternalJob(func(ctx context.Context) error { return nil }, keyHash1, 0)
	_, _, err = s.Enqueue(job1X)
	if err != nil {
		t.Errorf("expected Shard 1, Lane X enqueue to succeed, got %v", err)
	}

	// Job 5: Validate that Shard 0, Lane Y queue is NOT full
	job0Y, _ := NewInternalJob(func(ctx context.Context) error { return nil }, keyHash0, 1) // laneY is 1
	_, _, err = s.Enqueue(job0Y)
	if err != nil {
		t.Errorf("expected Shard 0, Lane Y enqueue to succeed, got %v", err)
	}
}

func TestTryEnqueueDoesNotDuplicateReadyShard(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg) // 1 worker

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)

	blockChan := make(chan struct{})
	defer close(blockChan)

	// Block the worker
	jobBlock, _ := NewInternalJob(func(ctx context.Context) error {
		<-blockChan
		return nil
	}, 0, 0)
	shardID, becameReady, err := s.TryEnqueue(jobBlock)
	if err != nil {
		t.Fatalf("block job failed: %v", err)
	}
	if becameReady {
		s.ReadyCh <- shardID
	}
	time.Sleep(10 * time.Millisecond)

	job1, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 0)
	job2, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 0)

	// First enqueue should mark ready
	_, becameReady1, err := s.TryEnqueue(job1)
	if err != nil {
		t.Fatalf("first enqueue failed: %v", err)
	}
	if !becameReady1 {
		t.Error("expected first enqueue to mark ready")
	}

	// Second enqueue should NOT mark ready (becameReady == false)
	_, becameReady2, err := s.TryEnqueue(job2)
	if err != nil {
		t.Fatalf("second enqueue failed: %v", err)
	}
	if becameReady2 {
		t.Error("expected second enqueue NOT to mark ready")
	}
}
