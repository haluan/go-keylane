package core

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerExecutesSubmittedJob(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 10})
	s, _ := NewScheduler(1, 1, 10, reg)
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var executed int32
	done := make(chan struct{})
	run := func(ctx context.Context) error {
		atomic.AddInt32(&executed, 1)
		close(done)
		return nil
	}

	_, _, _ = s.Enqueue(InternalJob{LaneID: 0, Run: run})
	
	// Notify Ready channel manually since we are testing WorkerLoop directly
	s.ReadyCh <- 0

	go s.WorkerLoop(ctx)

	select {
	case <-done:
		if atomic.LoadInt32(&executed) != 1 {
			t.Errorf("executed = %d, want 1", executed)
		}
	case <-time.After(time.Second):
		t.Fatal("worker timed out")
	}
}

func TestWorkerIgnoresInvalidShardID(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 10})
	s, _ := NewScheduler(1, 1, 10, reg)
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.WorkerLoop(ctx)

	// Send invalid shard IDs. Should not panic.
	s.ReadyCh <- -1
	s.ReadyCh <- 999
	
	// Give it a moment to process (or ignore)
	time.Sleep(10 * time.Millisecond)
}

func TestWorkerStopsOnContextCancel(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 10})
	s, _ := NewScheduler(1, 1, 10, reg)
	
	ctx, cancel := context.WithCancel(context.Background())
	
	done := make(chan struct{})
	go func() {
		s.WorkerLoop(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// success
	case <-time.After(time.Second):
		t.Fatal("worker did not stop on cancel")
	}
}
