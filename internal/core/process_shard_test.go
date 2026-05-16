package core

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestProcessShardRespectsLaneQuota(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 2})
	s, _ := NewScheduler(1, 1, 10, reg)
	
	ctx := context.Background()
	var executed int32
	run := func(ctx context.Context) error {
		atomic.AddInt32(&executed, 1)
		return nil
	}

	// Enqueue 5 jobs into lane 0 (quota 2)
	for i := 0; i < 5; i++ {
		_, _, _ = s.Enqueue(InternalJob{LaneID: 0, Run: run})
	}

	// One process pass
	s.processShard(ctx, 0)

	if atomic.LoadInt32(&executed) != 2 {
		t.Errorf("executed = %d, want 2 (quota)", executed)
	}

	// Should be requeued
	select {
	case id := <-s.ReadyCh:
		if id != 0 {
			t.Errorf("got shard %d from ReadyCh, want 0", id)
		}
	default:
		t.Fatal("shard should have been requeued")
	}
}

func TestProcessShardClearsReadyWhenEmpty(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 10})
	s, _ := NewScheduler(1, 1, 10, reg)
	
	ctx := context.Background()
	_, _, _ = s.Enqueue(InternalJob{LaneID: 0, Run: func(ctx context.Context) error { return nil }})

	if !s.shards[0].Ready {
		t.Fatal("shard should be Ready after enqueue")
	}

	s.processShard(ctx, 0)

	if s.shards[0].Ready {
		t.Error("shard should NOT be Ready after processing all jobs")
	}
}

func TestProcessShardPreservesFIFOWithinLane(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 10})
	s, _ := NewScheduler(1, 1, 10, reg)
	
	ctx := context.Background()
	var results []int
	for i := 0; i < 5; i++ {
		val := i
		_, _, _ = s.Enqueue(InternalJob{LaneID: 0, Run: func(ctx context.Context) error {
			results = append(results, val)
			return nil
		}})
	}

	s.processShard(ctx, 0)

	if len(results) != 5 {
		t.Fatalf("len(results) = %d, want 5", len(results))
	}
	for i, v := range results {
		if v != i {
			t.Errorf("results[%d] = %d, want %d", i, v, i)
		}
	}
}

func TestProcessShardContinuesAfterJobError(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 10})
	s, _ := NewScheduler(1, 1, 10, reg)
	
	ctx := context.Background()
	var executed int32
	
	_, _, _ = s.Enqueue(InternalJob{LaneID: 0, Run: func(ctx context.Context) error {
		atomic.AddInt32(&executed, 1)
		return context.Canceled // Some error
	}})
	_, _, _ = s.Enqueue(InternalJob{LaneID: 0, Run: func(ctx context.Context) error {
		atomic.AddInt32(&executed, 1)
		return nil
	}})

	s.processShard(ctx, 0)

	if atomic.LoadInt32(&executed) != 2 {
		t.Errorf("executed = %d, want 2", executed)
	}
}

func TestProcessShardNoisyLaneDoesNotStarveOtherLane(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"noisy": 1, "quiet": 1})
	s, _ := NewScheduler(1, 1, 100, reg)
	
	ctx := context.Background()
	var noisyExec, quietExec int32

	// Fill noisy lane (ID 0) with many jobs
	for i := 0; i < 10; i++ {
		_, _, _ = s.Enqueue(InternalJob{LaneID: 0, Run: func(ctx context.Context) error {
			atomic.AddInt32(&noisyExec, 1)
			return nil
		}})
	}
	// Add one job to quiet lane (ID 1)
	_, _, _ = s.Enqueue(InternalJob{LaneID: 1, Run: func(ctx context.Context) error {
		atomic.AddInt32(&quietExec, 1)
		return nil
	}})

	// Process one shard pass
	s.processShard(ctx, 0)

	if atomic.LoadInt32(&noisyExec) != 1 {
		t.Errorf("noisyExec = %d, want 1 (quota)", noisyExec)
	}
	if atomic.LoadInt32(&quietExec) != 1 {
		t.Errorf("quietExec = %d, want 1 (quota)", quietExec)
	}
}

func TestProcessShardDoesNotRequeueWhenEmpty(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 10})
	s, _ := NewScheduler(1, 1, 10, reg)
	
	ctx := context.Background()
	_, _, _ = s.Enqueue(InternalJob{LaneID: 0, Run: func(ctx context.Context) error { return nil }})

	// One pass clears everything
	s.processShard(ctx, 0)

	select {
	case id := <-s.ReadyCh:
		t.Errorf("got shard %d from ReadyCh, want nothing", id)
	default:
		// success
	}
}

func TestProcessShardDuplicateReadyPrevention(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 10})
	s, _ := NewScheduler(1, 1, 10, reg)
	
	// First enqueue sets Ready=true
	_, ready1, _ := s.Enqueue(InternalJob{LaneID: 0, Run: func(ctx context.Context) error { return nil }})
	if !ready1 { t.Fatal("should be ready") }

	// Second enqueue to same shard while Ready=true should return becameReady=false
	_, ready2, _ := s.Enqueue(InternalJob{LaneID: 0, Run: func(ctx context.Context) error { return nil }})
	if ready2 { t.Error("should NOT be ready again") }
}
