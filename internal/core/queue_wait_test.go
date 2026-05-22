// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestQueueWaitAcceptedJobRecordsSample(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)

	done := make(chan struct{})
	job, _ := NewInternalJob(func(ctx context.Context) error {
		close(done)
		return nil
	}, 0, 0)

	_, becameReady, _ := s.Enqueue(job)
	if becameReady {
		s.ReadyCh <- 0
	}
	<-done

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = s.Stop(stopCtx, true)

	qw := s.StatsGCPressure().QueueWait
	if qw.Count != 1 {
		t.Fatalf("global Count = %d, want 1", qw.Count)
	}
	// Immediate execution may record zero nanoseconds of queue wait; blocking tests cover non-zero wait.
}

func TestQueueWaitStoppedRejectionNoSample(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = s.Stop(stopCtx, true)

	job, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 0)
	_, _, err := s.Enqueue(job)
	if !errors.Is(err, ErrStopped) {
		t.Fatalf("expected ErrStopped, got %v", err)
	}

	if s.StatsGCPressure().QueueWait.Count != 0 {
		t.Errorf("Count = %d, want 0 for rejected admission", s.StatsGCPressure().QueueWait.Count)
	}
}

func TestQueueWaitQueueFullNoSample(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 2, reg)

	for i := 0; i < 5; i++ {
		job, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 0)
		_, _, _ = s.Enqueue(job)
	}

	qw := s.StatsGCPressure().QueueWait
	if qw.Count != 0 {
		t.Errorf("Count = %d, want 0 for unstarted queued jobs", qw.Count)
	}
}

func TestQueueWaitBlockingJobWaits(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)

	blockA := make(chan struct{})

	jobA, _ := NewInternalJob(func(ctx context.Context) error {
		<-blockA
		return nil
	}, 0, 0)
	_, becameReady, _ := s.Enqueue(jobA)
	if becameReady {
		s.ReadyCh <- 0
	}

	time.Sleep(20 * time.Millisecond)

	jobB, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 0)
	_, becameReadyB, _ := s.Enqueue(jobB)
	if becameReadyB {
		s.ReadyCh <- 0
	}

	if s.StatsGCPressure().TotalQueued == 0 {
		t.Fatal("expected job B queued behind A")
	}

	close(blockA)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = s.Stop(stopCtx, true)

	qw := s.StatsGCPressure().QueueWait
	if qw.Count < 2 {
		t.Fatalf("Count = %d, want at least 2", qw.Count)
	}
	if qw.MaxNanos == 0 {
		t.Fatal("MaxNanos = 0, want > 0")
	}
	if qw.TotalNanos < qw.MaxNanos {
		t.Fatalf("TotalNanos %d < MaxNanos %d", qw.TotalNanos, qw.MaxNanos)
	}
}

func TestQueueWaitMaxMonotonic(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)

	block := make(chan struct{})
	jobLong, _ := NewInternalJob(func(ctx context.Context) error {
		<-block
		return nil
	}, 0, 0)
	_, becameReady, _ := s.Enqueue(jobLong)
	if becameReady {
		s.ReadyCh <- 0
	}
	time.Sleep(30 * time.Millisecond)

	jobShort, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 1, 0)
	_, becameReady, _ = s.Enqueue(jobShort)
	if becameReady {
		s.ReadyCh <- 0
	}
	time.Sleep(20 * time.Millisecond)

	max1 := s.StatsGCPressure().QueueWait.MaxNanos
	close(block)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = s.Stop(stopCtx, true)

	max2 := s.StatsGCPressure().QueueWait.MaxNanos
	if max2 < max1 {
		t.Errorf("MaxNanos decreased from %d to %d", max1, max2)
	}
}

func sumLaneQueueWaitGCPressure(lanes []LaneStatsGCPressure) (count, total uint64) {
	for _, lane := range lanes {
		count += lane.QueueWait.Count
		total += lane.QueueWait.TotalNanos
	}
	return count, total
}

func TestQueueWaitGlobalEqualsSumOfLanes(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"laneA": 1, "laneB": 1})
	s, _ := NewScheduler(1, 1, 10, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)

	jobA, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 0)
	_, becameReady, _ := s.Enqueue(jobA)
	if becameReady {
		s.ReadyCh <- 0
	}

	jobB, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 1, 1)
	_, becameReady, _ = s.Enqueue(jobB)
	if becameReady {
		s.ReadyCh <- 0
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = s.Stop(stopCtx, true)

	snap := s.StatsGCPressure()
	laneCount, laneTotal := sumLaneQueueWaitGCPressure(snap.Lanes)

	if snap.QueueWait.Count != laneCount {
		t.Errorf("global Count = %d, sum of lanes = %d", snap.QueueWait.Count, laneCount)
	}
	if snap.QueueWait.TotalNanos != laneTotal {
		t.Errorf("global TotalNanos = %d, sum of lanes = %d", snap.QueueWait.TotalNanos, laneTotal)
	}
	if snap.Lanes[0].QueueWait.Count != 1 || snap.Lanes[1].QueueWait.Count != 1 {
		t.Errorf("per-lane Count: laneA=%d laneB=%d, want 1 each",
			snap.Lanes[0].QueueWait.Count, snap.Lanes[1].QueueWait.Count)
	}
}

func TestQueueWaitPerLaneIsolation(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"a": 1, "b": 1})
	s, _ := NewScheduler(1, 1, 10, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)

	job, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 0)
	_, becameReady, _ := s.Enqueue(job)
	if becameReady {
		s.ReadyCh <- 0
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = s.Stop(stopCtx, true)

	snap := s.StatsGCPressure()
	if snap.Lanes[0].QueueWait.Count == 0 {
		t.Error("expected lane a queue wait samples")
	}
	if snap.Lanes[1].QueueWait.Count != 0 {
		t.Errorf("lane b Count = %d, want 0", snap.Lanes[1].QueueWait.Count)
	}
}

func TestQueueWaitPerShard(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(2, 1, 10, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)

	job, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 0)
	_, becameReady, _ := s.Enqueue(job)
	if becameReady {
		s.ReadyCh <- 0
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = s.Stop(stopCtx, true)

	snap := s.StatsGCPressure()
	if snap.Shards[0].QueueWait.Count == 0 {
		t.Error("expected shard 0 queue wait samples")
	}
}

func TestQueueWaitExcludesRunDuration(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)

	job, _ := NewInternalJob(func(ctx context.Context) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	}, 0, 0)
	_, becameReady, _ := s.Enqueue(job)
	if becameReady {
		s.ReadyCh <- 0
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = s.Stop(stopCtx, true)

	qw := s.StatsGCPressure().QueueWait
	if qw.TotalNanos >= uint64(50*time.Millisecond) {
		t.Errorf("TotalNanos %d looks like it includes run duration", qw.TotalNanos)
	}
}

func TestQueueWaitV1OptInOffGCPressureStillOn(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)
	s.Obs.TrackQueueWait = false

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)

	job, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 0)
	_, becameReady, _ := s.Enqueue(job)
	if becameReady {
		s.ReadyCh <- 0
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = s.Stop(stopCtx, true)

	if s.StatsGCPressure().QueueWait.Count != 1 {
		t.Errorf("GC queue wait Count = %d, want 1", s.StatsGCPressure().QueueWait.Count)
	}
	v1, _ := s.Stats()
	if v1[0].Lanes[0].QueueWaitCount != 0 {
		t.Errorf("v1 QueueWaitCount = %d, want 0 when TrackQueueWait disabled", v1[0].Lanes[0].QueueWaitCount)
	}
}

func TestQueueWaitSnapshotImmutable(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)

	job, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 0)
	_, _, _ = s.Enqueue(job)

	snap1 := s.StatsGCPressure()
	snap1.QueueWait.Count = 999
	snap1.Lanes[0].QueueWait.MaxNanos = 999

	snap2 := s.StatsGCPressure()
	if snap2.QueueWait.Count != 0 {
		t.Errorf("global Count = %d after mutation, want 0", snap2.QueueWait.Count)
	}
}
