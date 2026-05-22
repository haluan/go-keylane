// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRunDurationAcceptedJobRecordsSample(t *testing.T) {
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

	run := s.StatsGCPressure().Run
	if run.Count != 1 {
		t.Fatalf("global Run.Count = %d, want 1", run.Count)
	}
}

func TestRunDurationFailedJobRecordsSample(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)

	job, _ := NewInternalJob(func(ctx context.Context) error {
		return errors.New("fail")
	}, 0, 0)
	_, becameReady, _ := s.Enqueue(job)
	if becameReady {
		s.ReadyCh <- 0
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = s.Stop(stopCtx, true)

	if s.StatsGCPressure().Run.Count != 1 {
		t.Errorf("Run.Count = %d, want 1 for failed job", s.StatsGCPressure().Run.Count)
	}
}

func TestRunDurationCanceledJobRecordsSample(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)

	job, _ := NewInternalJob(func(ctx context.Context) error {
		return context.Canceled
	}, 0, 0)
	_, becameReady, _ := s.Enqueue(job)
	if becameReady {
		s.ReadyCh <- 0
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = s.Stop(stopCtx, true)

	if s.StatsGCPressure().Run.Count != 1 {
		t.Errorf("Run.Count = %d, want 1 for canceled job", s.StatsGCPressure().Run.Count)
	}
}

func TestRunDurationStoppedRejectionNoSample(t *testing.T) {
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

	if s.StatsGCPressure().Run.Count != 0 {
		t.Errorf("Run.Count = %d, want 0 for rejected admission", s.StatsGCPressure().Run.Count)
	}
}

func TestRunDurationQueueFullNoSample(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 2, reg)

	for i := 0; i < 5; i++ {
		job, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 0)
		_, _, _ = s.Enqueue(job)
	}

	if s.StatsGCPressure().Run.Count != 0 {
		t.Errorf("Run.Count = %d, want 0 for unstarted queued jobs", s.StatsGCPressure().Run.Count)
	}
}

func TestRunDurationSlowJobRecordsNonZeroTotal(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)

	job, _ := NewInternalJob(func(ctx context.Context) error {
		time.Sleep(25 * time.Millisecond)
		return nil
	}, 0, 0)
	_, becameReady, _ := s.Enqueue(job)
	if becameReady {
		s.ReadyCh <- 0
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = s.Stop(stopCtx, true)

	run := s.StatsGCPressure().Run
	if run.Count != 1 {
		t.Fatalf("Run.Count = %d, want 1", run.Count)
	}
	if run.TotalNanos < uint64(20*time.Millisecond) {
		t.Errorf("Run.TotalNanos = %d, want >= 20ms", run.TotalNanos)
	}
	if run.MaxNanos < uint64(20*time.Millisecond) {
		t.Errorf("Run.MaxNanos = %d, want >= 20ms", run.MaxNanos)
	}
}

func TestRunDurationMaxUpdatesWhenSlowerJobRuns(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)

	doneFast := make(chan struct{})
	jobFast, _ := NewInternalJob(func(ctx context.Context) error {
		close(doneFast)
		return nil
	}, 0, 0)
	_, becameReady, _ := s.Enqueue(jobFast)
	if becameReady {
		s.ReadyCh <- 0
	}
	<-doneFast

	maxAfterFast := s.StatsGCPressure().Run.MaxNanos

	doneSlow := make(chan struct{})
	jobSlow, _ := NewInternalJob(func(ctx context.Context) error {
		time.Sleep(50 * time.Millisecond)
		close(doneSlow)
		return nil
	}, 0, 0)
	_, becameReady, _ = s.Enqueue(jobSlow)
	if becameReady {
		s.ReadyCh <- 0
	}
	<-doneSlow

	maxAfterSlow := s.StatsGCPressure().Run.MaxNanos
	if maxAfterSlow < uint64(40*time.Millisecond) {
		t.Errorf("MaxNanos = %d after slow job, want >= 40ms", maxAfterSlow)
	}
	if maxAfterSlow < maxAfterFast {
		t.Errorf("MaxNanos decreased from %d to %d when slower job ran", maxAfterFast, maxAfterSlow)
	}
}

func TestRunDurationPerLaneIsolation(t *testing.T) {
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

	jobB, _ := NewInternalJob(func(ctx context.Context) error {
		time.Sleep(15 * time.Millisecond)
		return nil
	}, 1, 1)
	_, becameReady, _ = s.Enqueue(jobB)
	if becameReady {
		s.ReadyCh <- 0
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = s.Stop(stopCtx, true)

	snap := s.StatsGCPressure()
	if snap.Lanes[0].Run.Count != 1 {
		t.Errorf("laneA Run.Count = %d, want 1", snap.Lanes[0].Run.Count)
	}
	if snap.Lanes[1].Run.Count != 1 {
		t.Errorf("laneB Run.Count = %d, want 1", snap.Lanes[1].Run.Count)
	}
	if snap.Lanes[1].Run.MaxNanos < snap.Lanes[0].Run.MaxNanos {
		t.Errorf("laneB MaxNanos %d < laneA MaxNanos %d", snap.Lanes[1].Run.MaxNanos, snap.Lanes[0].Run.MaxNanos)
	}
}

func TestRunDurationPerShardIsolation(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(2, 2, 10, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)

	for shard := 0; shard < 2; shard++ {
		job, _ := NewInternalJob(func(ctx context.Context) error { return nil }, uint64(shard), 0)
		shardID, becameReady, _ := s.Enqueue(job)
		if becameReady {
			s.ReadyCh <- shardID
		}
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = s.Stop(stopCtx, true)

	snap := s.StatsGCPressure()
	if snap.Shards[0].Run.Count == 0 || snap.Shards[1].Run.Count == 0 {
		t.Errorf("expected run samples on both shards, got %d and %d",
			snap.Shards[0].Run.Count, snap.Shards[1].Run.Count)
	}
}

func sumLaneRunGCPressure(lanes []LaneStatsGCPressure) (count, total uint64) {
	for _, lane := range lanes {
		count += lane.Run.Count
		total += lane.Run.TotalNanos
	}
	return count, total
}

func TestRunDurationGlobalEqualsSumOfLanes(t *testing.T) {
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
	laneCount, laneTotal := sumLaneRunGCPressure(snap.Lanes)

	if snap.Run.Count != laneCount {
		t.Errorf("global Run.Count = %d, sum of lanes = %d", snap.Run.Count, laneCount)
	}
	if snap.Run.TotalNanos != laneTotal {
		t.Errorf("global Run.TotalNanos = %d, sum of lanes = %d", snap.Run.TotalNanos, laneTotal)
	}
}

func TestRunDurationExcludesQueueWait(t *testing.T) {
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

	snap := s.StatsGCPressure()
	if snap.Run.TotalNanos < uint64(40*time.Millisecond) {
		t.Errorf("Run.TotalNanos %d, want >= 40ms", snap.Run.TotalNanos)
	}
	if snap.QueueWait.TotalNanos >= uint64(50*time.Millisecond) {
		t.Errorf("QueueWait.TotalNanos %d, want low for immediate execution", snap.QueueWait.TotalNanos)
	}
}

func TestRunDurationSnapshotImmutable(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
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

	snap1 := s.StatsGCPressure()
	snap1.Run.Count = 999
	snap1.Lanes[0].Run.MaxNanos = 999

	snap2 := s.StatsGCPressure()
	if snap2.Run.Count != 1 {
		t.Errorf("Run.Count = %d after mutation, want 1", snap2.Run.Count)
	}
}
