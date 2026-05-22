// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestStatsGCPressureEmptyScheduler(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, err := NewScheduler(2, 3, 5, reg)
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	snap := s.StatsGCPressure()
	if snap.Version != StatsGCPressureVersion {
		t.Errorf("Version = %q, want %q", snap.Version, StatsGCPressureVersion)
	}
	if snap.ShardCount != 2 {
		t.Errorf("ShardCount = %d, want 2", snap.ShardCount)
	}
	if snap.LaneCount != 1 {
		t.Errorf("LaneCount = %d, want 1", snap.LaneCount)
	}
	if snap.WorkerCount != 3 {
		t.Errorf("WorkerCount = %d, want 3", snap.WorkerCount)
	}
	if snap.TotalQueued != 0 || snap.TotalInFlight != 0 {
		t.Errorf("expected zero totals, got queued=%d inflight=%d", snap.TotalQueued, snap.TotalInFlight)
	}
	if len(snap.Shards) != 2 {
		t.Fatalf("len(Shards) = %d, want 2", len(snap.Shards))
	}
	for i, shard := range snap.Shards {
		if shard.Queued != 0 || shard.InFlight != 0 {
			t.Errorf("shard %d: queued=%d inflight=%d, want 0", i, shard.Queued, shard.InFlight)
		}
		if shard.Capacity != 5 {
			t.Errorf("shard %d capacity = %d, want 5", i, shard.Capacity)
		}
	}
}

func TestStatsGCPressureQueuedJobs(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1, "fast": 2})
	s, _ := NewScheduler(1, 1, 10, reg)

	for i := 0; i < 3; i++ {
		job, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 0)
		_, _, _ = s.Enqueue(job)
	}
	jobFast, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 1)
	_, _, _ = s.Enqueue(jobFast)

	snap := s.StatsGCPressure()
	if snap.TotalQueued != 4 {
		t.Errorf("TotalQueued = %d, want 4", snap.TotalQueued)
	}
	if snap.Shards[0].Queued != 4 {
		t.Errorf("shard queued = %d, want 4", snap.Shards[0].Queued)
	}
	if len(snap.Shards[0].PerLane) != 2 {
		t.Fatalf("len(PerLane) = %d, want 2", len(snap.Shards[0].PerLane))
	}
	if snap.Shards[0].PerLane[0].Queued != 3 {
		t.Errorf("default lane queued = %d, want 3", snap.Shards[0].PerLane[0].Queued)
	}
	if snap.Shards[0].PerLane[1].Queued != 1 {
		t.Errorf("fast lane queued = %d, want 1", snap.Shards[0].PerLane[1].Queued)
	}
	if snap.Lanes[0].Queued != 3 || snap.Lanes[1].Queued != 1 {
		t.Errorf("lane totals: default=%d fast=%d", snap.Lanes[0].Queued, snap.Lanes[1].Queued)
	}
	if snap.Lanes[0].Name != "default" || snap.Lanes[1].Name != "fast" {
		t.Errorf("lane names: %q, %q", snap.Lanes[0].Name, snap.Lanes[1].Name)
	}
}

func TestStatsGCPressureInFlightDuringExecution(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	jobBlock := make(chan struct{})
	jobStart := make(chan struct{})
	job, _ := NewInternalJob(func(ctx context.Context) error {
		close(jobStart)
		<-jobBlock
		return nil
	}, 0, 0)

	_, becameReady, _ := s.Enqueue(job)
	if becameReady {
		s.ReadyCh <- 0
	}

	select {
	case <-jobStart:
	case <-time.After(time.Second):
		t.Fatal("job did not start")
	}

	snap := s.StatsGCPressure()
	if snap.TotalInFlight != 1 {
		t.Errorf("TotalInFlight = %d, want 1", snap.TotalInFlight)
	}
	if snap.Shards[0].InFlight != 1 {
		t.Errorf("shard InFlight = %d, want 1", snap.Shards[0].InFlight)
	}
	if snap.Lanes[0].InFlight != 1 {
		t.Errorf("lane InFlight = %d, want 1", snap.Lanes[0].InFlight)
	}
	close(jobBlock)
}

func TestStatsGCPressureInFlightReturnsToZero(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	job, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 0)
	_, becameReady, _ := s.Enqueue(job)
	if becameReady {
		s.ReadyCh <- 0
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := s.Stop(stopCtx, true); err != nil {
		t.Fatalf("Stop with drain: %v", err)
	}

	snap := s.StatsGCPressure()
	if snap.TotalInFlight != 0 {
		t.Errorf("TotalInFlight = %d, want 0 after drain", snap.TotalInFlight)
	}
	if snap.TotalQueued != 0 {
		t.Errorf("TotalQueued = %d, want 0 after drain", snap.TotalQueued)
	}
}

func TestStatsGCPressureQueueFullDoesNotCorruptStats(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 2, reg)

	for i := 0; i < 5; i++ {
		job, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 0)
		_, _, err := s.Enqueue(job)
		if err != nil && !errors.Is(err, ErrQueueFull) {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	snap := s.StatsGCPressure()
	if snap.TotalQueued > 2 {
		t.Errorf("TotalQueued = %d, want at most 2", snap.TotalQueued)
	}
	if snap.Shards[0].Capacity != 2 {
		t.Errorf("Capacity = %d, want 2", snap.Shards[0].Capacity)
	}
}

func TestStatsGCPressureDoesNotExposeMutableInternals(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)

	job, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 0)
	_, _, _ = s.Enqueue(job)

	snap1 := s.StatsGCPressure()
	snap1.Shards[0].PerLane[0].Queued = 999
	snap1.Lanes[0].Queued = 999

	snap2 := s.StatsGCPressure()
	if snap2.Shards[0].PerLane[0].Queued != 1 {
		t.Errorf("PerLane queued = %d, want 1 after mutation", snap2.Shards[0].PerLane[0].Queued)
	}
	if snap2.Lanes[0].Queued != 1 {
		t.Errorf("lane queued = %d, want 1 after mutation", snap2.Lanes[0].Queued)
	}
}
