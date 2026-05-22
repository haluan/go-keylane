// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

func laneCountersFromSnap(snap StatsGCPressureSnapshot, laneIdx int) LaneCountersGCPressure {
	return snap.Lanes[laneIdx].Counters
}

func TestLaneCountersZeroedAtInit(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1, "fast": 2})
	s, err := NewScheduler(2, 1, 10, reg)
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	snap := s.StatsGCPressure()
	if snap.Version != StatsGCPressureVersion {
		t.Fatalf("Version = %q, want %q", snap.Version, StatsGCPressureVersion)
	}
	for i, c := range snap.Lanes {
		if c.Counters != (LaneCountersGCPressure{}) {
			t.Errorf("lane %d counters = %+v, want zero", i, c.Counters)
		}
	}
}

func TestLaneCountersSubmitAccepted(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)

	job, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 0)
	_, _, err := s.Enqueue(job)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	c := laneCountersFromSnap(s.StatsGCPressure(), 0)
	if c.Submitted != 1 || c.Accepted != 1 || c.Rejected != 0 {
		t.Errorf("counters = %+v, want Submitted=1 Accepted=1 Rejected=0", c)
	}
}

func TestLaneCountersQueueFull(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 2, reg)

	var queueFullCount int
	for i := 0; i < 5; i++ {
		job, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 0)
		_, _, err := s.Enqueue(job)
		if errors.Is(err, ErrQueueFull) {
			queueFullCount++
		}
	}

	c := laneCountersFromSnap(s.StatsGCPressure(), 0)
	if c.Submitted < c.Accepted+c.Rejected {
		t.Errorf("Submitted %d < Accepted %d + Rejected %d", c.Submitted, c.Accepted, c.Rejected)
	}
	if c.QueueFull != uint64(queueFullCount) {
		t.Errorf("QueueFull = %d, want %d", c.QueueFull, queueFullCount)
	}
	if c.Rejected != c.QueueFull {
		t.Errorf("Rejected = %d, want QueueFull %d", c.Rejected, c.QueueFull)
	}
}

func TestLaneCountersCompletedFailed(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)

	doneOK := make(chan struct{})
	jobOK, _ := NewInternalJob(func(ctx context.Context) error {
		close(doneOK)
		return nil
	}, 0, 0)
	_, becameReady, _ := s.Enqueue(jobOK)
	if becameReady {
		s.ReadyCh <- 0
	}
	<-doneOK

	doneFail := make(chan struct{})
	jobFail, _ := NewInternalJob(func(ctx context.Context) error {
		close(doneFail)
		return errors.New("fail")
	}, 1, 0)
	_, becameReady, _ = s.Enqueue(jobFail)
	if becameReady {
		s.ReadyCh <- 0
	}
	<-doneFail

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := s.Stop(stopCtx, true); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	c := laneCountersFromSnap(s.StatsGCPressure(), 0)
	if c.Completed != 1 {
		t.Errorf("Completed = %d, want 1", c.Completed)
	}
	if c.Failed != 1 {
		t.Errorf("Failed = %d, want 1", c.Failed)
	}
}

func TestLaneCountersCanceled(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)

	done := make(chan struct{})
	job, _ := NewInternalJob(func(ctx context.Context) error {
		close(done)
		return context.Canceled
	}, 0, 0)
	_, becameReady, _ := s.Enqueue(job)
	if becameReady {
		s.ReadyCh <- 0
	}
	<-done

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = s.Stop(stopCtx, true)

	c := laneCountersFromSnap(s.StatsGCPressure(), 0)
	if c.Canceled != 1 {
		t.Errorf("Canceled = %d, want 1", c.Canceled)
	}
	if c.Failed != 0 {
		t.Errorf("Failed = %d, want 0 for context.Canceled", c.Failed)
	}
}

func TestLaneCountersMultiLaneIsolation(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1, "fast": 1})
	s, _ := NewScheduler(1, 1, 10, reg)

	job, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 0)
	_, _, _ = s.Enqueue(job)

	snap := s.StatsGCPressure()
	if snap.Lanes[0].Counters.Submitted != 1 {
		t.Errorf("default Submitted = %d, want 1", snap.Lanes[0].Counters.Submitted)
	}
	if snap.Lanes[1].Counters.Submitted != 0 {
		t.Errorf("fast Submitted = %d, want 0", snap.Lanes[1].Counters.Submitted)
	}
}

func TestLaneCountersSnapshotImmutable(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)

	job, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 0)
	_, _, _ = s.Enqueue(job)

	snap1 := s.StatsGCPressure()
	snap1.Lanes[0].Counters.Submitted = 999

	snap2 := s.StatsGCPressure()
	if snap2.Lanes[0].Counters.Submitted != 1 {
		t.Errorf("Submitted = %d after mutation, want 1", snap2.Lanes[0].Counters.Submitted)
	}
}

func TestLaneCountersTerminalInvariant(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)

	for i := 0; i < 3; i++ {
		job, _ := NewInternalJob(func(ctx context.Context) error { return nil }, uint64(i), 0)
		_, becameReady, _ := s.Enqueue(job)
		if becameReady {
			s.ReadyCh <- 0
		}
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := s.Stop(stopCtx, true); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	c := laneCountersFromSnap(s.StatsGCPressure(), 0)
	terminal := c.Completed + c.Failed + c.Canceled + c.Panicked
	if terminal > c.Accepted {
		t.Errorf("terminal %d > accepted %d (completed=%d failed=%d canceled=%d panicked=%d)",
			terminal, c.Accepted, c.Completed, c.Failed, c.Canceled, c.Panicked)
	}
}
