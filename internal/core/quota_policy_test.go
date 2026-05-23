// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestBuildQuotaPolicySnapshotRejectsLowLaneQuota(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	_, err := buildQuotaPolicySnapshot(reg, QuotaPolicyInput{
		DefaultQuota: 1,
		LaneQuotas:   map[string]uint32{"default": 0},
	})
	if !errors.Is(err, ErrInvalidLaneQuota) {
		t.Errorf("err = %v, want ErrInvalidLaneQuota", err)
	}
}

func TestBuildQuotaPolicySnapshotRejectsQuotaTooLarge(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	_, err := buildQuotaPolicySnapshot(reg, QuotaPolicyInput{
		DefaultQuota: 1,
		LaneQuotas:   map[string]uint32{"default": MaxLaneQuota + 1},
	})
	if !errors.Is(err, ErrQuotaTooLarge) {
		t.Errorf("err = %v, want ErrQuotaTooLarge", err)
	}
}

func TestBuildQuotaPolicySnapshotRejectsDefaultQuotaTooLarge(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	_, err := buildQuotaPolicySnapshot(reg, QuotaPolicyInput{
		DefaultQuota: MaxLaneQuota + 1,
	})
	if !errors.Is(err, ErrQuotaTooLarge) {
		t.Errorf("err = %v, want ErrQuotaTooLarge", err)
	}
}

func TestLowQuotaLaneStillMakesProgress(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"large": 10, "small": 1})
	s, _ := NewScheduler(1, 1, 100, reg)
	ctx := context.Background()

	var executedLarge, executedSmall int32
	largeID, _ := reg.Lookup("large")
	smallID, _ := reg.Lookup("small")

	for i := 0; i < 20; i++ {
		_, _, _ = s.Enqueue(InternalJob{LaneID: largeID, Run: func(ctx context.Context) error {
			atomic.AddInt32(&executedLarge, 1)
			return nil
		}})
	}
	for i := 0; i < 5; i++ {
		_, _, _ = s.Enqueue(InternalJob{LaneID: smallID, Run: func(ctx context.Context) error {
			atomic.AddInt32(&executedSmall, 1)
			return nil
		}})
	}

	s.processShard(ctx, 0)

	if atomic.LoadInt32(&executedLarge) != 10 {
		t.Errorf("executedLarge = %d, want 10 (large lane quota)", executedLarge)
	}
	if atomic.LoadInt32(&executedSmall) != 1 {
		t.Errorf("executedSmall = %d, want 1 (small lane still makes progress)", executedSmall)
	}
}

func TestHighQuotaLaneDrainsMoreThanLowQuotaLane(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"high": 5, "low": 1})
	s, _ := NewScheduler(1, 1, 100, reg)
	ctx := context.Background()

	highID, _ := reg.Lookup("high")
	lowID, _ := reg.Lookup("low")

	var executedHigh, executedLow int32
	for i := 0; i < 8; i++ {
		_, _, _ = s.Enqueue(InternalJob{LaneID: highID, Run: func(ctx context.Context) error {
			atomic.AddInt32(&executedHigh, 1)
			return nil
		}})
	}
	for i := 0; i < 8; i++ {
		_, _, _ = s.Enqueue(InternalJob{LaneID: lowID, Run: func(ctx context.Context) error {
			atomic.AddInt32(&executedLow, 1)
			return nil
		}})
	}

	s.processShard(ctx, 0)

	high := atomic.LoadInt32(&executedHigh)
	low := atomic.LoadInt32(&executedLow)
	if high != 5 {
		t.Errorf("executedHigh = %d, want 5 (high quota)", high)
	}
	if low != 1 {
		t.Errorf("executedLow = %d, want 1 (low quota)", low)
	}
	if high <= low {
		t.Errorf("high quota lane ran %d jobs, low quota lane ran %d; want high > low", high, low)
	}
}

func TestProcessShardUsesUpdatedQuotaOnNextCycle(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 2})
	s, _ := NewScheduler(1, 1, 10, reg)
	ctx := context.Background()

	var executed int32
	run := func(ctx context.Context) error {
		atomic.AddInt32(&executed, 1)
		return nil
	}

	for i := 0; i < 5; i++ {
		_, _, _ = s.Enqueue(InternalJob{LaneID: 0, Run: run})
	}

	s.processShard(ctx, 0)
	if atomic.LoadInt32(&executed) != 2 {
		t.Fatalf("executed = %d, want 2 (initial quota)", executed)
	}
	// Drain requeue signal so a second processShard call does not block on ReadyCh.
	select {
	case <-s.ReadyCh:
	default:
	}

	if _, err := s.UpdateQuotaPolicy(QuotaPolicyInput{
		DefaultQuota: 1,
		LaneQuotas:   map[string]uint32{"default": 1},
	}); err != nil {
		t.Fatal(err)
	}

	s.processShard(ctx, 0)
	if atomic.LoadInt32(&executed) != 3 {
		t.Fatalf("executed = %d, want 3 after quota lowered to 1", executed)
	}
}

func TestProcessShardFIFOPreservedAcrossQuotaUpdate(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)
	ctx := context.Background()

	var results []int
	for i := 0; i < 3; i++ {
		val := i
		_, _, _ = s.Enqueue(InternalJob{LaneID: 0, Run: func(ctx context.Context) error {
			results = append(results, val)
			return nil
		}})
	}

	s.processShard(ctx, 0)
	if _, err := s.UpdateQuotaPolicy(QuotaPolicyInput{DefaultQuota: 2}); err != nil {
		t.Fatal(err)
	}
	s.processShard(ctx, 0)

	if len(results) != 3 {
		t.Fatalf("len(results) = %d, want 3", len(results))
	}
	for i, v := range results {
		if v != i {
			t.Errorf("results[%d] = %d, want %d", i, v, i)
		}
	}
}

func TestUpdateQuotaPolicyRejectedWhenStopped(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)
	ctx := context.Background()
	_ = s.Start(ctx)
	_ = s.Stop(context.Background(), true)

	_, err := s.UpdateQuotaPolicy(QuotaPolicyInput{DefaultQuota: 2})
	if !errors.Is(err, ErrStopped) {
		t.Errorf("err = %v, want ErrStopped", err)
	}
}
