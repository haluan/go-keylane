// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/haluan/go-keylane"
)

func maxStatsGCPressureQueued(cfg keylane.Config) uint64 {
	laneCount := len(cfg.LaneQuotas)
	return uint64(cfg.ShardCount) * uint64(laneCount) * uint64(cfg.QueueSizePerLane)
}

func maxStatsGCPressureInFlight(cfg keylane.Config) uint64 {
	var totalQuota int
	for _, quota := range cfg.LaneQuotas {
		totalQuota += quota
	}
	return uint64(cfg.WorkerCount) * uint64(totalQuota)
}

func checkStatsGCPressureSane(cfg keylane.Config, snap keylane.StatsGCPressureSnapshot) error {
	maxQueued := maxStatsGCPressureQueued(cfg)
	maxInFlight := maxStatsGCPressureInFlight(cfg)

	if snap.TotalQueued > maxQueued {
		return errors.New("TotalQueued exceeds configured capacity")
	}
	if snap.TotalInFlight > maxInFlight {
		return errors.New("TotalInFlight exceeds worker * quota bound")
	}

	for _, shard := range snap.Shards {
		if shard.Queued > maxQueued {
			return errors.New("shard Queued exceeds configured capacity")
		}
		if shard.InFlight > maxInFlight {
			return errors.New("shard InFlight exceeds worker * quota bound")
		}
		for _, pl := range shard.PerLane {
			if pl.Queued > maxQueued {
				return errors.New("PerLane Queued exceeds configured capacity")
			}
		}
	}
	for _, lane := range snap.Lanes {
		if lane.Queued > maxQueued {
			return errors.New("lane Queued exceeds configured capacity")
		}
		if lane.InFlight > maxInFlight {
			return errors.New("lane InFlight exceeds worker * quota bound")
		}
	}
	return nil
}

func assertStatsGCPressureSane(t *testing.T, cfg keylane.Config, snap keylane.StatsGCPressureSnapshot) {
	t.Helper()
	if err := checkStatsGCPressureSane(cfg, snap); err != nil {
		t.Error(err)
	}
}

func TestStatsGCPressureEmptyQueue(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       2,
		WorkerCount:      3,
		QueueSizePerLane: 5,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	snap := q.StatsGCPressure()
	if snap.Version != keylane.StatsGCPressureVersion {
		t.Errorf("Version = %q, want %q", snap.Version, keylane.StatsGCPressureVersion)
	}
	if snap.ShardCount != 2 || snap.LaneCount != 1 || snap.WorkerCount != 3 {
		t.Errorf("counts: shards=%d lanes=%d workers=%d", snap.ShardCount, snap.LaneCount, snap.WorkerCount)
	}
	if snap.TotalQueued != 0 || snap.TotalInFlight != 0 {
		t.Errorf("expected zero totals, got queued=%d inflight=%d", snap.TotalQueued, snap.TotalInFlight)
	}
}

func TestStatsGCPressureQueuedJobs(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	block := make(chan struct{})
	defer close(block)
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			<-block
			return nil
		},
	})
	time.Sleep(10 * time.Millisecond)

	for i := 0; i < 2; i++ {
		_ = q.Submit(context.Background(), keylane.Job{
			Key:  "key",
			Lane: "default",
			Run:  func(ctx context.Context) error { return nil },
		})
	}

	snap := q.StatsGCPressure()
	if snap.TotalQueued != 2 {
		t.Errorf("TotalQueued = %d, want 2", snap.TotalQueued)
	}
	if snap.Shards[0].Queued != 2 {
		t.Errorf("shard queued = %d, want 2", snap.Shards[0].Queued)
	}
	if snap.Lanes[0].Queued != 2 {
		t.Errorf("lane queued = %d, want 2", snap.Lanes[0].Queued)
	}
}

func TestStatsGCPressureInFlightJobs(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	block := make(chan struct{})
	started := make(chan struct{})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			close(started)
			<-block
			return nil
		},
	})

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("job did not start")
	}

	snap := q.StatsGCPressure()
	if snap.TotalInFlight != 1 {
		t.Errorf("TotalInFlight = %d, want 1", snap.TotalInFlight)
	}
	if snap.Lanes[0].InFlight != 1 {
		t.Errorf("lane InFlight = %d, want 1", snap.Lanes[0].InFlight)
	}
	close(block)
}

func TestStatsGCPressureInFlightReturnsToZero(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	})

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := q.Stop(stopCtx, keylane.WithDrain(true)); err != nil {
		t.Fatalf("Stop with drain: %v", err)
	}

	snap := q.StatsGCPressure()
	if snap.TotalInFlight != 0 {
		t.Errorf("TotalInFlight = %d, want 0 after drain", snap.TotalInFlight)
	}
	if snap.TotalQueued != 0 {
		t.Errorf("TotalQueued = %d, want 0 after drain", snap.TotalQueued)
	}
}

func TestStatsGCPressureDoesNotExposeInternalMutableState(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	})

	snap1 := q.StatsGCPressure()
	snap1.Shards[0].PerLane[0].Queued = 999

	snap2 := q.StatsGCPressure()
	if snap2.Shards[0].PerLane[0].Queued != 1 {
		t.Error("StatsGCPressure leaked internal mutable array references")
	}
}

func TestStatsGCPressureQueueFullState(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 2,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	var lastErr error
	for i := 0; i < 5; i++ {
		lastErr = q.Submit(context.Background(), keylane.Job{
			Key:  "key",
			Lane: "default",
			Run:  func(ctx context.Context) error { return nil },
		})
	}

	if !errors.Is(lastErr, keylane.ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", lastErr)
	}

	snap := q.StatsGCPressure()
	assertStatsGCPressureSane(t, cfg, snap)
	if snap.TotalQueued > 2 {
		t.Errorf("TotalQueued = %d, want at most 2", snap.TotalQueued)
	}
}

func TestStatsGCPressureConcurrentSubmitRunAndRead(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       4,
		WorkerCount:      4,
		QueueSizePerLane: 50,
		LaneQuotas:       map[keylane.Lane]int{"default": 2},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	recordErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	wg.Add(3)

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = q.Submit(context.Background(), keylane.Job{
				Key:  "key",
				Lane: "default",
				Run:  func(ctx context.Context) error { return nil },
			})
			time.Sleep(time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			snap := q.StatsGCPressure()
			recordErr(checkStatsGCPressureSane(cfg, snap))
			time.Sleep(time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_ = q.Stats()
			snap := q.StatsGCPressure()
			recordErr(checkStatsGCPressureSane(cfg, snap))
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()
	if firstErr != nil {
		t.Fatal(firstErr)
	}
}
