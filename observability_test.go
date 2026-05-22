// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haluan/go-keylane"
)

// ==========================================
// 1. Stats Tests
// ==========================================

func TestStatsEmptyQueue(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       2,
		WorkerCount:      1,
		QueueSizePerLane: 5,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	stats := q.Stats()
	if stats.ShardCount != 2 {
		t.Errorf("expected 2 shards, got %d", stats.ShardCount)
	}
	if stats.TotalDepth != 0 {
		t.Errorf("expected total depth 0, got %d", stats.TotalDepth)
	}
}

func TestStatsTotalDepth(t *testing.T) {
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

	blockChan := make(chan struct{})
	defer close(blockChan)

	// Block worker
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			<-blockChan
			return nil
		},
	})
	time.Sleep(10 * time.Millisecond)

	// Add 3 jobs to queue
	for i := 0; i < 3; i++ {
		_ = q.Submit(context.Background(), keylane.Job{
			Key:  "key",
			Lane: "default",
			Run:  func(ctx context.Context) error { return nil },
		})
	}

	stats := q.Stats()
	if stats.TotalDepth != 3 {
		t.Errorf("expected total depth 3, got %d", stats.TotalDepth)
	}
}

func TestStatsShardDepth(t *testing.T) {
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

	blockChan := make(chan struct{})
	defer close(blockChan)

	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			<-blockChan
			return nil
		},
	})
	time.Sleep(10 * time.Millisecond)

	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	})

	stats := q.Stats()
	if stats.Shards[0].TotalDepth != 1 {
		t.Errorf("expected shard 0 depth 1, got %d", stats.Shards[0].TotalDepth)
	}
}

func TestStatsLaneDepth(t *testing.T) {
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

	blockChan := make(chan struct{})
	defer close(blockChan)

	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			<-blockChan
			return nil
		},
	})
	time.Sleep(10 * time.Millisecond)

	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	})

	stats := q.Stats()
	if stats.Shards[0].Lanes[0].Depth != 1 {
		t.Errorf("expected lane 'default' depth 1, got %d", stats.Shards[0].Lanes[0].Depth)
	}
}

func TestStatsIncludesLaneQuota(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 3},
	}
	q, _ := keylane.New(cfg)

	stats := q.Stats()
	if stats.Shards[0].Lanes[0].Quota != 3 {
		t.Errorf("expected lane quota 3, got %d", stats.Shards[0].Lanes[0].Quota)
	}
}

func TestStatsIncludesLaneCapacity(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 7,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	stats := q.Stats()
	if stats.Shards[0].Lanes[0].Capacity != 7 {
		t.Errorf("expected lane capacity 7, got %d", stats.Shards[0].Lanes[0].Capacity)
	}
}

func TestStatsSafeWhileWorkersRunning(t *testing.T) {
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
	// Concurrently submit jobs and read stats
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = q.Submit(context.Background(), keylane.Job{
				Key:  "key",
				Lane: "default",
				Run:  func(ctx context.Context) error { return nil },
			})
			time.Sleep(1 * time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = q.Stats()
			time.Sleep(1 * time.Millisecond)
		}
	}()

	wg.Wait()
}

func TestStatsDoesNotExposeInternalMutableState(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	stats1 := q.Stats()
	stats1.Shards[0].Lanes[0].Depth = 999

	stats2 := q.Stats()
	if stats2.Shards[0].Lanes[0].Depth != 0 {
		t.Error("Stats() leaked internal mutable array references!")
	}
}

// ==========================================
// 2. Lane Counters Tests
// ==========================================

func TestLaneCountersSubmittedTotal(t *testing.T) {
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

	stats := q.Stats()
	if stats.Shards[0].Lanes[0].SubmittedTotal != 1 {
		t.Errorf("expected SubmittedTotal 1, got %d", stats.Shards[0].Lanes[0].SubmittedTotal)
	}
}

func TestLaneCountersCompletedTotal(t *testing.T) {
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

	jobRunChan := make(chan struct{})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			close(jobRunChan)
			return nil
		},
	})

	<-jobRunChan
	time.Sleep(5 * time.Millisecond)

	stats := q.Stats()
	if stats.Shards[0].Lanes[0].CompletedTotal != 1 {
		t.Errorf("expected CompletedTotal 1, got %d", stats.Shards[0].Lanes[0].CompletedTotal)
	}
}

func TestLaneCountersFailedTotal(t *testing.T) {
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

	jobRunChan := make(chan struct{})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			close(jobRunChan)
			return errors.New("failed")
		},
	})

	<-jobRunChan
	time.Sleep(5 * time.Millisecond)

	stats := q.Stats()
	if stats.Shards[0].Lanes[0].FailedTotal != 1 {
		t.Errorf("expected FailedTotal 1, got %d", stats.Shards[0].Lanes[0].FailedTotal)
	}
}

func TestLaneCountersQueueFullTotal(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 1,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	blockChan := make(chan struct{})
	defer close(blockChan)

	// Block worker
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			<-blockChan
			return nil
		},
	})
	time.Sleep(10 * time.Millisecond)

	// Fill queue
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	})

	// This should fail with QueueFull
	err := q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	})
	if !errors.Is(err, keylane.ErrQueueFull) {
		t.Errorf("expected ErrQueueFull, got %v", err)
	}

	stats := q.Stats()
	if stats.Shards[0].Lanes[0].QueueFullTotal != 1 {
		t.Errorf("expected QueueFullTotal 1, got %d", stats.Shards[0].Lanes[0].QueueFullTotal)
	}
}

func TestLaneCountersArePerLane(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas: map[keylane.Lane]int{
			"laneA": 1,
			"laneB": 1,
		},
	}
	q, _ := keylane.New(cfg)

	_ = q.Submit(context.Background(), keylane.Job{Key: "k", Lane: "laneA", Run: func(ctx context.Context) error { return nil }})

	stats := q.Stats()
	var statsA, statsB keylane.LaneStats
	for _, l := range stats.Shards[0].Lanes {
		if l.Lane == "laneA" {
			statsA = l
		} else if l.Lane == "laneB" {
			statsB = l
		}
	}

	if statsA.SubmittedTotal != 1 {
		t.Errorf("expected laneA SubmittedTotal 1, got %d", statsA.SubmittedTotal)
	}
	if statsB.SubmittedTotal != 0 {
		t.Errorf("expected laneB SubmittedTotal 0, got %d", statsB.SubmittedTotal)
	}
}

func TestLaneCountersConcurrentWorkers(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       4,
		WorkerCount:      4,
		QueueSizePerLane: 100,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = q.Submit(context.Background(), keylane.Job{
				Key:  "key",
				Lane: "default",
				Run:  func(ctx context.Context) error { return nil },
			})
		}()
	}

	wg.Wait()
	time.Sleep(15 * time.Millisecond)

	stats := q.Stats()
	totalSubmitted := stats.Shards[0].Lanes[0].SubmittedTotal
	totalCompleted := stats.Shards[0].Lanes[0].CompletedTotal

	if totalSubmitted != 50 {
		t.Errorf("expected total submitted 50, got %d", totalSubmitted)
	}
	if totalCompleted != 50 {
		t.Errorf("expected total completed 50, got %d", totalCompleted)
	}
}

// ==========================================
// 3. Queue Wait Tests
// ==========================================

func TestQueueWaitTrackingEnabled(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
		Observability: keylane.ObservabilityConfig{
			TrackQueueWait: true,
		},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	jobRunChan := make(chan struct{})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			close(jobRunChan)
			return nil
		},
	})

	<-jobRunChan
	time.Sleep(5 * time.Millisecond)

	stats := q.Stats()
	lane := stats.Shards[0].Lanes[0]

	if lane.QueueWaitCount != 1 {
		t.Errorf("expected QueueWaitCount 1, got %d", lane.QueueWaitCount)
	}
	if lane.QueueWaitTotalNanos <= 0 {
		t.Errorf("expected QueueWaitTotalNanos > 0, got %d", lane.QueueWaitTotalNanos)
	}
}

func TestQueueWaitTrackingDisabled(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
		Observability: keylane.ObservabilityConfig{
			TrackQueueWait: false, // Disabled!
		},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	jobRunChan := make(chan struct{})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			close(jobRunChan)
			return nil
		},
	})

	<-jobRunChan
	time.Sleep(5 * time.Millisecond)

	stats := q.Stats()
	lane := stats.Shards[0].Lanes[0]

	if lane.QueueWaitCount != 0 {
		t.Errorf("expected QueueWaitCount 0, got %d", lane.QueueWaitCount)
	}
	if lane.QueueWaitTotalNanos != 0 {
		t.Errorf("expected QueueWaitTotalNanos 0, got %d", lane.QueueWaitTotalNanos)
	}
}

func TestQueueWaitCountIncrements(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
		Observability: keylane.ObservabilityConfig{
			TrackQueueWait: true,
		},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	var runCount int32
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		_ = q.Submit(context.Background(), keylane.Job{
			Key:  "key",
			Lane: "default",
			Run: func(ctx context.Context) error {
				atomic.AddInt32(&runCount, 1)
				wg.Done()
				return nil
			},
		})
	}

	wg.Wait()
	time.Sleep(5 * time.Millisecond)

	stats := q.Stats()
	lane := stats.Shards[0].Lanes[0]

	if lane.QueueWaitCount != 3 {
		t.Errorf("expected QueueWaitCount 3, got %d", lane.QueueWaitCount)
	}
}

func TestQueueWaitTotalIncrements(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
		Observability: keylane.ObservabilityConfig{
			TrackQueueWait: true,
		},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Fill queue before starting workers to accumulate wait time
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	})

	time.Sleep(10 * time.Millisecond)

	jobRunChan := make(chan struct{})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			close(jobRunChan)
			return nil
		},
	})

	_ = q.Start(ctx)

	<-jobRunChan
	time.Sleep(5 * time.Millisecond)

	stats := q.Stats()
	lane := stats.Shards[0].Lanes[0]

	if lane.QueueWaitTotalNanos < int64(10*time.Millisecond) {
		t.Errorf("expected QueueWaitTotalNanos to show significant wait latency (>10ms), got %d", lane.QueueWaitTotalNanos)
	}
}

func TestQueueWaitIsPerLane(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas: map[keylane.Lane]int{
			"laneA": 1,
			"laneB": 1,
		},
		Observability: keylane.ObservabilityConfig{
			TrackQueueWait: true,
		},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	jobRunChan := make(chan struct{})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "laneA",
		Run: func(ctx context.Context) error {
			close(jobRunChan)
			return nil
		},
	})

	<-jobRunChan
	time.Sleep(5 * time.Millisecond)

	stats := q.Stats()
	var statsA, statsB keylane.LaneStats
	for _, l := range stats.Shards[0].Lanes {
		if l.Lane == "laneA" {
			statsA = l
		} else if l.Lane == "laneB" {
			statsB = l
		}
	}

	if statsA.QueueWaitCount != 1 {
		t.Errorf("expected laneA QueueWaitCount 1, got %d", statsA.QueueWaitCount)
	}
	if statsB.QueueWaitCount != 0 {
		t.Errorf("expected laneB QueueWaitCount 0, got %d", statsB.QueueWaitCount)
	}
}

func TestAverageQueueWait(t *testing.T) {
	lane := keylane.LaneStats{
		QueueWaitTotalNanos: int64(10 * time.Millisecond),
		QueueWaitCount:      2,
	}

	avg := lane.AverageQueueWait()
	if avg != 5*time.Millisecond {
		t.Errorf("expected 5ms, got %v", avg)
	}

	laneZero := keylane.LaneStats{
		QueueWaitTotalNanos: 0,
		QueueWaitCount:      0,
	}
	if laneZero.AverageQueueWait() != 0 {
		t.Errorf("expected 0 average queue wait, got %v", laneZero.AverageQueueWait())
	}
}

// ==========================================
// 4. Slow Job Hook Tests
// ==========================================

func TestSlowJobHookCalled(t *testing.T) {
	var slowJobCount int64

	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
		Observability: keylane.ObservabilityConfig{
			SlowJobThreshold: 5 * time.Millisecond,
			Hooks: keylane.Hooks{
				OnSlowJob: func(ev keylane.SlowJobEvent) {
					atomic.AddInt64(&slowJobCount, 1)
				},
			},
		},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	jobRunChan := make(chan struct{})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			close(jobRunChan)
			return nil
		},
	})

	<-jobRunChan
	time.Sleep(10 * time.Millisecond)

	if atomic.LoadInt64(&slowJobCount) != 1 {
		t.Errorf("expected OnSlowJob to be called exactly 1 time, got %d", atomic.LoadInt64(&slowJobCount))
	}
}

func TestSlowJobHookNotCalledBelowThreshold(t *testing.T) {
	var slowJobCount int64

	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
		Observability: keylane.ObservabilityConfig{
			SlowJobThreshold: 50 * time.Millisecond, // very high threshold
			Hooks: keylane.Hooks{
				OnSlowJob: func(ev keylane.SlowJobEvent) {
					atomic.AddInt64(&slowJobCount, 1)
				},
			},
		},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	jobRunChan := make(chan struct{})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			// runs fast
			close(jobRunChan)
			return nil
		},
	})

	<-jobRunChan
	time.Sleep(5 * time.Millisecond)

	if atomic.LoadInt64(&slowJobCount) != 0 {
		t.Errorf("expected OnSlowJob not to be called, got %d", atomic.LoadInt64(&slowJobCount))
	}
}

func TestSlowJobHookDisabledWhenThresholdZero(t *testing.T) {
	var slowJobCount int64

	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
		Observability: keylane.ObservabilityConfig{
			SlowJobThreshold: 0, // Disabled!
			Hooks: keylane.Hooks{
				OnSlowJob: func(ev keylane.SlowJobEvent) {
					atomic.AddInt64(&slowJobCount, 1)
				},
			},
		},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	jobRunChan := make(chan struct{})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			close(jobRunChan)
			return nil
		},
	})

	<-jobRunChan
	time.Sleep(10 * time.Millisecond)

	if atomic.LoadInt64(&slowJobCount) != 0 {
		t.Errorf("expected OnSlowJob not to be called since threshold is 0, got %d", atomic.LoadInt64(&slowJobCount))
	}
}

func TestSlowJobHookNilSafe(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
		Observability: keylane.ObservabilityConfig{
			SlowJobThreshold: 5 * time.Millisecond,
			Hooks:            keylane.Hooks{OnSlowJob: nil}, // Nil hook!
		},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	jobRunChan := make(chan struct{})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			close(jobRunChan)
			return nil
		},
	})

	<-jobRunChan
	// Should finish without panics
}

func TestSlowJobEventFields(t *testing.T) {
	var capturedEvent keylane.SlowJobEvent
	jobDone := make(chan struct{})

	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
		Observability: keylane.ObservabilityConfig{
			SlowJobThreshold: 5 * time.Millisecond,
			Hooks: keylane.Hooks{
				OnSlowJob: func(ev keylane.SlowJobEvent) {
					capturedEvent = ev
					close(jobDone)
				},
			},
		},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		},
	})

	<-jobDone
	if capturedEvent.Lane != "default" {
		t.Errorf("expected lane 'default', got %q", capturedEvent.Lane)
	}
	if capturedEvent.ShardID != 0 {
		t.Errorf("expected ShardID 0, got %d", capturedEvent.ShardID)
	}
	if capturedEvent.RunDuration < 5*time.Millisecond {
		t.Errorf("expected RunDuration >= 5ms, got %v", capturedEvent.RunDuration)
	}
	if capturedEvent.Threshold != 5*time.Millisecond {
		t.Errorf("expected Threshold 5ms, got %v", capturedEvent.Threshold)
	}
	if capturedEvent.Outcome != keylane.JobOutcomeCompleted {
		t.Errorf("expected Outcome Completed, got %v", capturedEvent.Outcome)
	}
	if capturedEvent.LaneID != 0 {
		t.Errorf("expected LaneID 0, got %d", capturedEvent.LaneID)
	}
}

func TestSlowJobHookNotCalledInsideShardLock(t *testing.T) {
	var statsAcquiredDuringHook bool
	jobDone := make(chan struct{})

	var q *keylane.Queue
	var err error

	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
		Observability: keylane.ObservabilityConfig{
			SlowJobThreshold: 5 * time.Millisecond,
			Hooks: keylane.Hooks{
				OnSlowJob: func(ev keylane.SlowJobEvent) {
					// To verify we are not holding the shard locks, we should be able to call q.Stats() successfully
					// without deadlock. If the shard lock was held, q.Stats() would block/deadlock forever because
					// q.Stats() acquires shard.mu.Lock().
					stats := q.Stats()
					if len(stats.Shards) == 1 {
						statsAcquiredDuringHook = true
					}
					close(jobDone)
				},
			},
		},
	}
	q, err = keylane.New(cfg)
	if err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		},
	})

	select {
	case <-jobDone:
		if !statsAcquiredDuringHook {
			t.Error("failed to retrieve stats during slow job hook execution")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Deadlocked! Hook executed inside shard lock, causing reentrant stats retrieval to block.")
	}
}

func TestJobTimingHookCalled(t *testing.T) {
	var timingCount int64
	timingDone := make(chan keylane.JobTimingEvent, 2)

	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
		Observability: keylane.ObservabilityConfig{
			Hooks: keylane.Hooks{
				OnJobTiming: func(ev keylane.JobTimingEvent) {
					atomic.AddInt64(&timingCount, 1)
					select {
					case timingDone <- ev:
					default:
					}
				},
			},
		},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	blockA := make(chan struct{})
	startedA := make(chan struct{})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key-a",
		Lane: "default",
		Run: func(ctx context.Context) error {
			close(startedA)
			<-blockA
			return nil
		},
	})
	<-startedA
	time.Sleep(25 * time.Millisecond)

	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key-b",
		Lane: "default",
		Run: func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		},
	})

	if q.StatsGCPressure().TotalQueued == 0 {
		t.Fatal("expected job B queued behind A before unblocking")
	}
	time.Sleep(25 * time.Millisecond)

	close(blockA)

	var captured keylane.JobTimingEvent
	var foundQueued bool
	deadline := time.After(500 * time.Millisecond)
	for i := 0; i < 2; i++ {
		select {
		case ev := <-timingDone:
			if ev.QueueWait >= 10*time.Millisecond {
				captured = ev
				foundQueued = true
			}
		case <-deadline:
			t.Fatal("OnJobTiming not called twice before timeout")
		}
	}
	if !foundQueued {
		t.Fatal("expected OnJobTiming event with non-zero QueueWait for job queued behind a blocker")
	}

	if atomic.LoadInt64(&timingCount) != 2 {
		t.Fatalf("OnJobTiming calls = %d, want 2", timingCount)
	}
	if captured.Outcome != keylane.JobOutcomeCompleted {
		t.Errorf("Outcome = %v, want Completed", captured.Outcome)
	}
	if captured.RunDuration < 5*time.Millisecond {
		t.Errorf("RunDuration = %v, want >= 5ms", captured.RunDuration)
	}
	if captured.QueueWait < 10*time.Millisecond {
		t.Errorf("QueueWait = %v, want >= 10ms", captured.QueueWait)
	}
	if captured.Lane != "default" {
		t.Errorf("Lane = %q, want default", captured.Lane)
	}
	if captured.LaneID != 0 {
		t.Errorf("LaneID = %d, want 0", captured.LaneID)
	}
	if captured.ShardID != 0 {
		t.Errorf("ShardID = %d, want 0", captured.ShardID)
	}
}

func TestJobTimingHookNilSafe(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
		Observability: keylane.ObservabilityConfig{
			Hooks: keylane.Hooks{OnJobTiming: nil},
		},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	done := make(chan struct{})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run:  func(ctx context.Context) error { close(done); return nil },
	})
	<-done
}

func TestJobTimingHookCanceledOutcome(t *testing.T) {
	timingDone := make(chan keylane.JobTimingEvent, 1)

	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
		Observability: keylane.ObservabilityConfig{
			Hooks: keylane.Hooks{
				OnJobTiming: func(ev keylane.JobTimingEvent) {
					select {
					case timingDone <- ev:
					default:
					}
				},
			},
		},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return context.Canceled },
	})

	var captured keylane.JobTimingEvent
	select {
	case captured = <-timingDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("OnJobTiming not called")
	}

	if captured.Outcome != keylane.JobOutcomeCanceled {
		t.Errorf("Outcome = %v, want Canceled", captured.Outcome)
	}
}

func TestJobTimingHookFailedOutcome(t *testing.T) {
	timingDone := make(chan keylane.JobTimingEvent, 1)

	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
		Observability: keylane.ObservabilityConfig{
			Hooks: keylane.Hooks{
				OnJobTiming: func(ev keylane.JobTimingEvent) {
					select {
					case timingDone <- ev:
					default:
					}
				},
			},
		},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return errors.New("fail") },
	})

	var captured keylane.JobTimingEvent
	select {
	case captured = <-timingDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("OnJobTiming not called")
	}

	if captured.Outcome != keylane.JobOutcomeFailed {
		t.Errorf("Outcome = %v, want Failed", captured.Outcome)
	}
}

func TestHookPanicDoesNotKillWorker(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
		Observability: keylane.ObservabilityConfig{
			SlowJobThreshold: time.Millisecond,
			Hooks: keylane.Hooks{
				OnJobTiming: func(ev keylane.JobTimingEvent) {
					panic("observer panic")
				},
			},
		},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	for i := 0; i < 3; i++ {
		done := make(chan struct{})
		_ = q.Submit(context.Background(), keylane.Job{
			Key:  "key",
			Lane: "default",
			Run: func(ctx context.Context) error {
				time.Sleep(2 * time.Millisecond)
				close(done)
				return nil
			},
		})
		<-done
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = q.Stop(stopCtx, keylane.WithDrain(true))

	if q.StatsGCPressure().Run.Count != 3 {
		t.Errorf("Run.Count = %d, want 3 after hook panics", q.StatsGCPressure().Run.Count)
	}
}
