// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRaceConcurrentSubmit(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	const count = 50
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%d", i)
		go func() {
			defer wg.Done()
			_ = q.Submit(ctx, Job{
				Key:  key,
				Lane: "default",
				Run:  func(ctx context.Context) error { return nil },
			})
		}()
	}

	wg.Wait()
}

func TestRaceConcurrentTrySubmit(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	const count = 50
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%d", i)
		go func() {
			defer wg.Done()
			_ = q.TrySubmit(Job{
				Key:  key,
				Lane: "default",
				Run:  func(ctx context.Context) error { return nil },
			})
		}()
	}

	wg.Wait()
}

func TestRaceConcurrentSubmitValue(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	const count = 50
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%d", i)
		go func() {
			defer wg.Done()
			_, _ = SubmitValue(ctx, q, ValueJob[int]{
				Key:  key,
				Lane: "default",
				Run:  func(ctx context.Context) (int, error) { return 42, nil },
			})
		}()
	}

	wg.Wait()
}

func TestRaceConcurrentAwait(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	future, _ := SubmitValue(ctx, q, ValueJob[int]{
		Key:  "k",
		Lane: "default",
		Run:  func(ctx context.Context) (int, error) { return 100, nil },
	})

	const count = 50
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			val, err := future.Await(ctx)
			if err != nil || val != 100 {
				t.Errorf("Await got (%d, %v), want (100, nil)", val, err)
			}
		}()
	}

	wg.Wait()
}

func TestRaceConcurrentStop(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	const count = 10
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			_ = q.Stop(ctx)
		}()
	}

	wg.Wait()
}

func TestRaceConcurrentStats(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	const count = 50
	var wg sync.WaitGroup
	wg.Add(count * 2)

	// One set of goroutines enqueuing, another reading Stats
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%d", i)
		go func() {
			defer wg.Done()
			_ = q.Submit(ctx, Job{
				Key:  key,
				Lane: "default",
				Run:  func(ctx context.Context) error { return nil },
			})
		}()

		go func() {
			defer wg.Done()
			_ = q.Stats()
		}()
	}

	wg.Wait()
}

func TestRaceConcurrentStatsGCPressure(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	const count = 50
	var wg sync.WaitGroup
	wg.Add(count * 2)

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%d", i)
		go func() {
			defer wg.Done()
			_ = q.Submit(ctx, Job{
				Key:  key,
				Lane: "default",
				Run:  func(ctx context.Context) error { return nil },
			})
		}()

		go func() {
			defer wg.Done()
			_ = q.StatsGCPressure()
		}()
	}

	wg.Wait()
}

func TestRaceConcurrentDebugSnapshotAndPressure(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	const count = 50
	var wg sync.WaitGroup
	wg.Add(count * 3)

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%d", i)
		go func() {
			defer wg.Done()
			_ = q.Submit(ctx, Job{
				Key:  key,
				Lane: "default",
				Run:  func(ctx context.Context) error { return nil },
			})
		}()

		go func() {
			defer wg.Done()
			_ = q.DebugSnapshot()
		}()

		go func() {
			defer wg.Done()
			_ = q.Pressure()
		}()
	}

	wg.Wait()
}

func TestRaceConcurrentLowAllocationObservability(t *testing.T) {
	ctx := testTimeout(t)
	cfg := newTestConfig()
	cfg.Observability = LowAllocationObservabilityConfig()
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	const count = 50
	var wg sync.WaitGroup
	wg.Add(count * 5)

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%d", i)
		go func() {
			defer wg.Done()
			_ = q.Submit(ctx, Job{
				Key:  key,
				Lane: "default",
				Run:  func(ctx context.Context) error { return nil },
			})
		}()
		go func() {
			defer wg.Done()
			f, _ := SubmitValue(ctx, q, ValueJob[int]{
				Key:  key,
				Lane: "default",
				Run:  func(ctx context.Context) (int, error) { return 1, nil },
			})
			_, _ = f.Await(ctx)
		}()
		go func() {
			defer wg.Done()
			_ = q.StatsGCPressure()
		}()
		go func() {
			defer wg.Done()
			_ = q.DebugSnapshot()
		}()
		go func() {
			defer wg.Done()
			_ = q.Pressure()
		}()
	}

	wg.Wait()
}

func TestRaceConcurrentSubmitAndUpdateLaneQuota(t *testing.T) {
	ctx := testTimeout(t)
	q, err := New(Config{
		ShardCount: 2, WorkerCount: 2, QueueSizePerLane: 32,
		LaneQuotas: map[Lane]int{"default": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = q.Start(ctx)
	defer func() {
		stopCtx, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		_ = q.Stop(stopCtx)
	}()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = q.Submit(ctx, Job{Key: "k", Lane: "default", Run: func(context.Context) error { return nil }})
		}()
		go func() {
			defer wg.Done()
			_, _ = q.UpdateLaneQuota("default", 2)
		}()
	}
	wg.Wait()
}

func TestRaceConcurrentSubmitAndAdaptiveSnapshot(t *testing.T) {
	q := adaptiveQuotaTestQueue(t)
	ctx := testTimeout(t)
	_ = q.Start(ctx)
	defer func() {
		stopCtx, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		_ = q.Stop(stopCtx)
	}()

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = q.Submit(ctx, Job{Key: "k", Lane: "default", Run: func(context.Context) error { return nil }})
		}()
		go func() {
			defer wg.Done()
			_ = q.AdaptiveDebugSnapshot()
			_ = q.AdaptiveQuotaSnapshot()
		}()
	}
	wg.Wait()
}

func TestRaceConcurrentSubmitAndCheckOverload(t *testing.T) {
	ctx := testTimeout(t)
	q, err := New(Config{
		ShardCount: 2, WorkerCount: 2, QueueSizePerLane: 32,
		LaneQuotas:      map[Lane]int{"default": 2, "best_effort": 1},
		OverloadEnabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{Class: LaneNormal, RejectAboveRatio: 0.90, MaxQueueDepth: 100},
	})
	_ = q.Start(ctx)
	defer func() {
		stopCtx, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		_ = q.Stop(stopCtx)
	}()

	var wg sync.WaitGroup
	for i := 0; i < 25; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = q.Submit(ctx, Job{Key: "k", Lane: "default", Run: func(context.Context) error { return nil }})
		}()
		go func() {
			defer wg.Done()
			_ = CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "best_effort"})
		}()
	}
	wg.Wait()
}
