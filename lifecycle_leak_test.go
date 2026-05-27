// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestLifecycleStartStopIdleNoLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	cfg := Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 8,
		LaneQuotas: map[Lane]int{"default": 1},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := q.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := q.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	eventuallyNoGoroutineGrowth(t, before, 8)
}

func TestLifecycleQueueFullThenStopNoLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	cfg := Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 1,
		LaneQuotas: map[Lane]int{"default": 1},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := q.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	block := make(chan struct{})
	_ = q.Submit(ctx, Job{
		Key: "fill", Lane: "default",
		Run: func(context.Context) error { <-block; return nil },
	})
	for i := 0; i < 20; i++ {
		_ = q.Submit(ctx, Job{
			Key: "reject", Lane: "default",
			Run: func(context.Context) error { return nil },
		})
	}
	close(block)
	cancel()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = q.Stop(stopCtx, WithDrain(false))
	eventuallyNoGoroutineGrowth(t, before, 10)
}

func TestStopUnderQueuePressureNoLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	cfg := Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 4,
		LaneQuotas: map[Lane]int{"default": 1},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	if err := q.Start(runCtx); err != nil {
		runCancel()
		t.Fatal(err)
	}
	block := make(chan struct{})
	_ = q.Submit(runCtx, Job{
		Key: "block", Lane: "default",
		Run: func(context.Context) error { <-block; return nil },
	})
	for i := 0; i < 8; i++ {
		_ = q.Submit(runCtx, Job{
			Key: "q", Lane: "default",
			Run: func(context.Context) error { return nil },
		})
	}
	runCancel()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = q.Stop(stopCtx, WithDrain(false))
	close(block)
	eventuallyNoGoroutineGrowth(t, before, 12)
}

func TestAwaitAfterShutdownNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	cfg := Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 16,
		LaneQuotas: map[Lane]int{"default": 1},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	if err := q.Start(runCtx); err != nil {
		runCancel()
		t.Fatal(err)
	}
	future, err := SubmitValue(runCtx, q, ValueJob[int]{
		Key: "slow", Lane: "default",
		Run: func(runCtx context.Context) (int, error) {
			<-runCtx.Done()
			return 0, runCtx.Err()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runCancel()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	_ = q.Stop(stopCtx, WithDrain(false))

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			awaitCtx, c := context.WithTimeout(context.Background(), time.Second)
			defer c()
			_, _ = future.Await(awaitCtx)
		}()
	}
	wg.Wait()
	eventuallyNoGoroutineGrowth(t, before, 12)
}

func TestAwaitAfterShutdownReturnsStopped(t *testing.T) {
	cfg := Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 8,
		LaneQuotas: map[Lane]int{"default": 1},
	}
	q, _ := New(cfg)
	_ = q.Start(context.Background())
	_ = q.Stop(context.Background(), WithDrain(false))

	future, err := SubmitValue(context.Background(), q, ValueJob[int]{
		Key: "k", Lane: "default",
		Run: func(context.Context) (int, error) { return 1, nil },
	})
	if !errors.Is(err, ErrStopped) {
		t.Fatalf("submit err = %v", err)
	}
	_, awaitErr := future.Await(context.Background())
	if !errors.Is(awaitErr, ErrStopped) {
		t.Fatalf("await err = %v", awaitErr)
	}
}
