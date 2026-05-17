package keylane

import (
	"context"
	"sync"
	"testing"
)

func TestLaneQuotaFairnessPaymentAudit(t *testing.T) {
	ctx := testTimeout(t)

	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 100,
		LaneQuotas: map[Lane]int{
			"audit":   1,
			"payment": 2,
		},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	var mu sync.Mutex
	var order []string

	done := make(chan struct{})
	var wg sync.WaitGroup

	runJob := func(lane string) func(context.Context) error {
		wg.Add(1)
		return func(ctx context.Context) error {
			defer wg.Done()
			mu.Lock()
			order = append(order, lane)
			mu.Unlock()
			return nil
		}
	}

	// Enqueue payment jobs
	for i := 0; i < 5; i++ {
		err := q.Submit(ctx, Job{
			Key:  "my-key",
			Lane: "payment",
			Run:  runJob("payment"),
		})
		if err != nil {
			t.Fatalf("submit error: %v", err)
		}
	}

	// Enqueue audit jobs
	for i := 0; i < 2; i++ {
		err := q.Submit(ctx, Job{
			Key:  "my-key",
			Lane: "audit",
			Run:  runJob("audit"),
		})
		if err != nil {
			t.Fatalf("submit error: %v", err)
		}
	}

	// Now start the worker queue to process them
	if err := q.Start(ctx); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	waitForSignal(t, done)

	mu.Lock()
	defer mu.Unlock()

	if len(order) != 7 {
		t.Fatalf("expected 7 jobs to execute, got %d: %v", len(order), order)
	}

	// First pass should execute: 1 audit, 2 payment
	// Second pass should execute: 1 audit, 2 payment
	// Third pass should execute: 1 payment
	// Expected ordering sequence:
	expected := []string{
		"audit", "payment", "payment",
		"audit", "payment", "payment",
		"payment",
	}

	for i, lane := range expected {
		if order[i] != lane {
			t.Errorf("at index %d: got lane %q, want %q. Complete execution order: %v", i, order[i], lane, order)
		}
	}
}

func TestLaneQuotaDoesNotDrainNoisyLaneInSinglePass(t *testing.T) {
	ctx := testTimeout(t)
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 100,
		LaneQuotas: map[Lane]int{
			"default": 1,
		},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	var mu sync.Mutex
	var order []int

	wg := sync.WaitGroup{}
	run := func(idx int) func(context.Context) error {
		wg.Add(1)
		return func(ctx context.Context) error {
			defer wg.Done()
			mu.Lock()
			order = append(order, idx)
			mu.Unlock()
			return nil
		}
	}

	for i := 0; i < 5; i++ {
		_ = q.Submit(ctx, Job{
			Key:  "my-key",
			Lane: "default",
			Run:  run(i),
		})
	}

	if err := q.Start(ctx); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	waitForSignal(t, done)

	mu.Lock()
	defer mu.Unlock()

	// Should execute all 5 sequentially because it requeues and worker executes next pass
	if len(order) != 5 {
		t.Fatalf("expected 5 executed, got %d", len(order))
	}
}

func TestLaneQuotaProcessesSmallQuotaLane(t *testing.T) {
	ctx := testTimeout(t)
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 100,
		LaneQuotas: map[Lane]int{
			"large": 10,
			"small": 1,
		},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	var mu sync.Mutex
	var order []string

	wg := sync.WaitGroup{}
	runJob := func(lane string) func(context.Context) error {
		wg.Add(1)
		return func(ctx context.Context) error {
			defer wg.Done()
			mu.Lock()
			order = append(order, lane)
			mu.Unlock()
			return nil
		}
	}

	for i := 0; i < 15; i++ {
		_ = q.Submit(ctx, Job{
			Key:  "my-key",
			Lane: "large",
			Run:  runJob("large"),
		})
	}
	_ = q.Submit(ctx, Job{
		Key:  "my-key",
		Lane: "small",
		Run:  runJob("small"),
	})

	if err := q.Start(ctx); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	waitForSignal(t, done)

	mu.Lock()
	defer mu.Unlock()

	if len(order) != 16 {
		t.Fatalf("expected 16 executed, got %d", len(order))
	}

	// Alphabetical order: "large" (ID 0) before "small" (ID 1)
	// Pass 1: 10 large, 1 small
	// Pass 2: 5 large
	expected := []string{
		"large", "large", "large", "large", "large", "large", "large", "large", "large", "large", "small",
		"large", "large", "large", "large", "large",
	}

	for i, lane := range expected {
		if order[i] != lane {
			t.Errorf("at index %d: got lane %q, want %q. Complete order: %v", i, order[i], lane, order)
		}
	}
}
