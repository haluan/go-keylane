package keylane

import (
	"context"
	"sync"
	"testing"
)

func TestNoisyLaneDoesNotStarvePayment(t *testing.T) {
	ctx := testTimeout(t)

	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 100,
		LaneQuotas: map[Lane]int{
			"payment": 2,
			"webhook": 1,
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

	// Enqueue 50 noisy webhook jobs
	for i := 0; i < 50; i++ {
		_ = q.Submit(ctx, Job{
			Key:  "shared-key",
			Lane: "webhook",
			Run:  runJob("webhook"),
		})
	}

	// Enqueue 2 quiet payment jobs
	for i := 0; i < 2; i++ {
		_ = q.Submit(ctx, Job{
			Key:  "shared-key",
			Lane: "payment",
			Run:  runJob("payment"),
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

	if len(order) != 52 {
		t.Fatalf("expected 52 executed, got %d", len(order))
	}

	// Find the positions of the 2 payment jobs.
	// Since alphabetical order places "payment" (ID 0) before "webhook" (ID 1),
	// Pass 1: 2 payment, 1 webhook
	// Pass 2: 1 webhook (as payment is now empty)
	// So payment jobs should be at index 0 and 1!
	if order[0] != "payment" || order[1] != "payment" {
		t.Errorf("expected payment jobs at index 0 and 1, got order: %v", order[:10])
	}
}

func TestNoisyLaneStillLeavesOtherLaneProgress(t *testing.T) {
	ctx := testTimeout(t)

	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 100,
		LaneQuotas: map[Lane]int{
			"payment": 1,
			"webhook": 1,
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

	// Enqueue 10 noisy webhook jobs
	for i := 0; i < 10; i++ {
		_ = q.Submit(ctx, Job{
			Key:  "shared-key",
			Lane: "webhook",
			Run:  runJob("webhook"),
		})
	}

	// Enqueue 1 quiet payment job
	_ = q.Submit(ctx, Job{
		Key:  "shared-key",
		Lane: "payment",
		Run:  runJob("payment"),
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

	// "payment" (ID 0) and "webhook" (ID 1)
	// Pass 1: 1 payment, 1 webhook
	if order[0] != "payment" {
		t.Errorf("expected first job to be payment, got: %v", order)
	}
}

func TestNoisyLaneWithQuotaOne(t *testing.T) {
	ctx := testTimeout(t)

	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 100,
		LaneQuotas: map[Lane]int{
			"laneA": 1,
			"laneB": 1,
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

	// Enqueue 5 noisy laneB jobs
	for i := 0; i < 5; i++ {
		_ = q.Submit(ctx, Job{
			Key:  "shared-key",
			Lane: "laneB",
			Run:  runJob("laneB"),
		})
	}

	// Enqueue 1 quiet laneA job
	_ = q.Submit(ctx, Job{
		Key:  "shared-key",
		Lane: "laneA",
		Run:  runJob("laneA"),
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

	// Alphabetical order: "laneA" (ID 0) before "laneB" (ID 1)
	if order[0] != "laneA" {
		t.Errorf("expected first job to be laneA, got: %v", order)
	}
}
