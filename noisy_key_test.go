package keylane

import (
	"context"
	"sync"
	"testing"
)

func TestFindKeysForDifferentShards(t *testing.T) {
	shardCount := 4
	key0 := findKeyForShardPublic(t, 0, shardCount)
	key1 := findKeyForShardPublic(t, 1, shardCount)
	key2 := findKeyForShardPublic(t, 2, shardCount)
	key3 := findKeyForShardPublic(t, 3, shardCount)

	if key0 == "" || key1 == "" || key2 == "" || key3 == "" {
		t.Fatal("failed to find keys for all shards")
	}
}

func TestNoisyKeyIsolationDifferentShards(t *testing.T) {
	ctx := testTimeout(t)

	cfg := Config{
		ShardCount:       2,
		WorkerCount:      1, // Single worker to show round-robin scheduling isolation
		QueueSizePerLane: 100,
		LaneQuotas: map[Lane]int{
			"default": 1, // Only 1 job per pass to make round-robin visible
		},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	keyA := findKeyForShardPublic(t, 0, 2)
	keyB := findKeyForShardPublic(t, 1, 2)

	var mu sync.Mutex
	var order []string

	wg := sync.WaitGroup{}
	runJob := func(name string) func(context.Context) error {
		wg.Add(1)
		return func(ctx context.Context) error {
			defer wg.Done()
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return nil
		}
	}

	// Enqueue 20 jobs to Shard 0 (keyA)
	for i := 0; i < 20; i++ {
		_ = q.Submit(ctx, Job{
			Key:  keyA,
			Lane: "default",
			Run:  runJob("noisy"),
		})
	}

	// Enqueue 1 job to Shard 1 (keyB)
	_ = q.Submit(ctx, Job{
		Key:  keyB,
		Lane: "default",
		Run:  runJob("quiet"),
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

	if len(order) != 21 {
		t.Fatalf("expected 21 executed, got %d", len(order))
	}

	// Because of round-robin shard scheduling:
	// Pass 1: Shard 0 executes 1 "noisy" job. Shard 0 is requeued.
	// Pass 2: Shard 1 executes 1 "quiet" job.
	// So "quiet" must execute at index 1!
	if order[1] != "quiet" {
		t.Errorf("expected quiet job to execute at index 1, got order: %v", order[:5])
	}
}

func TestNoisyKeyDoesNotBlockQuietKey(t *testing.T) {
	ctx := testTimeout(t)

	cfg := Config{
		ShardCount:       4,
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

	keyA := findKeyForShardPublic(t, 0, 4)
	keyB := findKeyForShardPublic(t, 2, 4)

	var mu sync.Mutex
	var order []string

	wg := sync.WaitGroup{}
	runJob := func(name string) func(context.Context) error {
		wg.Add(1)
		return func(ctx context.Context) error {
			defer wg.Done()
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return nil
		}
	}

	for i := 0; i < 10; i++ {
		_ = q.Submit(ctx, Job{
			Key:  keyA,
			Lane: "default",
			Run:  runJob("noisy"),
		})
	}

	_ = q.Submit(ctx, Job{
		Key:  keyB,
		Lane: "default",
		Run:  runJob("quiet"),
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

	// Quiet job must be executed within the first few spots
	foundQuiet := false
	for _, name := range order[:3] {
		if name == "quiet" {
			foundQuiet = true
			break
		}
	}
	if !foundQuiet {
		t.Errorf("expected quiet job in top 3 of execution order, got order: %v", order)
	}
}
