package keylane

import (
	"context"
	"fmt"
	"testing"

	"github.com/haluan/go-keylane/internal/core"
)

// helper to find key public-side
func findKeyForShardPublic(t *testing.T, shardID int, shardCount int) string {
	t.Helper()
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("key-%d", i)
		hash := core.HashKey(key)
		if int(hash%uint64(shardCount)) == shardID {
			return key
		}
	}
	t.Fatalf("failed to find key")
	return ""
}

func TestShardRoutingSameKeySameShard(t *testing.T) {
	ctx := testTimeout(t)
	cfg := newTestConfig()
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	key := "test-key"
	job := Job{
		Key:  key,
		Lane: "default",
		Run: func(ctx context.Context) error {
			return nil
		},
	}

	err1 := q.Submit(ctx, job)
	err2 := q.Submit(ctx, job)
	if err1 != nil || err2 != nil {
		t.Fatalf("unexpected submit errors: %v, %v", err1, err2)
	}

	stats := q.Stats()
	var activeShardID = -1
	for _, ss := range stats.Shards {
		if ss.TotalDepth > 0 {
			if activeShardID == -1 {
				activeShardID = ss.ShardID
			} else if activeShardID != ss.ShardID {
				t.Fatalf("jobs with same key routed to different shards: %d and %d", activeShardID, ss.ShardID)
			}
		}
	}
}

func TestShardRoutingIDWithinRange(t *testing.T) {
	ctx := testTimeout(t)
	cfg := newTestConfig()
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	job := Job{
		Key:  "my-key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	}

	if err := q.Submit(ctx, job); err != nil {
		t.Fatalf("failed to submit job: %v", err)
	}

	stats := q.Stats()
	shardCount := q.config.ShardCount

	found := false
	for _, ss := range stats.Shards {
		if ss.TotalDepth > 0 {
			found = true
			if ss.ShardID < 0 || ss.ShardID >= shardCount {
				t.Errorf("routed shard ID %d out of range [0, %d)", ss.ShardID, shardCount)
			}
		}
	}
	if !found {
		t.Error("expected to find at least one routed shard with non-zero depth")
	}
}

func TestShardRoutingShardCountOne(t *testing.T) {
	ctx := testTimeout(t)
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[Lane]int{"default": 1},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	for i := 0; i < 10; i++ {
		job := Job{
			Key:  "key-1",
			Lane: "default",
			Run:  func(ctx context.Context) error { return nil },
		}
		if err := q.Submit(ctx, job); err != nil {
			t.Fatalf("failed to submit job: %v", err)
		}
	}

	stats := q.Stats()
	if len(stats.Shards) != 1 {
		t.Fatalf("expected 1 shard in stats, got %d", len(stats.Shards))
	}
	if stats.Shards[0].ShardID != 0 {
		t.Errorf("expected shard ID 0, got %d", stats.Shards[0].ShardID)
	}
}

func TestShardRoutingDifferentShardCountsRemainValid(t *testing.T) {
	ctx := testTimeout(t)
	counts := []int{1, 2, 5, 8}

	for _, sc := range counts {
		cfg := Config{
			ShardCount:       sc,
			WorkerCount:      1,
			QueueSizePerLane: 10,
			LaneQuotas:       map[Lane]int{"default": 1},
		}
		q, err := New(cfg)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}

		job := Job{
			Key:  "some-key",
			Lane: "default",
			Run:  func(ctx context.Context) error { return nil },
		}
		if err := q.Submit(ctx, job); err != nil {
			t.Fatalf("failed to submit: %v", err)
		}

		stats := q.Stats()
		found := false
		for _, ss := range stats.Shards {
			if ss.TotalDepth > 0 {
				found = true
				if ss.ShardID < 0 || ss.ShardID >= sc {
					t.Errorf("with shardCount=%d, routed shard ID %d is out of range", sc, ss.ShardID)
				}
			}
		}
		if !found {
			t.Errorf("expected to find non-zero depth shard for shardCount=%d", sc)
		}
	}
}

func TestShardRoutingManyKeysWithinRange(t *testing.T) {
	ctx := testTimeout(t)
	cfg := newTestConfig()
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create: %v", err)
	}
	shardCount := q.config.ShardCount

	for i := 0; i < shardCount; i++ {
		key := findKeyForShardPublic(t, i, shardCount)
		job := Job{
			Key:  key,
			Lane: "default",
			Run:  func(ctx context.Context) error { return nil },
		}
		if err := q.Submit(ctx, job); err != nil {
			t.Fatalf("failed to submit job: %v", err)
		}
	}

	stats := q.Stats()
	activeCount := 0
	for _, ss := range stats.Shards {
		if ss.TotalDepth > 0 {
			activeCount++
			if ss.TotalDepth != 1 {
				t.Errorf("shard %d has depth %d, want 1", ss.ShardID, ss.TotalDepth)
			}
		}
	}
	if activeCount != shardCount {
		t.Errorf("expected %d active shards, got %d", shardCount, activeCount)
	}
}
