// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/haluan/go-keylane"
	"github.com/haluan/go-keylane/internal/core"
)

func findKeyForShard(t *testing.T, shardID int, shardCount int) string {
	t.Helper()
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("key-%d", i)
		hash := core.HashKey(key)
		if int(hash%uint64(shardCount)) == shardID {
			return key
		}
	}
	t.Fatalf("failed to find key routing to shard %d", shardID)
	return ""
}

func TestHotShardRanking(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       2,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	keyShard0 := findKeyForShard(t, 0, 2)
	keyShard1 := findKeyForShard(t, 1, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	block := make(chan struct{})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  keyShard0,
		Lane: "default",
		Run: func(ctx context.Context) error {
			<-block
			return nil
		},
	})
	time.Sleep(10 * time.Millisecond)

	for i := 0; i < 3; i++ {
		_ = q.Submit(context.Background(), keylane.Job{
			Key:  keyShard1,
			Lane: "default",
			Run:  func(ctx context.Context) error { return nil },
		})
	}

	snap := q.DebugSnapshot()
	if len(snap.HotShards) == 0 {
		t.Fatal("expected hot shards")
	}
	if snap.HotShards[0].ShardID != 1 {
		t.Errorf("hot shard = %d, want shard 1 with depth 3", snap.HotShards[0].ShardID)
	}
	if snap.HotShards[0].Depth != 3 {
		t.Errorf("hot shard depth = %d, want 3", snap.HotShards[0].Depth)
	}
}

func TestHotShardTieBreak(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       2,
		WorkerCount:      2,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	keyShard0 := findKeyForShard(t, 0, 2)
	keyShard1 := findKeyForShard(t, 1, 2)

	block0 := make(chan struct{})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  keyShard0,
		Lane: "default",
		Run: func(ctx context.Context) error {
			<-block0
			return nil
		},
	})

	block1 := make(chan struct{})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  keyShard1,
		Lane: "default",
		Run: func(ctx context.Context) error {
			<-block1
			return nil
		},
	})
	time.Sleep(20 * time.Millisecond)

	_ = q.Submit(context.Background(), keylane.Job{
		Key:  keyShard0,
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  keyShard1,
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	})

	snap := q.DebugSnapshot()
	if len(snap.HotShards) < 2 {
		t.Fatalf("expected at least 2 hot shard entries, got %d", len(snap.HotShards))
	}
	if snap.HotShards[0].Depth != snap.HotShards[1].Depth {
		t.Fatalf("expected equal depth for tie-break test, got %d and %d",
			snap.HotShards[0].Depth, snap.HotShards[1].Depth)
	}
	// Same depth: higher in-flight wins; both shards have 1 in-flight from blockers.
	// Shard with additional queued job still depth 1 each - inflight tie at 1.
	// Lower ShardID wins when depth and inflight equal.
	if snap.HotShards[0].ShardID != 0 {
		t.Errorf("tie-break hot shard = %d, want 0 (lower shard ID)", snap.HotShards[0].ShardID)
	}
}

func TestHotLaneRanking(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"laneA": 1, "laneB": 1},
	}
	q, _ := keylane.New(cfg)

	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "a",
		Lane: "laneA",
		Run:  func(ctx context.Context) error { return nil },
	})
	for i := 0; i < 4; i++ {
		_ = q.Submit(context.Background(), keylane.Job{
			Key:  "b",
			Lane: "laneB",
			Run:  func(ctx context.Context) error { return nil },
		})
	}

	snap := q.DebugSnapshot()
	if len(snap.HotLanes) == 0 {
		t.Fatal("expected hot lanes")
	}
	if snap.HotLanes[0].Name != "laneB" {
		t.Errorf("hot lane = %q, want laneB", snap.HotLanes[0].Name)
	}
	if snap.HotLanes[0].Depth != 4 {
		t.Errorf("hot lane depth = %d, want 4", snap.HotLanes[0].Depth)
	}
}

func TestHotLaneTieBreak(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"laneA": 1, "laneB": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	blockA := make(chan struct{})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "a-block",
		Lane: "laneA",
		Run: func(ctx context.Context) error {
			<-blockA
			return nil
		},
	})
	blockB := make(chan struct{})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "b-block",
		Lane: "laneB",
		Run: func(ctx context.Context) error {
			<-blockB
			return nil
		},
	})
	time.Sleep(15 * time.Millisecond)

	// Depth 1 on each lane behind blockers; laneA has lane ID 0.
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "a-q",
		Lane: "laneA",
		Run:  func(ctx context.Context) error { return nil },
	})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "b-q",
		Lane: "laneB",
		Run:  func(ctx context.Context) error { return nil },
	})

	snap := q.DebugSnapshot()
	if len(snap.HotLanes) < 2 {
		t.Fatalf("expected 2 hot lanes, got %d", len(snap.HotLanes))
	}
	if snap.HotLanes[0].Depth != snap.HotLanes[1].Depth {
		t.Fatalf("expected equal depth, got %d and %d", snap.HotLanes[0].Depth, snap.HotLanes[1].Depth)
	}
	if snap.HotLanes[0].LaneID != 0 {
		t.Errorf("tie-break hot lane ID = %d, want 0", snap.HotLanes[0].LaneID)
	}
}
