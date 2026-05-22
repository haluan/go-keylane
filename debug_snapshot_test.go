// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane_test

import (
	"context"
	"testing"
	"time"

	"github.com/haluan/go-keylane"
)

func TestDebugSnapshotEmptyScheduler(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       2,
		WorkerCount:      3,
		QueueSizePerLane: 5,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	snap := q.DebugSnapshot()
	if snap.Version != keylane.DebugSnapshotVersion {
		t.Errorf("Version = %q, want %q", snap.Version, keylane.DebugSnapshotVersion)
	}
	if snap.ShardCount != 2 || snap.LaneCount != 1 || snap.WorkerCount != 3 {
		t.Errorf("counts: shards=%d lanes=%d workers=%d", snap.ShardCount, snap.LaneCount, snap.WorkerCount)
	}
	if snap.TotalDepth != 0 || snap.TotalInFlight != 0 {
		t.Errorf("expected zero totals, got depth=%d inflight=%d", snap.TotalDepth, snap.TotalInFlight)
	}
	if len(snap.HotShards) != 0 || len(snap.HotLanes) != 0 {
		t.Error("expected empty hot lists")
	}
	if !snap.GeneratedAt.IsZero() && time.Since(snap.GeneratedAt) > time.Minute {
		t.Error("GeneratedAt looks stale")
	}
}

func TestDebugSnapshotTotals(t *testing.T) {
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

	snap := q.DebugSnapshot()
	if snap.TotalDepth != 2 {
		t.Errorf("TotalDepth = %d, want 2", snap.TotalDepth)
	}
	if snap.TotalCapacity != 10 {
		t.Errorf("TotalCapacity = %d, want 10", snap.TotalCapacity)
	}
	if snap.Pressure.TotalDepth != snap.TotalDepth {
		t.Errorf("embedded Pressure depth = %d, want %d", snap.Pressure.TotalDepth, snap.TotalDepth)
	}
}

func TestDebugSnapshotShardDepths(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       2,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "shard0-key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "shard1-key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	})

	snap := q.DebugSnapshot()
	if len(snap.Shards) != 2 {
		t.Fatalf("len(Shards) = %d, want 2", len(snap.Shards))
	}
	var total uint64
	for _, sh := range snap.Shards {
		if sh.Capacity != 10 {
			t.Errorf("shard %d capacity = %d, want 10", sh.ShardID, sh.Capacity)
		}
		total += sh.Depth
	}
	if total != snap.TotalDepth {
		t.Errorf("sum shard depth %d != TotalDepth %d", total, snap.TotalDepth)
	}
}

func TestDebugSnapshotLaneDepths(t *testing.T) {
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
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "b",
		Lane: "laneB",
		Run:  func(ctx context.Context) error { return nil },
	})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "b2",
		Lane: "laneB",
		Run:  func(ctx context.Context) error { return nil },
	})

	snap := q.DebugSnapshot()
	if snap.Lanes[0].Depth != 1 || snap.Lanes[1].Depth != 2 {
		t.Errorf("lane depths: A=%d B=%d, want 1 and 2", snap.Lanes[0].Depth, snap.Lanes[1].Depth)
	}
}

func TestDebugSnapshotDoesNotExposeMutableInternals(t *testing.T) {
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

	snap1 := q.DebugSnapshot()
	snap1.TotalDepth = 999
	snap1.HotShards = append(snap1.HotShards, keylane.HotShard{ShardID: 99})
	snap1.Shards[0].Depth = 999
	snap1.Lanes[0].Submitted = 999

	snap2 := q.DebugSnapshot()
	if snap2.TotalDepth != 1 {
		t.Errorf("TotalDepth = %d after mutation, want 1", snap2.TotalDepth)
	}
	if len(snap2.HotShards) > 1 && snap2.HotShards[0].ShardID == 99 {
		t.Error("mutation affected internal hot shard list")
	}
}
