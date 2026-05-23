// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"testing"
)

func TestNewScheduler(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 10})
	shardCount := 8
	workerCount := 4
	queueSize := 100

	s, err := NewScheduler(shardCount, workerCount, queueSize, reg)
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}

	if len(s.shards) != shardCount {
		t.Errorf("shards count = %d, want %d", len(s.shards), shardCount)
	}
	if cap(s.ReadyCh) != shardCount {
		t.Errorf("ReadyCh capacity = %d, want %d", cap(s.ReadyCh), shardCount)
	}
	if s.workerCount != workerCount {
		t.Errorf("workerCount = %d, want %d", s.workerCount, workerCount)
	}
	if len(s.loadQuotaPolicy().laneQuotas) != reg.Len() {
		t.Errorf("laneQuotas len = %d, want %d", len(s.loadQuotaPolicy().laneQuotas), reg.Len())
	}
}

func TestNewSchedulerCreatesShards(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 10, "high": 20})
	s, _ := NewScheduler(4, 2, 50, reg)

	if len(s.shards) != 4 {
		t.Fatalf("len(s.shards) = %d, want 4", len(s.shards))
	}
	for i := 0; i < 4; i++ {
		if len(s.shards[i].Lanes) != 2 {
			t.Errorf("shard %d lanes = %d, want 2", i, len(s.shards[i].Lanes))
		}
	}
}

func TestSchedulerEnqueueRoutesCorrectly(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 10})
	s, _ := NewScheduler(4, 2, 50, reg)

	// Key "A" hashes to a specific shard.
	// Key "B" might hash to another.
	h1 := HashKey("A")
	shardID1 := routeShardID(h1, 4)

	ij := InternalJob{KeyHash: h1, LaneID: 0, Run: func(ctx context.Context) error { return nil }}
	_, _, _ = s.Enqueue(ij)

	if s.shards[shardID1].Lanes[0].depth() != 1 {
		t.Errorf("shard %d lane 0 depth = %d, want 1", shardID1, s.shards[shardID1].Lanes[0].depth())
	}
}
