// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkProcessShardEmpty(b *testing.B) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 1000, reg)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.processShard(ctx, 0)
	}
}

func BenchmarkProcessShardSingleLane(b *testing.B) {
	benchmarkProcessShardSingleLane(b)
}

// BenchmarkProcessShardSingleLaneInflightGuardrail is the GC Pressure Snapshot guardrail for the
// processShard pop/execute hot path where shardInflight and laneInflight atomics are
// updated. It mirrors BenchmarkProcessShardSingleLane; compare allocs/op with benchstat
// before and after GC Pressure Snapshot to confirm in-flight accounting does not regress B/op or allocs/op.
func BenchmarkProcessShardSingleLaneInflightGuardrail(b *testing.B) {
	benchmarkProcessShardSingleLane(b)
}

func benchmarkProcessShardSingleLane(b *testing.B) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 10})
	s, _ := NewScheduler(1, 1, 1000, reg)
	ctx := context.Background()
	job := InternalJob{KeyHash: 1, LaneID: 0, Run: dummyRun}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for k := 0; k < 10; k++ {
			_ = s.shards[0].Lanes[0].push(job)
		}
		s.shards[0].Ready = true
		s.processShard(ctx, 0)
	}
}

func BenchmarkProcessShardMultipleLanes(b *testing.B) {
	reg, _ := NewLaneRegistry(map[string]int{
		"lane0": 5,
		"lane1": 5,
	})
	s, _ := NewScheduler(1, 1, 1000, reg)
	ctx := context.Background()
	job0 := InternalJob{KeyHash: 1, LaneID: 0, Run: dummyRun}
	job1 := InternalJob{KeyHash: 1, LaneID: 1, Run: dummyRun}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for k := 0; k < 5; k++ {
			_ = s.shards[0].Lanes[0].push(job0)
			_ = s.shards[0].Lanes[1].push(job1)
		}
		s.shards[0].Ready = true
		s.processShard(ctx, 0)
	}
}

func BenchmarkProcessShardManyLanes(b *testing.B) {
	quotas := make(map[string]int)
	for i := 0; i < 10; i++ {
		quotas[fmt.Sprintf("lane%d", i)] = 2
	}
	reg, _ := NewLaneRegistry(quotas)
	s, _ := NewScheduler(1, 1, 1000, reg)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for l := 0; l < 10; l++ {
			job := InternalJob{KeyHash: 1, LaneID: LaneID(l), Run: dummyRun}
			for k := 0; k < 2; k++ {
				_ = s.shards[0].Lanes[l].push(job)
			}
		}
		s.shards[0].Ready = true
		s.processShard(ctx, 0)
	}
}

func BenchmarkRouteShardID(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = routeShardID(uint64(i), 16)
	}
}

func BenchmarkLaneLookup(b *testing.B) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = reg.Lookup("default")
	}
}

func BenchmarkProcessShardWithoutPool(b *testing.B) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 10})
	s, _ := NewScheduler(1, 1, 1000, reg)
	s.Obs.DisablePooling = true
	ctx := context.Background()
	job := InternalJob{KeyHash: 1, LaneID: 0, Run: dummyRun}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for k := 0; k < 10; k++ {
			_ = s.shards[0].Lanes[0].push(job)
		}
		s.shards[0].Ready = true
		s.processShard(ctx, 0)
	}
}

func BenchmarkProcessShardWithPool(b *testing.B) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 10})
	s, _ := NewScheduler(1, 1, 1000, reg)
	s.Obs.DisablePooling = false
	ctx := context.Background()
	job := InternalJob{KeyHash: 1, LaneID: 0, Run: dummyRun}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for k := 0; k < 10; k++ {
			_ = s.shards[0].Lanes[0].push(job)
		}
		s.shards[0].Ready = true
		s.processShard(ctx, 0)
	}
}
