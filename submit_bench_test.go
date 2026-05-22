// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"math/rand"
	"testing"
)

func BenchmarkSubmitSingleLane(b *testing.B) {
	q, cancel := setupQueue(16, 4, 10000, map[Lane]int{"default": 1})
	defer cancel()

	ctx := context.Background()
	job := Job{
		Key:  "my-key",
		Lane: "default",
		Run:  dummyRun,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(ctx, job)
	}
}

func BenchmarkSubmitMultipleLanes(b *testing.B) {
	q, cancel := setupQueue(16, 4, 10000, map[Lane]int{
		"laneA": 1,
		"laneB": 2,
		"laneC": 3,
	})
	defer cancel()

	ctx := context.Background()
	jobs := []Job{
		{Key: "key-1", Lane: "laneA", Run: dummyRun},
		{Key: "key-2", Lane: "laneB", Run: dummyRun},
		{Key: "key-3", Lane: "laneC", Run: dummyRun},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(ctx, jobs[i%3])
	}
}

func BenchmarkSubmitSameKey(b *testing.B) {
	q, cancel := setupQueue(16, 4, 10000, map[Lane]int{"default": 1})
	defer cancel()

	ctx := context.Background()
	job := Job{
		Key:  "persistent-key",
		Lane: "default",
		Run:  dummyRun,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(ctx, job)
	}
}

func BenchmarkSubmitManyKeys(b *testing.B) {
	q, cancel := setupQueue(16, 4, 10000, map[Lane]int{"default": 1})
	defer cancel()

	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))

	// Pre-generate keys to avoid benchmark allocation pollution
	keys := make([]string, 1000)
	for i := 0; i < len(keys); i++ {
		keys[i] = randomKey(rng, 1000)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(ctx, Job{
			Key:  keys[i%1000],
			Lane: "default",
			Run:  dummyRun,
		})
	}
}

// BenchmarkSubmitHotPathAllocGuardrail is the guardrail for the Submit enqueue path.
// The queue is never started, so processShard and shardInflight/laneInflight atomics are not
// exercised. Compare allocs/op with benchstat against BenchmarkSubmitSingleLane before/after
// GC Pressure Snapshot; neither benchmark should gain allocations from stats/inflight wiring.
func BenchmarkSubmitHotPathAllocGuardrail(b *testing.B) {
	cfg := Config{
		ShardCount:       16,
		WorkerCount:      4,
		QueueSizePerLane: 10000,
		LaneQuotas:       map[Lane]int{"default": 1},
	}
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	job := Job{
		Key:  "guardrail-key",
		Lane: "default",
		Run:  dummyRun,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(ctx, job)
	}
}
