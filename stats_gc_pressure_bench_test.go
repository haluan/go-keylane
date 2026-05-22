// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"testing"
)

// BenchmarkStatsGCPressure measures allocation cost of an explicit StatsGCPressure()
// snapshot call. Snapshot collection allocates new slices; this benchmark documents
// that cost separately from the Submit enqueue path.
func BenchmarkStatsGCPressure(b *testing.B) {
	q, cancel := setupQueue(8, 4, 1000, map[Lane]int{
		"default": 2,
		"fast":    1,
	})
	defer cancel()

	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_ = q.Submit(ctx, Job{
			Key:  "warm-key",
			Lane: "default",
			Run:  dummyRun,
		})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.StatsGCPressure()
	}
}

// BenchmarkStatsGCPressureEmptyQueue measures snapshot cost on an idle queue.
func BenchmarkStatsGCPressureEmptyQueue(b *testing.B) {
	cfg := Config{
		ShardCount:       8,
		WorkerCount:      4,
		QueueSizePerLane: 1000,
		LaneQuotas:       map[Lane]int{"default": 2, "fast": 1},
	}
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.StatsGCPressure()
	}
}

// BenchmarkSubmitEnqueueGuardrail delegates to submit_bench_test.go guardrail.
func BenchmarkSubmitEnqueueGuardrail(b *testing.B) {
	BenchmarkSubmitHotPathAllocGuardrail(b)
}
