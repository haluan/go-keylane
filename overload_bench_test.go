// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"testing"
)

// BenchmarkOverloadPolicyDecision measures overload evaluation before enqueue (keep path).
func BenchmarkOverloadPolicyDecision(b *testing.B) {
	benchmarkCheckOverload(b)
}

// BenchmarkCheckOverload is the guardrail for the public overload hot path on successful admit.
func BenchmarkCheckOverload(b *testing.B) {
	benchmarkCheckOverload(b)
}

func benchmarkCheckOverload(b *testing.B) {
	cfg := Config{
		ShardCount:       4,
		WorkerCount:      2,
		QueueSizePerLane: 1000,
		LaneQuotas:       map[Lane]int{"default": 2},
	}
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	overloadCfg := OverloadConfig{Enabled: true}
	meta := RequestMeta{Key: "bench-key", Lane: "default"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CheckOverload(q, overloadCfg, meta)
	}
}

func BenchmarkOverloadBestEffortShedding(b *testing.B) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 64,
		LaneQuotas:      map[Lane]int{"critical": 2, "best_effort": 1},
		OverloadEnabled: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	_, _ = q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{Class: LaneNormal, RejectAboveRatio: 0.90, MaxQueueDepth: 100},
		Lanes: []LaneOverloadPolicy{
			{Lane: "best_effort", Class: LaneBestEffort, RejectAboveRatio: 0.75, ShedAboveRatio: 0.50, MaxQueueDepth: 100},
		},
	})
	ctx := context.Background()
	_ = q.Start(ctx)
	metaBE := RequestMeta{Key: "k", Lane: "best_effort"}
	metaCR := RequestMeta{Key: "k", Lane: "critical"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CheckOverload(q, OverloadConfig{Enabled: true}, metaBE)
		_ = CheckOverload(q, OverloadConfig{Enabled: true}, metaCR)
	}
}

func BenchmarkSubmitOverloadPolicyEnabled(b *testing.B) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 2, QueueSizePerLane: 32,
		LaneQuotas:      map[Lane]int{"default": 2},
		OverloadEnabled: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	_ = q.Start(ctx)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(ctx, Job{Key: "k", Lane: "default", Run: func(context.Context) error { return nil }})
	}
}
