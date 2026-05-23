// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"testing"
	"time"
)

func BenchmarkSubmitWithAdaptiveQuotaDisabled(b *testing.B) {
	q, _ := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 1},
	})
	ctx := context.Background()
	_ = q.Start(ctx)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(ctx, Job{Key: "k", Lane: "default", Run: func(context.Context) error { return nil }})
	}
}

func BenchmarkSubmitWithAdaptiveQuotaEnabled(b *testing.B) {
	q, _ := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas:    map[Lane]int{"default": 1},
		Observability: DefaultObservabilityConfig(),
		AdaptiveQuota: AdaptiveQuotaPolicy{
			Config: AdaptiveQuotaConfig{
				Enabled: true, EvaluationInterval: time.Second, MaxAdjustmentsPerTick: 1,
			},
		},
	})
	ctx := context.Background()
	_ = q.Start(ctx)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(ctx, Job{Key: "k", Lane: "default", Run: func(context.Context) error { return nil }})
	}
}

func criticalBackgroundBenchConfig(adaptive bool) Config {
	cfg := Config{
		ShardCount: 1, WorkerCount: 2, QueueSizePerLane: 32,
		LaneQuotas:    map[Lane]int{"critical": 2, "background": 1},
		Observability: DefaultObservabilityConfig(),
	}
	if adaptive {
		cfg.AdaptiveQuota = AdaptiveQuotaPolicy{
			Config: AdaptiveQuotaConfig{
				Enabled: true, EvaluationInterval: time.Second,
				WarmupDuration: time.Nanosecond, CooldownDuration: time.Second,
				MaxAdjustmentsPerTick: 1,
			},
			Lanes: []LaneAdaptivePolicy{
				{Lane: "critical", Class: LaneCritical, Enabled: true, MinQuota: 1, MaxQuota: 8, AllowIncrease: true, AllowDecrease: false},
				{Lane: "background", Class: LaneBackground, Enabled: true, MinQuota: 1, MaxQuota: 4, AllowIncrease: false, AllowDecrease: true},
			},
		}
	}
	return cfg
}

func BenchmarkFixedQuotaCriticalAndBackground(b *testing.B) {
	q, _ := New(criticalBackgroundBenchConfig(false))
	ctx := context.Background()
	_ = q.Start(ctx)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lane := Lane("critical")
		if i&1 == 1 {
			lane = "background"
		}
		_ = q.Submit(ctx, Job{Key: "k", Lane: lane, Run: func(context.Context) error { return nil }})
	}
}

func BenchmarkAdaptiveQuotaCriticalAndBackground(b *testing.B) {
	q, _ := New(criticalBackgroundBenchConfig(true))
	ctx := context.Background()
	_ = q.Start(ctx)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lane := Lane("critical")
		if i&1 == 1 {
			lane = "background"
		}
		_ = q.Submit(ctx, Job{Key: "k", Lane: lane, Run: func(context.Context) error { return nil }})
	}
}

func BenchmarkAdaptiveQuotaSnapshot(b *testing.B) {
	q, _ := New(criticalBackgroundBenchConfig(true))
	ctx := context.Background()
	_ = q.Start(ctx)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.AdaptiveQuotaSnapshot()
	}
}
