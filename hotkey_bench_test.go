// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"testing"
	"time"
)

func hotKeyBenchConfig(enabled bool) Config {
	return Config{
		ShardCount:       4,
		WorkerCount:      2,
		QueueSizePerLane: 256,
		LaneQuotas:       map[Lane]int{"default": 1},
		HotKey: HotKeyConfig{
			Enabled:                enabled,
			MaxTrackedKeysPerShard: 64,
			DetectionWindow:        30 * time.Second,
			HotKeyDepthRatio:       0.4,
		},
	}
}

func BenchmarkSubmitHotKeyTrackingDisabled(b *testing.B) {
	q, _ := New(hotKeyBenchConfig(false))
	ctx := context.Background()
	_ = q.Start(ctx)
	defer func() { _ = q.Stop(ctx, WithDrain(true)) }()
	job := Job{Key: "bench-key", Lane: "default", Run: func(ctx context.Context) error { return nil }}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(ctx, job)
	}
}

func BenchmarkSubmitHotKeyTrackingEnabled(b *testing.B) {
	q, _ := New(hotKeyBenchConfig(true))
	ctx := context.Background()
	_ = q.Start(ctx)
	defer func() { _ = q.Stop(ctx, WithDrain(true)) }()
	job := Job{Key: "bench-key", Lane: "default", Run: func(ctx context.Context) error { return nil }}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(ctx, job)
	}
}

func BenchmarkSubmitHotKeyOneHotManyKeys(b *testing.B) {
	q, _ := New(hotKeyBenchConfig(true))
	ctx := context.Background()
	_ = q.Start(ctx)
	defer func() { _ = q.Stop(ctx, WithDrain(true)) }()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := "other"
		if i%10 == 0 {
			key = "hot"
		}
		_ = q.Submit(ctx, Job{Key: key, Lane: "default", Run: func(ctx context.Context) error { return nil }})
	}
}
