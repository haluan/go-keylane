// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"testing"
)

func autoscalingBenchConfig(enabled bool) Config {
	cfg := autoscalingTestConfig()
	cfg.AutoscalingSignal.Enabled = enabled
	cfg.ShardPressure.Enabled = enabled
	return cfg
}

func BenchmarkScaleSignalHealthy(b *testing.B) {
	q, err := New(autoscalingBenchConfig(true))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.ScaleSignal()
	}
}

func BenchmarkScaleSignalHighQueueDepth(b *testing.B) {
	cfg := autoscalingBenchConfig(true)
	cfg.ShardCount = 1
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	block := make(chan struct{})
	defer close(block)
	run := func(ctx context.Context) error {
		select {
		case <-block:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for i := 0; i < 20; i++ {
		_ = q.Submit(context.Background(), Job{Key: "k", Lane: "default", Run: run})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.ScaleSignal()
	}
}

func BenchmarkScaleSignalWithHotShardDiagnostics(b *testing.B) {
	cfg := autoscalingBenchConfig(true)
	cfg.ShardCount = 4
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	block := make(chan struct{})
	defer close(block)
	run := func(ctx context.Context) error {
		select {
		case <-block:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for i := 0; i < 40; i++ {
		_ = q.Submit(context.Background(), Job{Key: "hot-key", Lane: "default", Run: run})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.ScaleSignal()
	}
}

func BenchmarkScaleSignalConcurrentRead(b *testing.B) {
	q, err := New(autoscalingBenchConfig(true))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = q.ScaleSignal()
		}
	})
}

func BenchmarkScaleSignalDisabled(b *testing.B) {
	q, err := New(autoscalingBenchConfig(false))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.ScaleSignal()
	}
}
