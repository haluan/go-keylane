// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"testing"
	"time"
)

func benchPressureQueue(b *testing.B, enabled bool) Config {
	cfg := shardPressureTestConfig()
	cfg.ShardPressure.Enabled = enabled
	cfg.Observability.EnableDebugSnapshot = true
	return cfg
}

func BenchmarkPressureSnapshotIdle(b *testing.B) {
	q, err := New(benchPressureQueue(b, true))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.PressureSummary()
	}
}

func BenchmarkPressureSnapshotManyShards(b *testing.B) {
	cfg := benchPressureQueue(b, true)
	cfg.ShardCount = 16
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.PressureSummary()
	}
}

func BenchmarkPressureSnapshotHotShard(b *testing.B) {
	cfg := benchPressureQueue(b, true)
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
	for i := 0; i < 30; i++ {
		_ = q.Submit(context.Background(), Job{Key: "hot", Lane: "default", Run: run})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = q.ShardPressure(0)
	}
}

func BenchmarkPressureSummaryWithHotKeys(b *testing.B) {
	cfg := benchPressureQueue(b, true)
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
	for i := 0; i < 25; i++ {
		_ = q.Submit(context.Background(), Job{Key: "bench-hot", Lane: "default", Run: run})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.PressureSummary()
	}
}

func BenchmarkPressureSummaryDiagnosticsEnabled(b *testing.B) {
	q, err := New(benchPressureQueue(b, true))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.PressureSummary()
	}
}

func BenchmarkPressureSummaryDiagnosticsDisabled(b *testing.B) {
	q, err := New(benchPressureQueue(b, false))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.PressureSummary()
	}
}

func BenchmarkSubmitWithPressureSummaryPoll(b *testing.B) {
	cfg := benchPressureQueue(b, true)
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(ctx, Job{
			Key: "k", Lane: "default",
			Run: func(context.Context) error { return nil },
		})
		_ = q.PressureSummary()
	}
}

func BenchmarkDebugSnapshotPressureSummary(b *testing.B) {
	cfg := benchPressureQueue(b, true)
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		_ = q.Submit(context.Background(), Job{
			Key: "k", Lane: "default",
			Run: func(context.Context) error { time.Sleep(time.Millisecond); return nil },
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.DebugSnapshot()
	}
}
