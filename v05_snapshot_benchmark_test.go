// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkV05SubmitBaseline measures submit with all v0.5 features disabled (pre-v0.5 path).
func BenchmarkV05SubmitBaseline(b *testing.B) {
	q, err := New(v05DisabledConfig())
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
	}
}

// BenchmarkSubmitBaseline is an alias for spec §9 naming; matches BenchmarkV05SubmitBaseline.
func BenchmarkSubmitBaseline(b *testing.B) {
	BenchmarkV05SubmitBaseline(b)
}

func BenchmarkSubmitWithHotKeyTrackingDisabled(b *testing.B) {
	q, err := New(v05HotKeyDisabledConfig())
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
	}
}

func BenchmarkSubmitWithHotKeyTrackingEnabled(b *testing.B) {
	BenchmarkSubmitHotKeyTrackingEnabled(b)
}

func BenchmarkSubmitSingleHotKey(b *testing.B) {
	BenchmarkSubmitHotKeyOneHotManyKeys(b)
}

// BenchmarkSubmitManyUniqueKeysBoundedTracker verifies many unique keys stay within MaxTrackedKeysPerShard.
func BenchmarkSubmitManyUniqueKeysBoundedTracker(b *testing.B) {
	cfg := v05EnabledConfig()
	cfg.ShardCount = 1
	cfg.PerKeyAdmission.Enabled = false
	maxKeys := cfg.HotKey.MaxTrackedKeysPerShard
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	block := make(chan struct{})
	defer close(block)
	run := blockedRun(block)
	// Warm tracker with more unique keys than capacity (queue not started — jobs only enqueue).
	for i := 0; i < int(maxKeys)*3; i++ {
		_ = q.Submit(context.Background(), Job{
			Key: fmt.Sprintf("unique-key-%d", i), Lane: "default", Run: run,
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(context.Background(), Job{
			Key: fmt.Sprintf("bench-unique-%d", i), Lane: "default", Run: run,
		})
	}
	b.StopTimer()
	snap := q.DebugSnapshot()
	total := 0
	for _, sh := range snap.Shards {
		total += len(sh.HotKeyCandidates)
		if sh.HotKeyCandidate != nil {
			total++
		}
	}
	if total > int(maxKeys)*cfg.ShardCount {
		b.Fatalf("hot key candidates = %d, want <= %d per shard", total, maxKeys)
	}
}

func BenchmarkPerKeyAdmissionAllow(b *testing.B) {
	BenchmarkCheckPerKeyAdmission(b)
}

func BenchmarkPerKeyAdmissionRejectHotKey(b *testing.B) {
	BenchmarkCheckPerKeyAdmissionReject(b)
}

func BenchmarkShardPressureSnapshot(b *testing.B) {
	BenchmarkPressureSummaryWithHotKeys(b)
}

func BenchmarkScaleSignalCalculation(b *testing.B) {
	BenchmarkScaleSignalHealthy(b)
}

func BenchmarkDebugSnapshotWithV05Diagnostics(b *testing.B) {
	cfg := v05EnabledConfig()
	cfg.ShardCount = 4
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	block := make(chan struct{})
	defer close(block)
	run := blockedRun(block)
	for i := 0; i < 40; i++ {
		_ = q.Submit(context.Background(), Job{Key: "hot-key", Lane: "default", Run: run})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.DebugSnapshot()
	}
}
