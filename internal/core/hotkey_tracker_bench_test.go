// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"testing"
	"time"
)

func benchHotKeyConfig(enabled bool) HotKeyConfig {
	return HotKeyConfig{
		Enabled:                enabled,
		MaxTrackedKeysPerShard: 64,
		DetectionWindow:        30 * time.Second,
		HotKeyDepthRatio:       0.4,
		HotKeyWaitRatio:        0.4,
	}
}

func BenchmarkHotKeyTrackerObserveDisabled(b *testing.B) {
	tr := newHotKeyTracker(benchHotKeyConfig(false))
	now := time.Now()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h := uint64(i)
		tr.observeSubmit(h, 0, "", now)
		tr.observeEnqueue(h, 0, "", now)
	}
}

func BenchmarkHotKeyTrackerObserveEnabled(b *testing.B) {
	tr := newHotKeyTracker(benchHotKeyConfig(true))
	now := time.Now()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h := uint64(i % 8)
		tr.observeSubmit(h, 0, "", now)
		tr.observeEnqueue(h, 0, "", now)
	}
}

func BenchmarkHotKeyTrackerManyUniqueKeys(b *testing.B) {
	tr := newHotKeyTracker(benchHotKeyConfig(true))
	now := time.Now()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h := uint64(i)
		tr.observeSubmit(h, 0, "", now)
	}
}

func BenchmarkHotKeyTrackerEviction(b *testing.B) {
	cfg := HotKeyConfig{
		Enabled:                true,
		MaxTrackedKeysPerShard: 64,
		DetectionWindow:        30 * time.Second,
		HotKeyDepthRatio:       0.4,
		HotKeyWaitRatio:        0.4,
	}
	tr := newHotKeyTracker(cfg)
	now := time.Now()
	for i := 0; i < 64; i++ {
		tr.observeSubmit(uint64(i), 0, "", now)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tr.observeSubmit(uint64(1000+i), 0, "", now)
	}
}

func BenchmarkEnqueueHotKeyTrackingQueueFull(b *testing.B) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 2, reg)
	s.ConfigureHotKey(benchHotKeyConfig(true))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)
	defer func() { _ = s.Stop(context.Background(), false) }()

	run := func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
	for i := 0; i < 2; i++ {
		_, _, _ = s.Enqueue(InternalJob{KeyHash: 1, LaneID: 0, Run: run})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = s.Enqueue(InternalJob{KeyHash: uint64(i), LaneID: 0, Run: run})
	}
}

func BenchmarkHotKeyTrackerDetectCandidates(b *testing.B) {
	tr := newHotKeyTracker(benchHotKeyConfig(true))
	now := time.Now()
	for i := 0; i < 64; i++ {
		tr.observeSubmit(uint64(i), 0, "", now)
		tr.observeEnqueue(uint64(i), 0, "", now)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = tr.detectHotKeyCandidates(0, 64, 0)
	}
}
