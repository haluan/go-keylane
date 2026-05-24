// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"
	"time"
)

func BenchmarkPressureSummarySnapshotIdle(b *testing.B) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(4, 2, 32, reg)
	s.ConfigureShardPressure(testShardPressureConfig())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.PressureSummarySnapshot()
	}
}

func BenchmarkPressureSummarySnapshotManyShards(b *testing.B) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(16, 4, 32, reg)
	s.ConfigureShardPressure(testShardPressureConfig())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.PressureSummarySnapshot()
	}
}

func BenchmarkClassifyShardPressure(b *testing.B) {
	cfg := testShardPressureConfig()
	in := shardPressureInput{
		PressureRatio:         0.85,
		HasHotKeyCandidate:    true,
		TopHotKeyContribution: 0.5,
		Cfg:                   cfg,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = classifyShardPressure(in)
	}
}

func BenchmarkBuildPressureSummary(b *testing.B) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1, "bulk": 1})
	s, _ := NewScheduler(4, 2, 32, reg)
	cfg := testShardPressureConfig()
	s.ConfigureShardPressure(cfg)
	view := s.collectSchedulerDebugView()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bundle := pressureViewBundle{view: view, summaries: make([]ShardPressureSnapshot, len(view.shards))}
		_ = s.buildPressureSummary(bundle, cfg, time.Now())
	}
}
