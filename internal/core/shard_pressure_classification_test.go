// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "testing"

func testShardPressureCfg() ShardPressureConfig {
	return testShardPressureConfig()
}

func TestShardPressureHealthy(t *testing.T) {
	t.Parallel()
	cfg := testShardPressureCfg()
	in := shardPressureInput{
		PressureRatio: 0.2,
		Cfg:           cfg,
	}
	if got := classifyShardPressure(in); got != ShardPressureHealthy {
		t.Fatalf("class = %q, want healthy", got)
	}
}

func TestShardPressureLocalizedHotKey(t *testing.T) {
	t.Parallel()
	cfg := testShardPressureCfg()
	in := shardPressureInput{
		PressureRatio:         0.8,
		HasHotKeyCandidate:    true,
		TopHotKeyContribution: 0.5,
		Cfg:                   cfg,
	}
	if got := classifyShardPressure(in); got != ShardPressureLocalizedKey {
		t.Fatalf("class = %q, want localized_key", got)
	}
}

func TestShardPressureLaneDominant(t *testing.T) {
	t.Parallel()
	cfg := testShardPressureCfg()
	in := shardPressureInput{
		PressureRatio:       0.8,
		TopLaneContribution: 0.7,
		Cfg:                 cfg,
	}
	if got := classifyShardPressure(in); got != ShardPressureLaneDominant {
		t.Fatalf("class = %q, want lane_dominant", got)
	}
}

func TestShardPressureShardHot(t *testing.T) {
	t.Parallel()
	cfg := testShardPressureCfg()
	in := shardPressureInput{
		PressureRatio: 0.85,
		SkewRatio:     2.0,
		Cfg:           cfg,
	}
	if got := classifyShardPressure(in); got != ShardPressureShardHot {
		t.Fatalf("class = %q, want shard_hot", got)
	}
}

func TestShardPressureHighPressureLowSkewReturnsShardHot(t *testing.T) {
	t.Parallel()
	cfg := testShardPressureCfg()
	in := shardPressureInput{
		PressureRatio:         0.85,
		SkewRatio:             1.0,
		HasHotKeyCandidate:    true,
		TopHotKeyContribution: 0.1,
		Cfg:                   cfg,
	}
	if got := classifyShardPressure(in); got != ShardPressureShardHot {
		t.Fatalf("class = %q, want shard_hot when above hot threshold and not localized/lane-dominant", got)
	}
}

func TestShardPressureHighPressureHighSkewReturnsShardHot(t *testing.T) {
	t.Parallel()
	cfg := testShardPressureCfg()
	in := shardPressureInput{
		PressureRatio: 0.85,
		SkewRatio:     1.5,
		Cfg:           cfg,
	}
	if got := classifyShardPressure(in); got != ShardPressureShardHot {
		t.Fatalf("class = %q, want shard_hot when skew >= 1.5", got)
	}
}

func TestGlobalPressureDistributed(t *testing.T) {
	t.Parallel()
	cfg := testShardPressureCfg()
	in := globalPressureInput{
		ShardCount:    4,
		HotShardCount: 3,
		HotShardRatio: 0.75,
		Cfg:           cfg,
	}
	if got := classifyGlobalPressure(in); got != ShardPressureDistributed {
		t.Fatalf("class = %q, want distributed", got)
	}
}

func TestGlobalPressureWorkerBound(t *testing.T) {
	t.Parallel()
	cfg := testShardPressureCfg()
	in := globalPressureInput{
		ShardCount:      4,
		HotShardCount:   1,
		HotShardRatio:   0.25,
		WorkerBusyRatio: 0.95,
		QueueWaitRatio:  0.5,
		MaxSkewRatio:    1.2,
		Cfg:             cfg,
	}
	if got := classifyGlobalPressure(in); got != ShardPressureWorkerBound {
		t.Fatalf("class = %q, want worker_bound", got)
	}
}

func TestComputeShardPressureRatio(t *testing.T) {
	t.Parallel()
	got := computeShardPressureRatio(0.3, 0.5, 0.2, 0.1)
	if got != 0.5 {
		t.Fatalf("ratio = %v, want 0.5", got)
	}
}

func TestIsLocalizedHotKeyPressure(t *testing.T) {
	t.Parallel()
	cfg := testShardPressureCfg()
	if !isLocalizedHotKeyPressure(shardPressureInput{
		PressureRatio: 0.8, HasHotKeyCandidate: true, TopHotKeyContribution: 0.5, Cfg: cfg,
	}) {
		t.Fatal("expected localized hot key")
	}
	if isLocalizedHotKeyPressure(shardPressureInput{
		PressureRatio: 0.8, HasHotKeyCandidate: true, TopHotKeyContribution: 0.1, Cfg: cfg,
	}) {
		t.Fatal("expected not localized below ratio threshold")
	}
}

func TestIsDistributedPressure(t *testing.T) {
	t.Parallel()
	cfg := testShardPressureCfg()
	if !isDistributedPressure(globalPressureInput{HotShardRatio: 0.6, Cfg: cfg}) {
		t.Fatal("expected distributed")
	}
}

func TestComputeScaleMitigationFlags(t *testing.T) {
	t.Parallel()
	cfg := testShardPressureCfg()
	scale, mit := computeScaleMitigationFlags(ShardPressureDistributed, globalPressureInput{Cfg: cfg})
	if !scale || mit {
		t.Fatalf("distributed: scale=%v mit=%v", scale, mit)
	}
	_, mit = computeScaleMitigationFlags(ShardPressureLocalizedKey, globalPressureInput{Cfg: cfg})
	if !mit {
		t.Fatal("localized should be mitigation relevant")
	}
}
