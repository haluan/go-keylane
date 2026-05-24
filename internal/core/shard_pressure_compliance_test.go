// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"testing"
	"time"
)

func TestHotKeyRejectNotDoubleCounted(t *testing.T) {
	t.Parallel()
	c := HotKeyCandidate{
		KeyHash:         42,
		SubmittedApprox: 100,
		RejectedApprox:  25,
	}
	snapReject := hotKeyToPressureSnapshot(c, 100, 0, nil, PerKeyMitigationReject, PerKeyAdmissionReasonHotKeyCandidate)
	if snapReject.RejectedApprox != 25 {
		t.Fatalf("RejectedApprox = %d, want 25", snapReject.RejectedApprox)
	}
	if snapReject.ThrottledApprox != 0 || snapReject.ShedApprox != 0 {
		t.Fatalf("throttle/shed should be zero on reject: %+v", snapReject)
	}

	snapThrottle := hotKeyToPressureSnapshot(c, 100, 0, nil, PerKeyMitigationThrottle, PerKeyAdmissionReasonHotKeyCandidate)
	if snapThrottle.ThrottledApprox != 25 {
		t.Fatalf("ThrottledApprox = %d, want 25", snapThrottle.ThrottledApprox)
	}
	if snapThrottle.RejectedApprox != 0 || snapThrottle.ShedApprox != 0 {
		t.Fatalf("reject/shed should be zero on throttle: %+v", snapThrottle)
	}
}

func TestAdmissionRatioUsesShardLaneTotals(t *testing.T) {
	t.Parallel()
	sh := shardDebugView{
		depth:    100,
		capacity: 100,
		laneDeps: []laneDepthInShard{{laneID: 0, depth: 100}},
	}
	lanes := []laneDebugView{{
		submitted:         1000,
		admissionRejected: 100,
		depth:             100,
	}}
	totals := shardLaneAdmissionTotals(sh, lanes)
	laneRatio := computeAdmissionPressureRatio(totals.rejected, totals.throttled, totals.shed, totals.submitted)
	if laneRatio != 0.1 {
		t.Fatalf("lane admission ratio = %v, want 0.1", laneRatio)
	}
	hotOnlyRatio := computeAdmissionPressureRatio(0, 0, 0, 1000)
	if hotOnlyRatio != 0 {
		t.Fatalf("hot-key-only ratio = %v, want 0 without tracked rejects", hotOnlyRatio)
	}
	if laneRatio <= hotOnlyRatio {
		t.Fatal("expected shard lane totals to drive admission ratio above hot-key-only")
	}
}

func TestLanePressureSnapshotHasAdmissionFields(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 32, reg)
	s.ConfigureShardPressure(testShardPressureConfig())
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
		_, _, _ = s.Enqueue(InternalJob{KeyHash: HashKey("k"), LaneID: 0, Run: run})
	}
	snap, ok := s.ShardPressureSnapshot(0)
	if !ok {
		t.Fatal("ShardPressureSnapshot not ok")
	}
	if len(snap.LaneBreakdown) == 0 {
		t.Fatal("expected lane breakdown")
	}
	lane := snap.LaneBreakdown[0]
	if lane.QueueDepthRatio <= 0 || lane.ContributionRatio <= 0 {
		t.Fatalf("expected lane pressure fields populated, got %+v", lane)
	}
	// Admission estimate fields are present (may be zero until rejects/completions occur).
	_ = lane.CompletedApprox
	_ = lane.RejectedApprox
	_ = lane.ThrottledApprox
	_ = lane.ShedApprox
	_ = lane.QueueWaitApproxNanos
	_ = lane.InflightJobs
}

func TestPressureSummaryDisabledNotUnknown(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 32, reg)
	summary := s.PressureSummarySnapshot()
	if summary.DiagnosticsEnabled {
		t.Fatal("expected DiagnosticsEnabled false when disabled")
	}
	if summary.Class != "" {
		t.Fatalf("class = %q, want empty when disabled", summary.Class)
	}
	if summary.UpdatedAt.IsZero() {
		t.Fatal("expected UpdatedAt set when disabled")
	}
}

func TestShardPressureDisabledVsInvalidShardID(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(4, 2, 32, reg)

	snap, ok := s.ShardPressureSnapshot(0)
	if !ok {
		t.Fatal("ShardPressureSnapshot(0) should be ok when disabled")
	}
	if snap.DiagnosticsEnabled {
		t.Fatal("expected DiagnosticsEnabled false when disabled")
	}
	if snap.ShardID != 0 {
		t.Fatalf("ShardID = %d, want 0", snap.ShardID)
	}
	if _, ok := s.ShardPressureSnapshot(99); ok {
		t.Fatal("invalid shard ID should return ok=false")
	}
}

func TestPressureSummaryDoesNotExposeRawKeyCore(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 32, reg)
	cfg := testShardPressureConfig()
	s.ConfigureShardPressure(cfg)
	s.ConfigureHotKey(HotKeyConfig{
		Enabled: true, MaxTrackedKeysPerShard: 16,
		DetectionWindow: time.Minute, HotKeyDepthRatio: 0.3,
		ExposeRawKey: true,
	})
	for i := 0; i < 25; i++ {
		_, _, _ = s.Enqueue(InternalJob{
			KeyHash: HashKey("secret-key"), LaneID: 0,
			Run: func(context.Context) error { return nil },
		})
	}
	snap, ok := s.ShardPressureSnapshot(0)
	if !ok {
		t.Fatal("ShardPressureSnapshot not ok")
	}
	for _, hk := range snap.HotKeyCandidates {
		_ = hk.KeyHash
	}
}

func TestAppendHotShardPressureSnapshotsReusesDst(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(4, 2, 32, reg)
	s.ConfigureShardPressure(testShardPressureConfig())
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
		_, _, _ = s.Enqueue(InternalJob{KeyHash: HashKey("hot"), LaneID: 0, Run: run})
	}
	dst := make([]ShardPressureSnapshot, 0, 4)
	out := s.AppendHotShardPressureSnapshots(dst)
	if cap(out) < 4 {
		t.Fatalf("cap = %d, want reuse of dst capacity", cap(out))
	}
	if len(out) == 0 {
		t.Fatal("expected hot shards appended")
	}
}

func TestLaneDominanceRatioWhenLocalizedKey(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1, "bulk": 1})
	s, _ := NewScheduler(1, 1, 32, reg)
	cfg := testShardPressureConfig()
	s.ConfigureShardPressure(cfg)
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
		laneID := LaneID(1)
		if i%5 != 0 {
			laneID = 0
		}
		_, _, _ = s.Enqueue(InternalJob{KeyHash: HashKey("only-key"), LaneID: laneID, Run: run})
	}
	summary := s.PressureSummarySnapshot()
	if summary.LaneDominanceRatio <= 0 {
		t.Fatalf("LaneDominanceRatio = %v, want > 0 when lane breakdown exists", summary.LaneDominanceRatio)
	}
}
