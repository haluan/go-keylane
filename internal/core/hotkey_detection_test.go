// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"testing"
	"time"
)

func TestHotKeyTrackerDominantKeyDetected(t *testing.T) {
	t.Parallel()
	cfg := HotKeyConfig{
		Enabled:                true,
		MaxTrackedKeysPerShard: 16,
		DetectionWindow:        time.Minute,
		HotKeyDepthRatio:       0.3,
		HotKeyWaitRatio:        0.9,
	}
	tr := newHotKeyTracker(cfg)
	now := time.Now()
	hot := uint64(42)
	for i := 0; i < 50; i++ {
		tr.observeSubmit(hot, 0, "", now)
		tr.observeEnqueue(hot, 0, "", now)
	}
	for i := uint64(1); i < 10; i++ {
		tr.observeSubmit(i, 0, "", now)
	}
	top, _ := tr.detectHotKeyCandidates(0, 50, 0)
	if top == nil {
		t.Fatal("expected hot key candidate")
	}
	if top.KeyHash != hot {
		t.Fatalf("KeyHash = %d, want %d", top.KeyHash, hot)
	}
	if top.Status == HotKeyStatusNone {
		t.Fatalf("status = %q", top.Status)
	}
}

func TestHotKeyTrackerUniformKeysNoDominant(t *testing.T) {
	t.Parallel()
	cfg := HotKeyConfig{
		Enabled:                true,
		MaxTrackedKeysPerShard: 16,
		DetectionWindow:        time.Minute,
		HotKeyDepthRatio:       0.9,
		HotKeyWaitRatio:        0.9,
	}
	tr := newHotKeyTracker(cfg)
	now := time.Now()
	for i := uint64(0); i < 8; i++ {
		tr.observeSubmit(i, 0, "", now)
		tr.observeEnqueue(i, 0, "", now)
	}
	top, all := tr.detectHotKeyCandidates(0, 8, 0)
	if top != nil || len(all) > 0 {
		t.Fatalf("unexpected candidate: top=%v all=%d", top, len(all))
	}
}

func TestHotKeyDetectionReasonDepthRatio(t *testing.T) {
	t.Parallel()
	cfg := HotKeyConfig{
		Enabled:                true,
		MaxTrackedKeysPerShard: 8,
		DetectionWindow:        time.Minute,
		HotKeyDepthRatio:       0.3,
		HotKeyWaitRatio:        0.99,
	}
	tr := newHotKeyTracker(cfg)
	now := time.Now()
	hot := uint64(1)
	for i := 0; i < 20; i++ {
		tr.observeSubmit(hot, 0, "", now)
		tr.observeEnqueue(hot, 0, "", now)
	}
	top, _ := tr.detectHotKeyCandidates(0, 20, 0)
	if top == nil {
		t.Fatal("expected candidate")
	}
	switch top.Reason {
	case "depth_ratio", "depth_and_submit_ratio", "dominant_key_concentration":
	default:
		t.Fatalf("Reason = %q, want depth_ratio, depth_and_submit_ratio, or dominant_key_concentration", top.Reason)
	}
}

func TestHotKeyCandidateLimitFromConfig(t *testing.T) {
	t.Parallel()
	cfg := HotKeyConfig{
		Enabled:                  true,
		MaxTrackedKeysPerShard:   16,
		DetectionWindow:          time.Minute,
		HotKeyDepthRatio:         0.2,
		HotKeyWaitRatio:          0.99,
		MaxCandidatesPerSnapshot: 2,
	}
	tr := newHotKeyTracker(cfg)
	now := time.Now()
	for k := uint64(1); k <= 6; k++ {
		for i := 0; i < 30; i++ {
			tr.observeSubmit(k, 0, "", now)
			tr.observeEnqueue(k, 0, "", now)
		}
	}
	_, all := tr.detectHotKeyCandidates(0, 180, 0)
	if len(all) > 2 {
		t.Fatalf("len(all) = %d, want <= 2", len(all))
	}
}

func TestHotKeyCandidateReportsLaneID(t *testing.T) {
	t.Parallel()
	reg, _ := NewLaneRegistry(map[string]int{"laneA": 1, "laneB": 1})
	s, _ := NewScheduler(1, 1, 64, reg)
	s.ConfigureHotKey(HotKeyConfig{
		Enabled:                true,
		MaxTrackedKeysPerShard: 16,
		DetectionWindow:        time.Minute,
		HotKeyDepthRatio:       0.25,
		HotKeyWaitRatio:        0.99,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)
	defer func() { _ = s.Stop(context.Background(), false) }()

	laneB, ok := reg.Lookup("laneB")
	if !ok {
		t.Fatal("laneB not registered")
	}
	hot := HashKey("hot-on-lane-b")
	run := func(c context.Context) error { <-c.Done(); return c.Err() }
	for i := 0; i < 40; i++ {
		_, _, err := s.Enqueue(InternalJob{KeyHash: hot, LaneID: laneB, Run: run})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		_, _, _ = s.Enqueue(InternalJob{KeyHash: HashKey("other"), LaneID: 0, Run: run})
	}
	snap := s.DebugSnapshot()
	if snap.Shards[0].HotKeyCandidate == nil {
		t.Fatal("expected HotKeyCandidate")
	}
	if snap.Shards[0].HotKeyCandidate.LaneID != uint16(laneB) {
		t.Fatalf("LaneID = %d, want %d", snap.Shards[0].HotKeyCandidate.LaneID, laneB)
	}
}
