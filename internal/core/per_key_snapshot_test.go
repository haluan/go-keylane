// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"testing"
	"time"
)

func TestDebugSnapshotPerKeyAdmissionSnapshots(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 32, reg)
	s.ConfigureHotKey(HotKeyConfig{
		Enabled: true, MaxTrackedKeysPerShard: 8,
		DetectionWindow: time.Minute, HotKeyDepthRatio: 0.25,
	})
	if err := s.ConfigurePerKeyAdmission(PerKeyAdmissionConfig{
		Enabled: true, MinStatus: HotKeyStatusCandidate,
		DefaultAction: PerKeyMitigationReject, PressureRatioThreshold: 0.3,
		MaxSnapshotsPerShard: 3,
	}); err != nil {
		t.Fatal(err)
	}

	h := HashKey("snap-key")
	now := time.Now()
	hk := s.hotKeyTrackers[0]
	for i := 0; i < 20; i++ {
		hk.observeSubmit(h, 0, "", now)
		hk.observeEnqueue(h, 0, "", now)
	}
	_ = s.EvaluatePerKeyAdmission(0, h, 0)

	snap := s.DebugSnapshot()
	if len(snap.PerKeyAdmissionSnapshots) == 0 {
		t.Fatal("expected per-key admission snapshots")
	}
	if snap.PerKeyAdmissionSnapshots[0].KeyHash != h {
		t.Fatalf("KeyHash = %d", snap.PerKeyAdmissionSnapshots[0].KeyHash)
	}
	snap.PerKeyAdmissionSnapshots[0].KeyHash = 0
	snap2 := s.DebugSnapshot()
	if snap2.PerKeyAdmissionSnapshots[0].KeyHash != h {
		t.Fatal("snapshot should be copy-out")
	}
}

func TestPerKeySnapshotsRespectTotalCap(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(4, 1, 8, reg)
	s.ConfigureHotKey(HotKeyConfig{
		Enabled: true, MaxTrackedKeysPerShard: 8,
		DetectionWindow: time.Minute, HotKeyDepthRatio: 0.2,
	})
	if err := s.ConfigurePerKeyAdmission(PerKeyAdmissionConfig{
		Enabled: true, DefaultAction: PerKeyMitigationReject,
		MaxSnapshotsPerShard: 10, MaxSnapshotsTotal: 3,
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for shard := 0; shard < 4; shard++ {
		hk := s.hotKeyTrackers[shard]
		for k := uint64(0); k < 5; k++ {
			h := HashKey(string(rune('a'+int(k))) + string(rune('0'+shard)))
			hk.observeSubmit(h, 0, "", now)
			hk.observeEnqueue(h, 0, "", now)
			_ = s.EvaluatePerKeyAdmission(shard, h, 0)
		}
	}
	snap := s.DebugSnapshot()
	if len(snap.PerKeyAdmissionSnapshots) > 3 {
		t.Fatalf("len = %d, want <= 3", len(snap.PerKeyAdmissionSnapshots))
	}
}

func TestPerKeySnapshotNoRawKey(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 16, reg)
	s.ConfigureHotKey(HotKeyConfig{
		Enabled: true, MaxTrackedKeysPerShard: 4, DetectionWindow: time.Minute,
		HotKeyDepthRatio: 0.2, ExposeRawKey: true,
	})
	if err := s.ConfigurePerKeyAdmission(PerKeyAdmissionConfig{Enabled: true, DefaultAction: PerKeyMitigationReject}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)
	defer func() { _ = s.Stop(context.Background(), false) }()

	h := HashKey("tenant-secret")
	for i := 0; i < 15; i++ {
		_, _, _ = s.Enqueue(InternalJob{KeyHash: h, LaneID: 0, RawKey: "tenant-secret",
			Run: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }})
	}
	_ = s.EvaluatePerKeyAdmission(0, h, 0)
	for _, ps := range s.PerKeyAdmissionSnapshots() {
		_ = ps.KeyHash // snapshots use hash only
	}
}
