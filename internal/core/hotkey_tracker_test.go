// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"
	"time"
)

func TestHotKeyTrackerDisabledNoOp(t *testing.T) {
	t.Parallel()
	tr := newHotKeyTracker(HotKeyConfig{Enabled: false})
	now := time.Now()
	tr.observeSubmit(1, 0, "k", now)
	tr.observeEnqueue(1, 0, "k", now)
	tr.observeDequeue(1, now)
	if tr.len() != 0 {
		t.Fatalf("len = %d, want 0", tr.len())
	}
}

func TestHotKeyTrackerNeverExceedsMaxEntries(t *testing.T) {
	t.Parallel()
	cfg := HotKeyConfig{
		Enabled:                true,
		MaxTrackedKeysPerShard: 4,
		DetectionWindow:        time.Minute,
		HotKeyDepthRatio:       0.4,
		HotKeyWaitRatio:        0.4,
	}
	tr := newHotKeyTracker(cfg)
	now := time.Now()
	for i := uint64(0); i < 100; i++ {
		tr.observeSubmit(i, 0, "", now)
		tr.observeEnqueue(i, 0, "", now)
	}
	if tr.len() > cfg.MaxTrackedKeysPerShard {
		t.Fatalf("len = %d, want <= %d", tr.len(), cfg.MaxTrackedKeysPerShard)
	}
}

func TestHotKeyTrackerDequeueReducesQueued(t *testing.T) {
	t.Parallel()
	cfg := HotKeyConfig{
		Enabled:                true,
		MaxTrackedKeysPerShard: 8,
		DetectionWindow:        time.Minute,
		HotKeyDepthRatio:       0.4,
	}
	tr := newHotKeyTracker(cfg)
	now := time.Now()
	h := uint64(7)
	tr.observeEnqueue(h, 0, "", now)
	tr.observeEnqueue(h, 0, "", now)
	tr.observeDequeue(h, now)
	idx := tr.index[h]
	if tr.entries[idx].queuedApprox != 1 {
		t.Fatalf("queuedApprox = %d, want 1", tr.entries[idx].queuedApprox)
	}
}

func TestHotKeyTrackerRejectExistingSlot(t *testing.T) {
	t.Parallel()
	cfg := HotKeyConfig{Enabled: true, MaxTrackedKeysPerShard: 4, DetectionWindow: time.Minute}
	tr := newHotKeyTracker(cfg)
	now := time.Now()
	h := uint64(99)
	tr.observeSubmit(h, 0, "", now)
	tr.observeReject(h, now)
	idx := tr.index[h]
	if tr.entries[idx].rejectedApprox != 1 {
		t.Fatalf("rejectedApprox = %d, want 1", tr.entries[idx].rejectedApprox)
	}
	tr.observeReject(uint64(1000), now)
	if tr.len() != 1 {
		t.Fatalf("len = %d, want 1 (no new slot for unknown reject)", tr.len())
	}
}

func TestHotKeyTrackerSlotReuseIndexInvariant(t *testing.T) {
	t.Parallel()
	cfg := HotKeyConfig{
		Enabled:                true,
		MaxTrackedKeysPerShard: 4,
		DetectionWindow:        time.Minute,
	}
	tr := newHotKeyTracker(cfg)
	now := time.Now()
	for round := 0; round < 50; round++ {
		for k := uint64(0); k < 20; k++ {
			tr.observeSubmit(k+uint64(round*100), 0, "", now)
		}
	}
	if tr.len() > cfg.MaxTrackedKeysPerShard {
		t.Fatalf("len = %d, want <= %d", tr.len(), cfg.MaxTrackedKeysPerShard)
	}
	for h, idx := range tr.index {
		if tr.entries[idx].keyHash != h {
			t.Fatalf("index[%d]=%d maps to entry keyHash=%d", h, idx, tr.entries[idx].keyHash)
		}
	}
}

func TestHotKeyTrackerExposeRawKey(t *testing.T) {
	t.Parallel()
	cfg := HotKeyConfig{
		Enabled:                true,
		MaxTrackedKeysPerShard: 4,
		DetectionWindow:        time.Minute,
		HotKeyDepthRatio:       0.2,
		ExposeRawKey:           true,
	}
	tr := newHotKeyTracker(cfg)
	now := time.Now()
	tr.observeSubmit(1, 0, "tenant-a", now)
	tr.observeEnqueue(1, 0, "tenant-a", now)
	top, _ := tr.detectHotKeyCandidates(0, 10, 0)
	if top == nil {
		t.Fatal("expected candidate")
	}
	if top.Key != "tenant-a" {
		t.Fatalf("Key = %q, want tenant-a", top.Key)
	}
}
