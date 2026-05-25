// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"
	"time"
)

func seedFreshAndStaleTrackerEntries(tr *hotKeyTracker, fresh, stale uint64, now time.Time) int {
	window := tr.cfg.DetectionWindow
	staleSeen := now.Add(-2 * window)

	tr.mu.Lock()
	defer tr.mu.Unlock()

	staleIdx := tr.allocateSlot(stale, 0, "", staleSeen)
	tr.entries[staleIdx].lastSeenUnixNano = staleSeen.UnixNano()
	tr.entries[staleIdx].submittedApprox = 20
	tr.entries[staleIdx].queuedApprox = 10

	freshIdx := tr.allocateSlot(fresh, 0, "", now)
	tr.entries[freshIdx].lastSeenUnixNano = now.UnixNano()
	tr.entries[freshIdx].submittedApprox = 50
	tr.entries[freshIdx].queuedApprox = 40

	return staleIdx
}

func assertStaleEntryStillIndexed(t *testing.T, tr *hotKeyTracker, stale uint64, staleIdx int, indexLenBefore int) {
	t.Helper()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.index) != indexLenBefore {
		t.Fatalf("index len = %d, want %d", len(tr.index), indexLenBefore)
	}
	idx, ok := tr.index[stale]
	if !ok {
		t.Fatalf("stale key %d missing from index", stale)
	}
	if idx != staleIdx {
		t.Fatalf("stale index slot = %d, want %d", idx, staleIdx)
	}
	if tr.entries[staleIdx].keyHash != stale {
		t.Fatalf("stale slot keyHash = %d, want %d", tr.entries[staleIdx].keyHash, stale)
	}
}

func TestHotKeyStatusForKeyDoesNotExpireUnrelatedStaleEntry(t *testing.T) {
	tr, _ := testPerKeyPolicyTracker(t)
	now := time.Now()
	const fresh = uint64(1)
	const stale = uint64(2)
	staleIdx := seedFreshAndStaleTrackerEntries(tr, fresh, stale, now)

	tr.mu.Lock()
	indexLenBefore := len(tr.index)
	tr.mu.Unlock()

	status := tr.hotKeyStatusForKey(0, fresh, 50, 0)
	if status == HotKeyStatusNone {
		t.Fatalf("status = %q, want hot-key signal for fresh key", status)
	}
	assertStaleEntryStillIndexed(t, tr, stale, staleIdx, indexLenBefore)
}

func TestPlanPerKeyAdmissionDoesNotExpireUnrelatedStaleEntry(t *testing.T) {
	tr, pkCfg := testPerKeyPolicyTracker(t)
	now := time.Now()
	const fresh = uint64(1)
	const stale = uint64(2)
	staleIdx := seedFreshAndStaleTrackerEntries(tr, fresh, stale, now)

	tr.mu.Lock()
	indexLenBefore := len(tr.index)
	tr.mu.Unlock()

	_ = tr.planPerKeyAdmission(0, fresh, 0, 50, 0, 0.5, pkCfg, now)
	_ = tr.planPerKeyAdmission(0, fresh, 0, 50, 0, 0.5, pkCfg, now)
	assertStaleEntryStillIndexed(t, tr, stale, staleIdx, indexLenBefore)
}

func TestSchedulerObserveSuppressionDoesNotExpireUnrelatedStaleEntry(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 32, reg)
	s.ConfigureHotKey(HotKeyConfig{
		Enabled: true, MaxTrackedKeysPerShard: 16,
		DetectionWindow: time.Minute, HotKeyDepthRatio: 0.3, HotKeyWaitRatio: 0.9,
	})
	pkCfg := PerKeyAdmissionConfig{
		Enabled: true, MinStatus: HotKeyStatusCandidate,
		DefaultAction: PerKeyMitigationThrottle, PressureRatioThreshold: 0.35,
	}
	if err := s.ConfigurePerKeyAdmission(pkCfg); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	const fresh = uint64(1)
	const stale = uint64(2)
	hk := s.hotKeyTrackers[0]
	staleIdx := seedFreshAndStaleTrackerEntries(hk, fresh, stale, now)

	hk.mu.Lock()
	indexLenBefore := len(hk.index)
	hk.mu.Unlock()

	_ = s.ObservePerKeyAdmissionForSuppression(0, fresh, 0, pkCfg)
	_ = s.HotKeyStatusForKey(0, fresh)
	assertStaleEntryStillIndexed(t, hk, stale, staleIdx, indexLenBefore)
}

func TestEvaluatePerKeyAdmissionStillExpiresStaleEntries(t *testing.T) {
	tr, pkCfg := testPerKeyPolicyTracker(t)
	now := time.Now()
	const fresh = uint64(1)
	const stale = uint64(2)
	seedFreshAndStaleTrackerEntries(tr, fresh, stale, now)

	_ = tr.evaluatePerKeyAdmission(0, fresh, 0, 50, 0, 0.5, pkCfg, now)

	tr.mu.Lock()
	defer tr.mu.Unlock()
	if _, ok := tr.index[stale]; ok {
		t.Fatal("stale key should be expired on mutate path")
	}
}
