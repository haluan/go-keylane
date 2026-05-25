// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"
	"time"
)

func TestPlanPerKeyAdmissionDoesNotMutateEntry(t *testing.T) {
	tr, pkCfg := testPerKeyPolicyTracker(t)
	now := time.Now()
	hot := uint64(99)
	for i := 0; i < 40; i++ {
		tr.observeSubmit(hot, 0, "", now)
		tr.observeEnqueue(hot, 0, "", now)
	}
	_ = tr.evaluatePerKeyAdmission(0, hot, 0, 64, 0, 0.5, pkCfg, now)

	tr.mu.Lock()
	idx := tr.index[hot]
	lastDecision := tr.entries[idx].lastDecisionUnixNano
	cooldownUntil := tr.entries[idx].cooldownUntilUnixNano
	tr.mu.Unlock()

	_ = tr.planPerKeyAdmission(0, hot, 0, 64, 0, 0.5, pkCfg, now)
	_ = tr.planPerKeyAdmission(0, hot, 0, 64, 0, 0.5, pkCfg, now)

	tr.mu.Lock()
	gotDecision := tr.entries[idx].lastDecisionUnixNano
	gotCooldown := tr.entries[idx].cooldownUntilUnixNano
	tr.mu.Unlock()

	if gotDecision != lastDecision || gotCooldown != cooldownUntil {
		t.Fatalf("entry mutated: decision %d->%d cooldown %d->%d",
			lastDecision, gotDecision, cooldownUntil, gotCooldown)
	}
}

func TestHotKeyStatusForKeyReadOnly(t *testing.T) {
	cfg := HotKeyConfig{
		Enabled: true, MaxTrackedKeysPerShard: 16,
		DetectionWindow: time.Minute, HotKeyDepthRatio: 0.3, HotKeyWaitRatio: 0.9,
	}
	tr := newHotKeyTracker(cfg)
	now := time.Now()
	hot := uint64(42)
	for i := 0; i < 50; i++ {
		tr.observeSubmit(hot, 0, "", now)
		tr.observeEnqueue(hot, 0, "", now)
	}
	status := tr.hotKeyStatusForKey(0, hot, 50, 0)
	if status == HotKeyStatusNone {
		t.Fatalf("status = %q", status)
	}
}
