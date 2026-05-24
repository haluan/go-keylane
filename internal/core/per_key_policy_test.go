// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"errors"
	"testing"
	"time"
)

func testPerKeyPolicyTracker(t *testing.T) (*hotKeyTracker, PerKeyAdmissionConfig) {
	t.Helper()
	hkCfg := HotKeyConfig{
		Enabled:                true,
		MaxTrackedKeysPerShard: 16,
		DetectionWindow:        time.Minute,
		HotKeyDepthRatio:       0.3,
		HotKeyWaitRatio:        0.9,
	}
	tr := newHotKeyTracker(hkCfg)
	pkCfg := PerKeyAdmissionConfig{
		Enabled:                true,
		MinStatus:              HotKeyStatusCandidate,
		DefaultAction:          PerKeyMitigationThrottle,
		PressureRatioThreshold: 0.35,
		Cooldown:               time.Second,
		RecoveryWindow:         2 * time.Second,
	}
	return tr, pkCfg
}

func TestPerKeyPolicyUntrackedKeyAllows(t *testing.T) {
	t.Parallel()
	tr, pkCfg := testPerKeyPolicyTracker(t)
	now := time.Now()
	dec := tr.evaluatePerKeyAdmission(0, 999, 0, 10, 0, 0.5, pkCfg, now)
	if dec.Action != PerKeyMitigationAllow {
		t.Fatalf("action = %q, want allow", dec.Action)
	}
	if dec.HotKeyStatus != HotKeyStatusNone {
		t.Fatalf("HotKeyStatus = %q, want none", dec.HotKeyStatus)
	}
}

func TestPerKeyPolicyHotKeyThrottles(t *testing.T) {
	t.Parallel()
	tr, pkCfg := testPerKeyPolicyTracker(t)
	now := time.Now()
	hot := uint64(42)
	for i := 0; i < 40; i++ {
		tr.observeSubmit(hot, 0, "", now)
		tr.observeEnqueue(hot, 0, "", now)
	}
	dec := tr.evaluatePerKeyAdmission(0, hot, 0, 40, 0, 0.5, pkCfg, now)
	if dec.Action != PerKeyMitigationThrottle {
		t.Fatalf("action = %q, want throttle", dec.Action)
	}
	if dec.Reason != PerKeyAdmissionReasonHotKeyCandidate && dec.Reason != PerKeyAdmissionReasonDominantHotKey {
		t.Fatalf("reason = %q", dec.Reason)
	}
}

func TestPerKeyPolicyMaxQueuedPerKey(t *testing.T) {
	t.Parallel()
	tr, pkCfg := testPerKeyPolicyTracker(t)
	pkCfg.MaxQueuedPerKey = 2
	now := time.Now()
	h := uint64(7)
	tr.observeEnqueue(h, 0, "", now)
	tr.observeEnqueue(h, 0, "", now)
	dec := tr.evaluatePerKeyAdmission(0, h, 0, 2, 0, 0, pkCfg, now)
	if dec.Action != PerKeyMitigationThrottle || dec.Reason != PerKeyAdmissionReasonMaxQueuedPerKey {
		t.Fatalf("dec = %+v, want max_queued throttle", dec)
	}
}

func TestPerKeyPolicyCooldownActive(t *testing.T) {
	t.Parallel()
	tr, pkCfg := testPerKeyPolicyTracker(t)
	pkCfg.DefaultAction = PerKeyMitigationReject
	now := time.Now()
	h := uint64(1)
	for i := 0; i < 30; i++ {
		tr.observeSubmit(h, 0, "", now)
		tr.observeEnqueue(h, 0, "", now)
	}
	d1 := tr.evaluatePerKeyAdmission(0, h, 0, 30, 0, 0.5, pkCfg, now)
	if d1.Action != PerKeyMitigationReject {
		t.Fatalf("first action = %q", d1.Action)
	}
	d2 := tr.evaluatePerKeyAdmission(0, h, 0, 0, 0, 0, pkCfg, now)
	if d2.Action != PerKeyMitigationReject || d2.Reason != PerKeyAdmissionReasonCooldownActive {
		t.Fatalf("second dec = %+v, want cooldown reject", d2)
	}
}

func TestConfigurePerKeyAdmissionRequiresHotKey(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 8, reg)
	err := s.ConfigurePerKeyAdmission(PerKeyAdmissionConfig{Enabled: true})
	if !errors.Is(err, ErrInvalidPerKeyAdmissionConfig) {
		t.Fatalf("err = %v, want ErrInvalidPerKeyAdmissionConfig", err)
	}
}

func TestPerKeyPolicyRecoveryAllowsAfterWindow(t *testing.T) {
	t.Parallel()
	tr, pkCfg := testPerKeyPolicyTracker(t)
	pkCfg.DefaultAction = PerKeyMitigationReject
	pkCfg.RecoveryWindow = time.Millisecond
	now := time.Now()
	h := uint64(99)
	for i := 0; i < 30; i++ {
		tr.observeSubmit(h, 0, "", now)
		tr.observeEnqueue(h, 0, "", now)
	}
	_ = tr.evaluatePerKeyAdmission(0, h, 0, 30, 0, 0.5, pkCfg, now)
	idx := tr.index[h]
	e := &tr.entries[idx]
	e.queuedApprox = 0
	e.submittedApprox = 0
	e.rejectedApprox = 0
	e.recoveryUntilUnixNano = now.Add(-time.Second).UnixNano()
	e.cooldownUntilUnixNano = 0
	e.lastAction = PerKeyMitigationReject
	later := now.Add(2 * time.Second)
	dec := tr.evaluatePerKeyAdmission(0, h, 0, 0, 0, 0, pkCfg, later)
	if dec.Action != PerKeyMitigationAllow {
		t.Fatalf("action = %q, want allow after recovery", dec.Action)
	}
}

func TestPerKeyPolicyDominantOnlyRejects(t *testing.T) {
	t.Parallel()
	tr, pkCfg := testPerKeyPolicyTracker(t)
	pkCfg.MinStatus = HotKeyStatusDominant
	pkCfg.DefaultAction = PerKeyMitigationReject
	now := time.Now()
	for i := uint64(1); i < 8; i++ {
		tr.observeSubmit(i, 0, "", now)
		tr.observeEnqueue(i, 0, "", now)
	}
	warm := uint64(10)
	for i := 0; i < 5; i++ {
		tr.observeSubmit(warm, 0, "", now)
		tr.observeEnqueue(warm, 0, "", now)
	}
	decWarm := tr.evaluatePerKeyAdmission(0, warm, 0, 13, 0, 0.5, pkCfg, now)
	if decWarm.Action != PerKeyMitigationAllow {
		t.Fatalf("warm action = %q, want allow below dominant threshold", decWarm.Action)
	}
	hot := uint64(42)
	for i := 0; i < 50; i++ {
		tr.observeSubmit(hot, 0, "", now)
		tr.observeEnqueue(hot, 0, "", now)
	}
	decHot := tr.evaluatePerKeyAdmission(0, hot, 0, 54, 0, 0.5, pkCfg, now)
	if decHot.Action != PerKeyMitigationReject {
		t.Fatalf("hot action = %q, want reject for dominant key", decHot.Action)
	}
}

func TestPerKeyPolicyShedOnlyWhenConfigured(t *testing.T) {
	t.Parallel()
	tr, pkCfg := testPerKeyPolicyTracker(t)
	now := time.Now()
	h := uint64(3)
	for i := 0; i < 40; i++ {
		tr.observeSubmit(h, 0, "", now)
		tr.observeEnqueue(h, 0, "", now)
	}
	dec := tr.evaluatePerKeyAdmission(0, h, 0, 40, 0, 0.5, pkCfg, now)
	if dec.Action == PerKeyMitigationShed {
		t.Fatalf("default throttle config must not shed, got %q", dec.Action)
	}
	tr2, pkShed := testPerKeyPolicyTracker(t)
	pkShed.DefaultAction = PerKeyMitigationShed
	for i := 0; i < 40; i++ {
		tr2.observeSubmit(h, 0, "", now)
		tr2.observeEnqueue(h, 0, "", now)
	}
	dec2 := tr2.evaluatePerKeyAdmission(0, h, 0, 40, 0, 0.5, pkShed, now)
	if dec2.Action != PerKeyMitigationShed {
		t.Fatalf("action = %q, want shed when configured", dec2.Action)
	}
}

func TestPerKeyPolicyMaxInflightThrottles(t *testing.T) {
	t.Parallel()
	tr, pkCfg := testPerKeyPolicyTracker(t)
	pkCfg.MaxInflightPerKey = 1
	pkCfg.PressureRatioThreshold = 0.99
	now := time.Now()
	h := uint64(88)
	tr.observeSubmit(h, 0, "", now)
	tr.observeInflightStart(h, now)
	tr.observeInflightStart(h, now)
	dec := tr.evaluatePerKeyAdmission(0, h, 0, 0, 0, 0, pkCfg, now)
	if dec.Action != PerKeyMitigationThrottle || dec.Reason != PerKeyAdmissionReasonMaxInflightPerKey {
		t.Fatalf("dec = %+v, want max_inflight throttle", dec)
	}
}

func TestPerKeyPolicyInflightObserve(t *testing.T) {
	t.Parallel()
	tr, _ := testPerKeyPolicyTracker(t)
	now := time.Now()
	h := uint64(5)
	tr.observeSubmit(h, 0, "", now)
	tr.observeInflightStart(h, now)
	tr.observeInflightStart(h, now)
	tr.observeInflightEnd(h, now)
	idx := tr.index[h]
	if tr.entries[idx].inflightApprox != 1 {
		t.Fatalf("inflight = %d, want 1", tr.entries[idx].inflightApprox)
	}
}
