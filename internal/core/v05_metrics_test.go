// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"
	"time"
)

func TestPerKeyAdmissionDecisionCountersIncrement(t *testing.T) {
	t.Parallel()
	reg, err := NewLaneRegistry(map[string]int{"default": 2})
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewScheduler(1, 1, 32, reg)
	if err != nil {
		t.Fatal(err)
	}
	hkCfg := HotKeyConfig{
		Enabled: true, MaxTrackedKeysPerShard: 16,
		DetectionWindow: time.Minute, HotKeyDepthRatio: 0.3,
	}
	s.ConfigureHotKey(hkCfg)
	pkCfg := PerKeyAdmissionConfig{
		Enabled: true, MinStatus: HotKeyStatusCandidate,
		DefaultAction: PerKeyMitigationThrottle, PressureRatioThreshold: 0.2,
	}
	if err := s.ConfigurePerKeyAdmission(pkCfg); err != nil {
		t.Fatal(err)
	}
	h := uint64(42)
	now := time.Now()
	hk := s.hotKeyTrackers[0]
	hk.observeSubmit(h, 0, "", now)
	hk.observeEnqueue(h, 0, "", now)
	for i := 0; i < 20; i++ {
		_ = s.EvaluatePerKeyAdmissionWithConfig(0, h, 0, pkCfg)
	}
	totals := s.PerKeyAdmissionDecisionTotals()
	if len(totals) == 0 {
		t.Fatal("expected per-key decision counter buckets")
	}
	var throttleCount uint64
	for _, b := range totals {
		if b.Action == "throttle" {
			throttleCount += b.Count
		}
	}
	if throttleCount == 0 {
		t.Fatalf("expected throttle decisions recorded, totals=%v", totals)
	}
}
