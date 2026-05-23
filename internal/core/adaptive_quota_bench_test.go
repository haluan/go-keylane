// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"
	"time"
)

func BenchmarkAdaptiveQuotaDecisionTick(b *testing.B) {
	cfg := AdaptiveQuotaConfig{
		Enabled: true, WarmupDuration: 0, CooldownDuration: 0,
		PressureHigh: 0.85, PressureLow: 0.60,
		QueueWaitHigh: 10 * time.Millisecond, IncreaseStep: 1, DecreaseStep: 1,
		MaxAdjustmentsPerTick: 1, EnableIncrease: true, EnableDecrease: true,
	}
	policies := []resolvedLaneAdaptivePolicy{
		{LaneID: 0, Lane: "a", Class: LaneClassCritical, Enabled: true, MinQuota: 1, MaxQuota: 8, AllowIncrease: true},
		{LaneID: 1, Lane: "b", Class: LaneClassBestEffort, Enabled: true, MinQuota: 1, MaxQuota: 4, AllowDecrease: true},
	}
	snap := AdaptiveSignalSnapshot{
		GlobalPressure: 0.5,
		Lanes: []LaneAdaptiveSignal{
			{LaneID: 0, CurrentQuota: 2, QueueWaitMax: 30 * time.Millisecond, QueueWaitSamples: 10},
			{LaneID: 1, CurrentQuota: 2, QueueWaitSamples: 10},
		},
	}
	state := newAdaptiveControllerState(time.Now().Add(-time.Hour), 2)
	now := time.Now()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = EvaluateAdaptiveQuota(cfg, policies, snap, state, now)
	}
}

func BenchmarkAdaptiveQuotaDecisionTick4Lanes(b *testing.B) {
	benchmarkAdaptiveQuotaDecisionTickLanes(b, 4)
}

func BenchmarkAdaptiveQuotaDecisionTick16Lanes(b *testing.B) {
	benchmarkAdaptiveQuotaDecisionTickLanes(b, 16)
}

func BenchmarkAdaptiveQuotaDecisionTick64Lanes(b *testing.B) {
	benchmarkAdaptiveQuotaDecisionTickLanes(b, 64)
}

func benchmarkAdaptiveQuotaDecisionTickLanes(b *testing.B, n int) {
	cfg := AdaptiveQuotaConfig{
		Enabled: true, WarmupDuration: 0, CooldownDuration: 0,
		PressureHigh: 0.85, PressureLow: 0.60,
		IncreaseStep: 1, DecreaseStep: 1,
		MaxAdjustmentsPerTick: 1, EnableIncrease: true, EnableDecrease: true,
	}
	policies := make([]resolvedLaneAdaptivePolicy, n)
	lanes := make([]LaneAdaptiveSignal, n)
	for i := 0; i < n; i++ {
		policies[i] = resolvedLaneAdaptivePolicy{
			LaneID: LaneID(i), Lane: "l", Class: LaneClassNormal,
			Enabled: true, MinQuota: 1, MaxQuota: 8, AllowIncrease: true, AllowDecrease: true,
		}
		lanes[i] = LaneAdaptiveSignal{
			LaneID: LaneID(i), CurrentQuota: 2, QueueWaitSamples: 5,
		}
	}
	snap := AdaptiveSignalSnapshot{GlobalPressure: 0.5, Lanes: lanes}
	state := newAdaptiveControllerState(time.Now().Add(-time.Hour), n)
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EvaluateAdaptiveQuota(cfg, policies, snap, state, now)
	}
}

func BenchmarkAdaptiveQuotaWithOverloadSignals(b *testing.B) {
	reg, _ := NewLaneRegistry(map[string]int{"critical": 2, "best_effort": 1})
	s, _ := NewScheduler(1, 2, 100, reg)
	policies := resolveAdaptiveLanePolicies(reg, s, []LaneAdaptivePolicy{
		{Lane: "critical", Class: LaneClassCritical, Enabled: true, MinQuota: 1, MaxQuota: 8, AllowIncrease: true},
		{Lane: "best_effort", Class: LaneClassBestEffort, Enabled: true, MinQuota: 1, MaxQuota: 4, AllowDecrease: true},
	}, map[string]int{"critical": 2, "best_effort": 1})
	cfg := AdaptiveQuotaConfig{
		Enabled: true, WarmupDuration: 0, CooldownDuration: 0,
		PressureHigh: 0.85, PressureLow: 0.60, DecreaseStep: 1,
		MaxAdjustmentsPerTick: 1, EnableDecrease: true,
	}
	state := newAdaptiveControllerState(time.Now().Add(-time.Hour), 2)
	now := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sig := buildAdaptiveSignalSnapshot(s, reg, policies, 1)
		_ = EvaluateAdaptiveQuota(cfg, policies, sig, state, now)
	}
}
