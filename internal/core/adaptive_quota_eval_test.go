// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"
	"time"
)

func adaptiveEvalCfg() AdaptiveQuotaConfig {
	return AdaptiveQuotaConfig{
		Enabled:               true,
		EvaluationInterval:    time.Second,
		WarmupDuration:        0,
		CooldownDuration:      0,
		PressureHigh:          0.85,
		PressureLow:           0.60,
		QueueWaitHigh:         10 * time.Millisecond,
		RunTimeHigh:           time.Second,
		IncreaseStep:          1,
		DecreaseStep:          1,
		MaxAdjustmentsPerTick: 1,
		EnableIncrease:        true,
		EnableDecrease:        true,
	}
}

func TestEvaluateAdaptiveQuotaCriticalIncreasesOnQueueWait(t *testing.T) {
	cfg := adaptiveEvalCfg()
	policies := []resolvedLaneAdaptivePolicy{{
		LaneID: LaneID(0), Lane: "payment", Class: LaneClassCritical,
		Enabled: true, MinQuota: 1, MaxQuota: 8, AllowIncrease: true, AllowDecrease: false,
		TargetQueueWait: 20 * time.Millisecond,
	}}
	snap := AdaptiveSignalSnapshot{
		GlobalPressure: 0.45,
		Lanes: []LaneAdaptiveSignal{{
			LaneID: LaneID(0), Lane: "payment", Class: LaneClassCritical,
			CurrentQuota: 2, MinQuota: 1, MaxQuota: 8,
			QueueWaitMax: 40 * time.Millisecond, QueueWaitSamples: 5,
		}},
	}
	state := newAdaptiveControllerState(time.Now().Add(-time.Minute), 1)
	decisions := EvaluateAdaptiveQuota(cfg, policies, snap, state, time.Now())
	if len(decisions) != 1 {
		t.Fatalf("len = %d, want 1", len(decisions))
	}
	d := decisions[0]
	if d.Action != QuotaAdjustmentIncrease {
		t.Fatalf("action = %q, want increase", d.Action)
	}
	if d.Reason != QuotaReasonCriticalQueueWaitHigh {
		t.Fatalf("reason = %q", d.Reason)
	}
	if d.NewQuota != 3 {
		t.Fatalf("new quota = %d, want 3", d.NewQuota)
	}
}

func TestEvaluateAdaptiveQuotaHoldsCriticalOnHighGlobalPressure(t *testing.T) {
	cfg := adaptiveEvalCfg()
	policies := []resolvedLaneAdaptivePolicy{{
		LaneID: LaneID(0), Lane: "payment", Class: LaneClassCritical,
		Enabled: true, MinQuota: 1, MaxQuota: 8, AllowIncrease: true, AllowDecrease: false,
		TargetQueueWait: 20 * time.Millisecond,
	}}
	snap := AdaptiveSignalSnapshot{
		GlobalPressure: 0.91,
		Lanes: []LaneAdaptiveSignal{{
			LaneID: LaneID(0), Lane: "payment", Class: LaneClassCritical,
			CurrentQuota: 2, QueueWaitMax: 80 * time.Millisecond, QueueWaitSamples: 5,
		}},
	}
	state := newAdaptiveControllerState(time.Now().Add(-time.Minute), 1)
	d := EvaluateAdaptiveQuota(cfg, policies, snap, state, time.Now())[0]
	if d.Action != QuotaAdjustmentHold {
		t.Fatalf("action = %q, want hold", d.Action)
	}
	if d.Reason != QuotaReasonGlobalPressureHigh && d.Reason != QuotaReasonDecreaseDisabled {
		t.Fatalf("reason = %q", d.Reason)
	}
}

func TestEvaluateAdaptiveQuotaBestEffortDecreasesOnHighPressure(t *testing.T) {
	cfg := adaptiveEvalCfg()
	policies := []resolvedLaneAdaptivePolicy{{
		LaneID: LaneID(0), Lane: "report", Class: LaneClassBestEffort,
		Enabled: true, MinQuota: 1, MaxQuota: 4, AllowIncrease: false, AllowDecrease: true,
	}}
	snap := AdaptiveSignalSnapshot{
		GlobalPressure: 0.92,
		Lanes: []LaneAdaptiveSignal{{
			LaneID: LaneID(0), Lane: "report", Class: LaneClassBestEffort,
			CurrentQuota: 2, MinQuota: 1, MaxQuota: 4, QueueWaitSamples: 3,
		}},
	}
	state := newAdaptiveControllerState(time.Now().Add(-time.Minute), 1)
	d := EvaluateAdaptiveQuota(cfg, policies, snap, state, time.Now())[0]
	if d.Action != QuotaAdjustmentDecrease {
		t.Fatalf("action = %q, want decrease", d.Action)
	}
	if d.NewQuota != 1 {
		t.Fatalf("new quota = %d, want 1", d.NewQuota)
	}
}

func TestEvaluateAdaptiveQuotaWarmupHolds(t *testing.T) {
	cfg := adaptiveEvalCfg()
	cfg.WarmupDuration = time.Hour
	policies := []resolvedLaneAdaptivePolicy{{
		LaneID: LaneID(0), Lane: "report", Class: LaneClassBestEffort,
		Enabled: true, MinQuota: 1, MaxQuota: 4, AllowDecrease: true,
	}}
	snap := AdaptiveSignalSnapshot{
		GlobalPressure: 0.92,
		Lanes: []LaneAdaptiveSignal{{
			CurrentQuota: 2, QueueWaitSamples: 3,
		}},
	}
	state := newAdaptiveControllerState(time.Now(), 1)
	d := EvaluateAdaptiveQuota(cfg, policies, snap, state, time.Now())[0]
	if d.Reason != QuotaReasonWarmupActive {
		t.Fatalf("reason = %q, want warmup", d.Reason)
	}
}

func TestEvaluateAdaptiveQuotaCooldownHolds(t *testing.T) {
	cfg := adaptiveEvalCfg()
	cfg.CooldownDuration = time.Hour
	policies := []resolvedLaneAdaptivePolicy{{
		LaneID: LaneID(0), Lane: "report", Class: LaneClassBestEffort,
		Enabled: true, MinQuota: 1, MaxQuota: 4, AllowDecrease: true,
	}}
	snap := AdaptiveSignalSnapshot{
		GlobalPressure: 0.92,
		Lanes: []LaneAdaptiveSignal{{
			LaneID: LaneID(0), CurrentQuota: 2, QueueWaitSamples: 3,
		}},
	}
	state := newAdaptiveControllerState(time.Now().Add(-time.Hour), 1)
	state.LastAdjusted[LaneID(0)] = time.Now()
	d := EvaluateAdaptiveQuota(cfg, policies, snap, state, time.Now())[0]
	if d.Reason != QuotaReasonCooldownActive {
		t.Fatalf("reason = %q, want cooldown", d.Reason)
	}
}

func TestEvaluateAdaptiveQuotaBestEffortDoesNotIncrease(t *testing.T) {
	cfg := adaptiveEvalCfg()
	policies := []resolvedLaneAdaptivePolicy{{
		LaneID: LaneID(0), Lane: "report", Class: LaneClassBestEffort,
		Enabled: true, MinQuota: 1, MaxQuota: 4, AllowIncrease: false, AllowDecrease: true,
	}}
	snap := AdaptiveSignalSnapshot{
		GlobalPressure: 0.40,
		Lanes: []LaneAdaptiveSignal{{
			CurrentQuota: 2, QueueWaitMax: 100 * time.Millisecond, QueueWaitSamples: 5,
		}},
	}
	state := newAdaptiveControllerState(time.Now().Add(-time.Hour), 1)
	d := EvaluateAdaptiveQuota(cfg, policies, snap, state, time.Now())[0]
	if d.Action == QuotaAdjustmentIncrease {
		t.Fatal("best-effort should not increase")
	}
}

func TestEvaluateAdaptiveQuotaNormalIncreasesWhenAllowed(t *testing.T) {
	cfg := adaptiveEvalCfg()
	policies := []resolvedLaneAdaptivePolicy{{
		LaneID: LaneID(0), Lane: "read", Class: LaneClassNormal,
		Enabled: true, MinQuota: 1, MaxQuota: 8, AllowIncrease: true, AllowDecrease: true,
		TargetQueueWait: 20 * time.Millisecond,
	}}
	snap := AdaptiveSignalSnapshot{
		GlobalPressure: 0.50,
		Lanes: []LaneAdaptiveSignal{{
			LaneID: LaneID(0), CurrentQuota: 2,
			QueueWaitMax: 30 * time.Millisecond, QueueWaitSamples: 5,
		}},
	}
	state := newAdaptiveControllerState(time.Now().Add(-time.Hour), 1)
	d := EvaluateAdaptiveQuota(cfg, policies, snap, state, time.Now())[0]
	if d.Action != QuotaAdjustmentIncrease || d.Reason != QuotaReasonNormalQueueWaitHigh {
		t.Fatalf("got action=%q reason=%q", d.Action, d.Reason)
	}
}

func TestEvaluateAdaptiveQuotaBackgroundDecreasesOnHighPressure(t *testing.T) {
	cfg := adaptiveEvalCfg()
	policies := []resolvedLaneAdaptivePolicy{{
		LaneID: LaneID(0), Lane: "webhook", Class: LaneClassBackground,
		Enabled: true, MinQuota: 1, MaxQuota: 4, AllowIncrease: false, AllowDecrease: true,
	}}
	snap := AdaptiveSignalSnapshot{
		GlobalPressure: 0.90,
		Lanes: []LaneAdaptiveSignal{{
			LaneID: LaneID(0), CurrentQuota: 3, QueueWaitSamples: 2,
		}},
	}
	state := newAdaptiveControllerState(time.Now().Add(-time.Hour), 1)
	d := EvaluateAdaptiveQuota(cfg, policies, snap, state, time.Now())[0]
	if d.Action != QuotaAdjustmentDecrease {
		t.Fatalf("action = %q, want decrease", d.Action)
	}
}

func TestEvaluateAdaptiveQuotaDecreasePriorityBestEffortBeforeBackground(t *testing.T) {
	cfg := adaptiveEvalCfg()
	cfg.MaxAdjustmentsPerTick = 1
	policies := []resolvedLaneAdaptivePolicy{
		{LaneID: LaneID(0), Lane: "bg", Class: LaneClassBackground, Enabled: true, MinQuota: 1, MaxQuota: 4, AllowDecrease: true},
		{LaneID: LaneID(1), Lane: "be", Class: LaneClassBestEffort, Enabled: true, MinQuota: 1, MaxQuota: 4, AllowDecrease: true},
	}
	snap := AdaptiveSignalSnapshot{
		GlobalPressure: 0.92,
		Lanes: []LaneAdaptiveSignal{
			{LaneID: LaneID(0), CurrentQuota: 3, QueueWaitSamples: 2},
			{LaneID: LaneID(1), CurrentQuota: 3, QueueWaitSamples: 2},
		},
	}
	state := newAdaptiveControllerState(time.Now().Add(-time.Hour), 2)
	decisions := EvaluateAdaptiveQuota(cfg, policies, snap, state, time.Now())
	var decreased []string
	for _, d := range decisions {
		if d.Action == QuotaAdjustmentDecrease {
			decreased = append(decreased, d.Lane)
		}
	}
	if len(decreased) != 1 || decreased[0] != "be" {
		t.Fatalf("decreased lanes = %v, want [be]", decreased)
	}
}

func TestEvaluateAdaptiveQuotaCriticalDoesNotDecreaseByDefault(t *testing.T) {
	cfg := adaptiveEvalCfg()
	policies := []resolvedLaneAdaptivePolicy{{
		LaneID: LaneID(0), Lane: "pay", Class: LaneClassCritical,
		Enabled: true, MinQuota: 1, MaxQuota: 8, AllowIncrease: true, AllowDecrease: false,
	}}
	snap := AdaptiveSignalSnapshot{
		GlobalPressure: 0.95,
		Lanes: []LaneAdaptiveSignal{{
			LaneID: LaneID(0), CurrentQuota: 2, QueueWaitSamples: 5,
		}},
	}
	state := newAdaptiveControllerState(time.Now().Add(-time.Hour), 1)
	d := EvaluateAdaptiveQuota(cfg, policies, snap, state, time.Now())[0]
	if d.Action == QuotaAdjustmentDecrease {
		t.Fatal("critical should not decrease by default")
	}
}

func TestEvaluateAdaptiveQuotaDoesNotExceedMaxBound(t *testing.T) {
	cfg := adaptiveEvalCfg()
	policies := []resolvedLaneAdaptivePolicy{{
		LaneID: LaneID(0), Lane: "pay", Class: LaneClassCritical,
		Enabled: true, MinQuota: 1, MaxQuota: 2, AllowIncrease: true, AllowDecrease: false,
		TargetQueueWait: 5 * time.Millisecond,
	}}
	snap := AdaptiveSignalSnapshot{
		GlobalPressure: 0.40,
		Lanes: []LaneAdaptiveSignal{{
			LaneID: LaneID(0), CurrentQuota: 2,
			QueueWaitMax: 50 * time.Millisecond, QueueWaitSamples: 5,
		}},
	}
	state := newAdaptiveControllerState(time.Now().Add(-time.Hour), 1)
	d := EvaluateAdaptiveQuota(cfg, policies, snap, state, time.Now())[0]
	if d.Action != QuotaAdjustmentHold || d.Reason != QuotaReasonAtMaxBound {
		t.Fatalf("action=%q reason=%q, want hold at_max", d.Action, d.Reason)
	}
}

func TestEvaluateAdaptiveQuotaDoesNotGoBelowMinBound(t *testing.T) {
	cfg := adaptiveEvalCfg()
	policies := []resolvedLaneAdaptivePolicy{{
		LaneID: LaneID(0), Lane: "be", Class: LaneClassBestEffort,
		Enabled: true, MinQuota: 1, MaxQuota: 4, AllowDecrease: true,
	}}
	snap := AdaptiveSignalSnapshot{
		GlobalPressure: 0.92,
		Lanes: []LaneAdaptiveSignal{{
			LaneID: LaneID(0), CurrentQuota: 1, QueueWaitSamples: 3,
		}},
	}
	state := newAdaptiveControllerState(time.Now().Add(-time.Hour), 1)
	d := EvaluateAdaptiveQuota(cfg, policies, snap, state, time.Now())[0]
	if d.Reason != QuotaReasonAtMinBound {
		t.Fatalf("reason = %q, want at_min_bound", d.Reason)
	}
}

func TestEvaluateAdaptiveQuotaInsufficientSamplesHolds(t *testing.T) {
	cfg := adaptiveEvalCfg()
	policies := []resolvedLaneAdaptivePolicy{{
		LaneID: LaneID(0), Lane: "read", Class: LaneClassNormal,
		Enabled: true, AllowIncrease: true, MaxQuota: 8,
	}}
	snap := AdaptiveSignalSnapshot{
		GlobalPressure: 0.50,
		Lanes: []LaneAdaptiveSignal{{
			LaneID: LaneID(0), CurrentQuota: 2, QueueWaitSamples: 0,
		}},
	}
	state := newAdaptiveControllerState(time.Now().Add(-time.Hour), 1)
	d := EvaluateAdaptiveQuota(cfg, policies, snap, state, time.Now())[0]
	if d.Reason != QuotaReasonInsufficientSamples {
		t.Fatalf("reason = %q", d.Reason)
	}
}

func TestEvaluateAdaptiveQuotaRunTimeTooHighBlocksIncrease(t *testing.T) {
	cfg := adaptiveEvalCfg()
	cfg.RunTimeHigh = 100 * time.Millisecond
	policies := []resolvedLaneAdaptivePolicy{{
		LaneID: LaneID(0), Lane: "pay", Class: LaneClassCritical,
		Enabled: true, AllowIncrease: true, MaxQuota: 8, TargetQueueWait: 5 * time.Millisecond,
	}}
	snap := AdaptiveSignalSnapshot{
		GlobalPressure: 0.40,
		Lanes: []LaneAdaptiveSignal{{
			LaneID: LaneID(0), CurrentQuota: 2,
			QueueWaitMax: 50 * time.Millisecond, QueueWaitSamples: 5,
			RunMax: 200 * time.Millisecond,
		}},
	}
	state := newAdaptiveControllerState(time.Now().Add(-time.Hour), 1)
	d := EvaluateAdaptiveQuota(cfg, policies, snap, state, time.Now())[0]
	if d.Reason != QuotaReasonRunTimeTooHigh {
		t.Fatalf("reason = %q", d.Reason)
	}
}

func TestEvaluateAdaptiveQuotaConfigDecreaseDisabled(t *testing.T) {
	cfg := adaptiveEvalCfg()
	cfg.EnableDecrease = false
	policies := []resolvedLaneAdaptivePolicy{{
		LaneID: LaneID(0), Lane: "be", Class: LaneClassBestEffort,
		Enabled: true, AllowDecrease: true, MinQuota: 1, MaxQuota: 4,
	}}
	snap := AdaptiveSignalSnapshot{
		GlobalPressure: 0.92,
		Lanes:          []LaneAdaptiveSignal{{LaneID: LaneID(0), CurrentQuota: 3, QueueWaitSamples: 3}},
	}
	state := newAdaptiveControllerState(time.Now().Add(-time.Hour), 1)
	d := EvaluateAdaptiveQuota(cfg, policies, snap, state, time.Now())[0]
	if d.Action == QuotaAdjustmentDecrease {
		t.Fatal("want no decrease when cfg.EnableDecrease=false")
	}
}

func TestEvaluateAdaptiveQuotaConfigIncreaseDisabled(t *testing.T) {
	cfg := adaptiveEvalCfg()
	cfg.EnableIncrease = false
	policies := []resolvedLaneAdaptivePolicy{{
		LaneID: LaneID(0), Lane: "pay", Class: LaneClassCritical,
		Enabled: true, AllowIncrease: true, MaxQuota: 8, TargetQueueWait: 5 * time.Millisecond,
	}}
	snap := AdaptiveSignalSnapshot{
		GlobalPressure: 0.40,
		Lanes: []LaneAdaptiveSignal{{
			LaneID: LaneID(0), CurrentQuota: 2,
			QueueWaitMax: 50 * time.Millisecond, QueueWaitSamples: 5,
		}},
	}
	state := newAdaptiveControllerState(time.Now().Add(-time.Hour), 1)
	d := EvaluateAdaptiveQuota(cfg, policies, snap, state, time.Now())[0]
	if d.Action == QuotaAdjustmentIncrease {
		t.Fatal("want no increase when cfg.EnableIncrease=false")
	}
}

func TestEvaluateAdaptiveQuotaBestEffortDecreasesOnLocalizedOverload(t *testing.T) {
	cfg := adaptiveEvalCfg()
	policies := []resolvedLaneAdaptivePolicy{{
		LaneID: LaneID(0), Lane: "be", Class: LaneClassBestEffort,
		Enabled: true, MinQuota: 1, MaxQuota: 4, AllowDecrease: true,
	}}
	snap := AdaptiveSignalSnapshot{
		GlobalPressure: 0.72,
		Lanes: []LaneAdaptiveSignal{{
			LaneID: LaneID(0), CurrentQuota: 3, QueueWaitSamples: 2,
			OverloadShedCount: 5,
		}},
	}
	state := newAdaptiveControllerState(time.Now().Add(-time.Hour), 1)
	d := EvaluateAdaptiveQuota(cfg, policies, snap, state, time.Now())[0]
	if d.Action != QuotaAdjustmentDecrease {
		t.Fatalf("action = %q, want decrease on localized overload", d.Action)
	}
}
