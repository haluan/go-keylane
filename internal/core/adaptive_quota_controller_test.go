// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

func testLaneQuotaApplier(t *testing.T, check func(lane string, quota uint32, expectedVer uint64)) LaneQuotaApplier {
	t.Helper()
	return func(ctx context.Context, lane string, quota uint32, expectedVer uint64) (uint64, error) {
		_ = ctx
		check(lane, quota, expectedVer)
		return 1, nil
	}
}

func TestAdaptiveQuotaControllerStopIdempotent(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)
	cfg := AdaptiveQuotaConfig{
		Enabled: true, EvaluationInterval: time.Millisecond,
		WarmupDuration: 0, CooldownDuration: 0,
	}
	c := NewAdaptiveQuotaController(s, reg, cfg, nil, map[string]int{"default": 1}, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	c.Stop()
	c.Stop()
	_, running, _, _, _, _, _ := c.Snapshot()
	if running {
		t.Error("controller still running after double Stop")
	}
}

func TestAdaptiveQuotaControllerDecreasesOnLocalizedOverloadDegrade(t *testing.T) {
	reg, err := NewLaneRegistry(map[string]int{"background": 3})
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewScheduler(1, 1, 10, reg)
	if err != nil {
		t.Fatal(err)
	}
	laneID, ok := reg.Lookup("background")
	if !ok {
		t.Fatal("lane not found")
	}
	s.RecordOverloadDecision(laneID, OverloadActionDegrade)

	explicit := []LaneAdaptivePolicy{
		{Lane: "background", Class: LaneClassBackground, Enabled: true, MinQuota: 1, MaxQuota: 4, AllowDecrease: true},
	}
	cfg := AdaptiveQuotaConfig{
		Enabled: true, EvaluationInterval: time.Millisecond,
		WarmupDuration: 0, CooldownDuration: 0,
		PressureHigh: 0.85, PressureLow: 0.60, DecreaseStep: 1,
		MaxAdjustmentsPerTick: 1, EnableIncrease: true, EnableDecrease: true,
	}

	var applied uint32
	c := NewAdaptiveQuotaController(
		s, reg, cfg, explicit, map[string]int{"background": 3},
		testLaneQuotaApplier(t, func(lane string, quota uint32, _ uint64) {
			if lane != "background" || quota != 2 {
				t.Fatalf("apply lane=%q quota=%d", lane, quota)
			}
			applied = quota
		}),
		nil,
	)
	c.RunTickContext(context.Background())
	if applied != 2 {
		t.Fatalf("applied quota = %d, want decrease to 2 on degrade counter", applied)
	}
}

func TestAdaptiveQuotaControllerDecreasesOnLocalizedOverloadCounters(t *testing.T) {
	reg, err := NewLaneRegistry(map[string]int{"best_effort": 3})
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewScheduler(1, 1, 10, reg)
	if err != nil {
		t.Fatal(err)
	}
	laneID, ok := reg.Lookup("best_effort")
	if !ok {
		t.Fatal("lane not found")
	}
	s.RecordOverloadDecision(laneID, OverloadActionShed)

	explicit := []LaneAdaptivePolicy{
		{Lane: "best_effort", Class: LaneClassBestEffort, Enabled: true, MinQuota: 1, MaxQuota: 4, AllowDecrease: true},
	}
	cfg := AdaptiveQuotaConfig{
		Enabled: true, EvaluationInterval: time.Millisecond,
		WarmupDuration: 0, CooldownDuration: 0,
		PressureHigh: 0.85, PressureLow: 0.60, DecreaseStep: 1,
		MaxAdjustmentsPerTick: 1, EnableIncrease: true, EnableDecrease: true,
	}

	var applied uint32
	c := NewAdaptiveQuotaController(
		s, reg, cfg, explicit, map[string]int{"best_effort": 3},
		testLaneQuotaApplier(t, func(lane string, quota uint32, _ uint64) {
			if lane != "best_effort" || quota != 2 {
				t.Fatalf("apply lane=%q quota=%d", lane, quota)
			}
			applied = quota
		}),
		nil,
	)
	c.RunTickContext(context.Background())
	if applied != 2 {
		t.Fatalf("applied quota = %d, want decrease to 2", applied)
	}
}

func TestAdaptiveQuotaControllerThreeLanesLocalizedOverload(t *testing.T) {
	reg, err := NewLaneRegistry(map[string]int{"default": 2, "critical": 2, "best_effort": 3})
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewScheduler(1, 1, 10, reg)
	if err != nil {
		t.Fatal(err)
	}
	laneID, ok := reg.Lookup("best_effort")
	if !ok {
		t.Fatal("lane not found")
	}
	s.RecordOverloadDecision(laneID, OverloadActionShed)

	explicit := []LaneAdaptivePolicy{
		{Lane: "best_effort", Class: LaneClassBestEffort, Enabled: true, MinQuota: 1, MaxQuota: 4, AllowDecrease: true},
	}
	cfg := AdaptiveQuotaConfig{
		Enabled: true, EvaluationInterval: time.Millisecond,
		WarmupDuration: 0, CooldownDuration: 0,
		PressureHigh: 0.85, PressureLow: 0.60, DecreaseStep: 1,
		MaxAdjustmentsPerTick: 1, EnableIncrease: true, EnableDecrease: true,
	}

	var applied uint32
	c := NewAdaptiveQuotaController(
		s, reg, cfg, explicit, map[string]int{"default": 2, "critical": 2, "best_effort": 3},
		testLaneQuotaApplier(t, func(lane string, quota uint32, _ uint64) {
			if lane != "best_effort" || quota != 2 {
				t.Fatalf("apply lane=%q quota=%d", lane, quota)
			}
			applied = quota
		}),
		nil,
	)
	c.RunTickContext(context.Background())
	if applied != 2 {
		sig := buildAdaptiveSignalSnapshot(s, reg, c.policies, c.policyVersion)
		t.Fatalf("applied quota = %d, want 2; snap=%+v", applied, sig.Lanes)
	}
}

func TestAdaptiveQuotaControllerApplyFailureEmitsHook(t *testing.T) {
	reg, err := NewLaneRegistry(map[string]int{"best_effort": 3})
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewScheduler(1, 1, 10, reg)
	if err != nil {
		t.Fatal(err)
	}
	laneID, _ := reg.Lookup("best_effort")
	s.RecordOverloadDecision(laneID, OverloadActionShed)

	cfg := AdaptiveQuotaConfig{
		Enabled: true, WarmupDuration: 0, CooldownDuration: 0,
		PressureHigh: 0.85, PressureLow: 0.60, DecreaseStep: 1,
		MaxAdjustmentsPerTick: 1, EnableDecrease: true,
	}
	explicit := []LaneAdaptivePolicy{
		{Lane: "best_effort", Class: LaneClassBestEffort, Enabled: true, MinQuota: 1, MaxQuota: 4, AllowDecrease: true},
	}

	var hookReason QuotaAdjustmentReason
	c := NewAdaptiveQuotaController(
		s, reg, cfg, explicit, map[string]int{"best_effort": 3},
		func(ctx context.Context, lane string, quota uint32, expectedVer uint64) (uint64, error) {
			_ = ctx
			_ = lane
			_ = quota
			_ = expectedVer
			return 0, ErrStopped
		},
		func(d QuotaAdjustmentDecision, _ time.Time) {
			hookReason = d.Reason
		},
	)
	c.RunTickContext(context.Background())
	if hookReason != QuotaReasonUpdateFailed {
		t.Fatalf("hook reason = %q, want quota_update_failed", hookReason)
	}
	_, _, _, _, decisions, policyVer, _ := c.Snapshot()
	if policyVer == 0 {
		t.Error("PolicyVersion = 0 in snapshot")
	}
	if len(decisions) == 0 || decisions[len(decisions)-1].Reason != QuotaReasonUpdateFailed {
		t.Fatalf("last decision = %+v", decisions)
	}
}

func TestAdaptiveQuotaControllerSnapshotPolicyVersion(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)
	cfg := AdaptiveQuotaConfig{Enabled: true, EvaluationInterval: time.Second}
	c := NewAdaptiveQuotaController(s, reg, cfg, nil, map[string]int{"default": 1}, nil, nil)
	_, _, _, _, _, policyVer, _ := c.Snapshot()
	if policyVer != 1 {
		t.Fatalf("PolicyVersion = %d, want 1", policyVer)
	}
}

func TestEvaluateAdaptiveQuotaThreeLanesKeylaneHookScenario(t *testing.T) {
	reg, err := NewLaneRegistry(map[string]int{"default": 2, "critical": 2, "best_effort": 3})
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewScheduler(1, 1, 10, reg)
	if err != nil {
		t.Fatal(err)
	}
	laneID, _ := reg.Lookup("best_effort")
	s.RecordOverloadDecision(laneID, OverloadActionShed)

	explicit := []LaneAdaptivePolicy{
		{Lane: "best_effort", Class: LaneClassBestEffort, Enabled: true, MinQuota: 1, MaxQuota: 4, AllowDecrease: true},
	}
	policies := resolveAdaptiveLanePolicies(reg, s, explicit, map[string]int{"default": 2, "critical": 2, "best_effort": 3})
	cfg := AdaptiveQuotaConfig{
		Enabled: true, WarmupDuration: 0, CooldownDuration: 0,
		PressureHigh: 0.85, PressureLow: 0.60, DecreaseStep: 1,
		MaxAdjustmentsPerTick: 1, EnableIncrease: true, EnableDecrease: true,
	}
	snap := buildAdaptiveSignalSnapshot(s, reg, policies, 1)
	state := newAdaptiveControllerState(time.Now().Add(-time.Hour), reg.Len())
	decisions := EvaluateAdaptiveQuota(cfg, policies, snap, state, time.Now())
	var be QuotaAdjustmentDecision
	for i, p := range policies {
		if p.Lane == "best_effort" {
			be = decisions[i]
		}
	}
	if be.Action != QuotaAdjustmentDecrease {
		t.Fatalf("action=%q reason=%q", be.Action, be.Reason)
	}
	if be.PolicyVersion != 1 {
		t.Errorf("PolicyVersion = %d, want 1", be.PolicyVersion)
	}
}

func TestEvaluateAdaptiveQuotaMatchesKeylaneHookScenario(t *testing.T) {
	cfg := AdaptiveQuotaConfig{
		Enabled: true, WarmupDuration: 0, CooldownDuration: 0,
		PressureHigh: 0.85, PressureLow: 0.60, DecreaseStep: 1,
		MaxAdjustmentsPerTick: 1, EnableIncrease: true, EnableDecrease: true,
	}
	policies := []resolvedLaneAdaptivePolicy{{
		LaneID: LaneID(0), Lane: "best_effort", Class: LaneClassBestEffort,
		Enabled: true, MinQuota: 1, MaxQuota: 4, AllowDecrease: true,
	}}
	snap := AdaptiveSignalSnapshot{
		GlobalPressure: 0.5333333333333333,
		PolicyVersion:  1,
		Lanes: []LaneAdaptiveSignal{{
			LaneID: LaneID(0), Lane: "best_effort", CurrentQuota: 3,
			OverloadShedCount: 9, QueueWaitSamples: 0,
		}},
	}
	state := newAdaptiveControllerState(time.Now().Add(-time.Hour), 1)
	d := EvaluateAdaptiveQuota(cfg, policies, snap, state, time.Now())
	if d[0].Action != QuotaAdjustmentDecrease {
		t.Fatalf("action=%q reason=%q", d[0].Action, d[0].Reason)
	}
}

func TestEvaluateAdaptiveQuotaBackgroundIncreaseReason(t *testing.T) {
	cfg := adaptiveEvalCfg()
	policies := []resolvedLaneAdaptivePolicy{{
		LaneID: LaneID(0), Lane: "bg", Class: LaneClassBackground,
		Enabled: true, MinQuota: 1, MaxQuota: 8, AllowIncrease: true, AllowDecrease: false,
		TargetQueueWait: 50 * time.Millisecond,
	}}
	snap := AdaptiveSignalSnapshot{
		GlobalPressure: 0.4,
		PolicyVersion:  1,
		Lanes: []LaneAdaptiveSignal{{
			LaneID: LaneID(0), CurrentQuota: 2, QueueWaitMax: 80 * time.Millisecond, QueueWaitSamples: 5,
		}},
	}
	state := newAdaptiveControllerState(time.Now().Add(-time.Hour), 1)
	d := EvaluateAdaptiveQuota(cfg, policies, snap, state, time.Now())[0]
	if d.Reason != QuotaReasonBackgroundQueueWaitHigh {
		t.Fatalf("reason = %q, want background_queue_wait_high", d.Reason)
	}
}

func TestEvaluateAdaptiveQuotaBlocksIncreaseOnQueueFull(t *testing.T) {
	cfg := adaptiveEvalCfg()
	policies := []resolvedLaneAdaptivePolicy{{
		LaneID: LaneID(0), Lane: "default", Class: LaneClassNormal,
		Enabled: true, MinQuota: 1, MaxQuota: 8, AllowIncrease: true,
		TargetQueueWait: 10 * time.Millisecond,
	}}
	snap := AdaptiveSignalSnapshot{
		GlobalPressure: 0.4,
		Lanes: []LaneAdaptiveSignal{{
			LaneID: LaneID(0), CurrentQuota: 2, QueueWaitMax: 50 * time.Millisecond,
			QueueWaitSamples: 5, QueueFullCount: 3,
		}},
	}
	state := newAdaptiveControllerState(time.Now().Add(-time.Hour), 1)
	d := EvaluateAdaptiveQuota(cfg, policies, snap, state, time.Now())[0]
	if d.Reason != QuotaReasonQueueFull {
		t.Fatalf("reason = %q, want queue_full", d.Reason)
	}
}

// Ensure apply failure path does not panic when hook is nil.
func TestAdaptiveQuotaControllerApplyFailureNoHook(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"x": 2})
	s, _ := NewScheduler(1, 1, 10, reg)
	cfg := AdaptiveQuotaConfig{
		Enabled: true, WarmupDuration: 0, CooldownDuration: 0,
		PressureHigh: 0.01, PressureLow: 0.99, DecreaseStep: 1, EnableDecrease: true,
	}
	c := NewAdaptiveQuotaController(
		s, reg, cfg,
		[]LaneAdaptivePolicy{{Lane: "x", Class: LaneClassBestEffort, Enabled: true, MinQuota: 1, MaxQuota: 4, AllowDecrease: true}},
		map[string]int{"x": 2},
		func(context.Context, string, uint32, uint64) (uint64, error) {
			return 0, errors.New("fail")
		},
		nil,
	)
	c.RunTickContext(context.Background())
}
