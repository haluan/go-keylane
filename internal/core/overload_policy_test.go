// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

func overloadTestRegistry() (*LaneRegistry, OverloadPolicyInput) {
	reg, _ := NewLaneRegistry(map[string]int{
		"default": 1, "normal": 1, "critical": 2, "best": 1, "background": 1, "best_effort": 1,
	})
	policy := OverloadPolicyInput{
		Default: LaneOverloadPolicyInput{
			Class:             LaneClassNormal,
			RejectAboveRatio:  0.90,
			ShedAboveRatio:    1.00,
			DegradeAboveRatio: 1.00,
			MaxQueueDepth:     100,
			RetryAfter:        250 * time.Millisecond,
		},
		Lanes: []LaneOverloadPolicyInput{
			{Lane: "critical", Class: LaneClassCritical, RejectAboveRatio: 0.98, ShedAboveRatio: 1.0, DegradeAboveRatio: 1.0, MaxQueueDepth: 100},
			{Lane: "best", Class: LaneClassBestEffort, RejectAboveRatio: 0.75, ShedAboveRatio: 0.60, DegradeAboveRatio: 1.0, MaxQueueDepth: 100},
			{Lane: "best_effort", Class: LaneClassBestEffort, RejectAboveRatio: 0.75, ShedAboveRatio: 0.60, DegradeAboveRatio: 1.0, MaxQueueDepth: 100},
			{Lane: "background", Class: LaneClassBackground, RejectAboveRatio: 0.85, ShedAboveRatio: 0.75, DegradeAboveRatio: 1.0, MaxQueueDepth: 100},
		},
	}
	return reg, policy
}

func overloadTestPolicy() OverloadPolicyInput {
	_, policy := overloadTestRegistry()
	return policy
}

func TestEvaluateOverloadKeepBelowThresholds(t *testing.T) {
	reg, policy := overloadTestRegistry()
	snap, err := buildOverloadPolicySnapshot(reg, policy)
	if err != nil {
		t.Fatal(err)
	}
	defaultID, _ := reg.Lookup("default")
	r := evaluateOverload(snap, defaultID, OverloadSignals{GlobalPressure: 0.5})
	if r.Action != OverloadActionKeep {
		t.Fatalf("action = %q, want keep", r.Action)
	}
	if r.Reason != OverloadReasonNone {
		t.Fatalf("reason = %q, want none", r.Reason)
	}
}

func TestEvaluateOverloadQueueClosed(t *testing.T) {
	reg, policy := overloadTestRegistry()
	snap, _ := buildOverloadPolicySnapshot(reg, policy)
	id, _ := reg.Lookup("default")
	r := evaluateOverload(snap, id, OverloadSignals{QueueClosed: true})
	if r.Action != OverloadActionReject || r.Reason != OverloadReasonQueueClosed {
		t.Fatalf("got action=%q reason=%q", r.Action, r.Reason)
	}
}

func TestEvaluateOverloadQueueFull(t *testing.T) {
	reg, policy := overloadTestRegistry()
	snap, _ := buildOverloadPolicySnapshot(reg, policy)
	id, _ := reg.Lookup("default")
	r := evaluateOverload(snap, id, OverloadSignals{QueueFull: true})
	if r.Action != OverloadActionReject || r.Reason != OverloadReasonQueueFull {
		t.Fatalf("got action=%q reason=%q", r.Action, r.Reason)
	}
}

func TestEvaluateOverloadLaneDepthExceeded(t *testing.T) {
	reg, policy := overloadTestRegistry()
	snap, _ := buildOverloadPolicySnapshot(reg, policy)
	id, _ := reg.Lookup("default")
	r := evaluateOverload(snap, id, OverloadSignals{LaneDepth: 100, GlobalPressure: 0.1})
	if r.Action != OverloadActionReject || r.Reason != OverloadReasonLaneDepthExceeded {
		t.Fatalf("got action=%q reason=%q", r.Action, r.Reason)
	}
}

func TestEvaluateOverloadBestEffortShedsBeforeNormalRejects(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"normal": 1, "best": 1})
	policy := overloadTestPolicy()
	policy.Lanes = []LaneOverloadPolicyInput{
		{Lane: "best", Class: LaneClassBestEffort, RejectAboveRatio: 0.75, ShedAboveRatio: 0.60, MaxQueueDepth: 100},
	}
	snap, err := buildOverloadPolicySnapshot(reg, policy)
	if err != nil {
		t.Fatal(err)
	}
	normalID, _ := reg.Lookup("normal")
	bestID, _ := reg.Lookup("best")
	pressure := 0.70

	rBest := evaluateOverload(snap, bestID, OverloadSignals{GlobalPressure: pressure})
	if rBest.Action != OverloadActionShed {
		t.Fatalf("best: action = %q, want shed", rBest.Action)
	}
	rNormal := evaluateOverload(snap, normalID, OverloadSignals{GlobalPressure: pressure})
	if rNormal.Action != OverloadActionKeep {
		t.Fatalf("normal: action = %q, want keep", rNormal.Action)
	}
}

func TestEvaluateOverloadCriticalKeepsWhenBestEffortSheds(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"critical": 1, "best": 1})
	policy := overloadTestPolicy()
	policy.Lanes = []LaneOverloadPolicyInput{
		{Lane: "critical", Class: LaneClassCritical, RejectAboveRatio: 0.98, ShedAboveRatio: 1.0, MaxQueueDepth: 100},
		{Lane: "best", Class: LaneClassBestEffort, RejectAboveRatio: 0.75, ShedAboveRatio: 0.60, MaxQueueDepth: 100},
	}
	snap, err := buildOverloadPolicySnapshot(reg, policy)
	if err != nil {
		t.Fatal(err)
	}
	criticalID, _ := reg.Lookup("critical")
	bestID, _ := reg.Lookup("best")
	pressure := 0.70

	if r := evaluateOverload(snap, bestID, OverloadSignals{GlobalPressure: pressure}); r.Action != OverloadActionShed {
		t.Fatalf("best: %q", r.Action)
	}
	if r := evaluateOverload(snap, criticalID, OverloadSignals{GlobalPressure: pressure}); r.Action != OverloadActionKeep {
		t.Fatalf("critical: %q", r.Action)
	}
}

func TestEvaluateOverloadBackgroundShedsUnderThreshold(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"background": 1, "critical": 1})
	snap, err := buildOverloadPolicySnapshot(reg, OverloadPolicyInput{
		Default: LaneOverloadPolicyInput{
			Class: LaneClassNormal, RejectAboveRatio: 0.90, ShedAboveRatio: 1.0, MaxQueueDepth: 100,
		},
		Lanes: []LaneOverloadPolicyInput{
			{Lane: "background", Class: LaneClassBackground, RejectAboveRatio: 0.85, ShedAboveRatio: 0.75, MaxQueueDepth: 100},
			{Lane: "critical", Class: LaneClassCritical, RejectAboveRatio: 0.98, ShedAboveRatio: 1.0, MaxQueueDepth: 100},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	backgroundID, _ := reg.Lookup("background")
	criticalID, _ := reg.Lookup("critical")
	pressure := 0.78

	rBg := evaluateOverload(snap, backgroundID, OverloadSignals{GlobalPressure: pressure})
	if rBg.Action != OverloadActionShed {
		t.Fatalf("background: action = %q, want shed", rBg.Action)
	}
	if rBg.Reason != OverloadReasonBackgroundShedding {
		t.Fatalf("background: reason = %q, want %q", rBg.Reason, OverloadReasonBackgroundShedding)
	}
	rCrit := evaluateOverload(snap, criticalID, OverloadSignals{GlobalPressure: pressure})
	if rCrit.Action != OverloadActionKeep {
		t.Fatalf("critical: action = %q, want keep", rCrit.Action)
	}
}

func TestEvaluateOverloadDegradeWhenConfigured(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"degrade_lane": 1})
	snap, err := buildOverloadPolicySnapshot(reg, OverloadPolicyInput{
		Default: LaneOverloadPolicyInput{
			Class: LaneClassNormal, RejectAboveRatio: 0.90, ShedAboveRatio: 1.0,
			DegradeAboveRatio: 0.80, MaxQueueDepth: 100, RetryAfter: time.Second,
		},
		Lanes: []LaneOverloadPolicyInput{
			{Lane: "degrade_lane", Class: LaneClassNormal, RejectAboveRatio: 0.95, ShedAboveRatio: 1.0,
				DegradeAboveRatio: 0.70, MaxQueueDepth: 100, RetryAfter: 2 * time.Second},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	id, _ := reg.Lookup("degrade_lane")
	r := evaluateOverload(snap, id, OverloadSignals{GlobalPressure: 0.75})
	if r.Action != OverloadActionDegrade || r.Reason != OverloadReasonDegradePreferred {
		t.Fatalf("got action=%q reason=%q", r.Action, r.Reason)
	}
	if r.RetryAfter != 2*time.Second {
		t.Fatalf("RetryAfter = %v, want 2s", r.RetryAfter)
	}
}

func TestEvaluateOverloadRejectIncludesRetryAfter(t *testing.T) {
	reg, policy := overloadTestRegistry()
	snap, _ := buildOverloadPolicySnapshot(reg, policy)
	id, _ := reg.Lookup("default")
	r := evaluateOverload(snap, id, OverloadSignals{GlobalPressure: 0.95})
	if r.Action != OverloadActionReject {
		t.Fatalf("action = %q", r.Action)
	}
	if r.RetryAfter != 250*time.Millisecond {
		t.Fatalf("RetryAfter = %v", r.RetryAfter)
	}
}

func TestBuildOverloadPolicyRejectsDuplicateLane(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"a": 1})
	_, err := buildOverloadPolicySnapshot(reg, OverloadPolicyInput{
		Default: LaneOverloadPolicyInput{Class: LaneClassNormal, RejectAboveRatio: 0.9, MaxQueueDepth: 10},
		Lanes: []LaneOverloadPolicyInput{
			{Lane: "a", Class: LaneClassNormal, RejectAboveRatio: 0.9, MaxQueueDepth: 10},
			{Lane: "a", Class: LaneClassBackground, RejectAboveRatio: 0.5, MaxQueueDepth: 10},
		},
	})
	if !errors.Is(err, ErrInvalidOverloadPolicy) {
		t.Errorf("err = %v", err)
	}
}

func TestBuildOverloadPolicyMissingLaneUsesDefaults(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"listed": 1, "unlisted": 1})
	snap, err := buildOverloadPolicySnapshot(reg, OverloadPolicyInput{
		Default: LaneOverloadPolicyInput{
			Class: LaneClassNormal, RejectAboveRatio: 0.90, ShedAboveRatio: 1.0, MaxQueueDepth: 100,
		},
		Lanes: []LaneOverloadPolicyInput{
			{Lane: "listed", Class: LaneClassBestEffort, RejectAboveRatio: 0.75, ShedAboveRatio: 0.50, MaxQueueDepth: 100},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	listedID, _ := reg.Lookup("listed")
	unlistedID, _ := reg.Lookup("unlisted")
	if r := evaluateOverload(snap, listedID, OverloadSignals{GlobalPressure: 0.70}); r.Action != OverloadActionShed {
		t.Fatal("listed should shed at 0.70")
	}
	if r := evaluateOverload(snap, unlistedID, OverloadSignals{GlobalPressure: 0.70}); r.Action != OverloadActionKeep {
		t.Fatal("unlisted should use default 0.90 threshold")
	}
}

func TestOverloadPolicyDoesNotInterruptRunningJob(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)

	started := make(chan struct{})
	release := make(chan struct{})
	go func() {
		shardID, becameReady, _ := s.Enqueue(InternalJob{
			LaneID: 0,
			Run: func(ctx context.Context) error {
				close(started)
				<-release
				return nil
			},
		})
		if becameReady {
			select {
			case s.ReadyCh <- shardID:
			default:
			}
		}
	}()
	<-started
	_, err := s.UpdateOverloadPolicy(OverloadPolicyInput{
		Default: LaneOverloadPolicyInput{
			Class: LaneClassBestEffort, RejectAboveRatio: 0.01, ShedAboveRatio: 0.01, MaxQueueDepth: 10,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	close(release)
}

func TestCriticalLaneProgressUnderBestEffortFloodOverload(t *testing.T) {
	reg, policy := overloadTestRegistry()
	policy.Lanes = []LaneOverloadPolicyInput{
		{Lane: "critical", Class: LaneClassCritical, RejectAboveRatio: 0.98, ShedAboveRatio: 1.0, MaxQueueDepth: 100},
		{Lane: "best", Class: LaneClassBestEffort, RejectAboveRatio: 0.75, ShedAboveRatio: 0.60, MaxQueueDepth: 100},
	}
	s, _ := NewScheduler(1, 1, 50, reg)
	snap, _ := buildOverloadPolicySnapshot(reg, policy)
	s.overloadPolicy.Store(snap)

	criticalID, _ := reg.Lookup("critical")
	bestID, _ := reg.Lookup("best")
	pressure := 0.70
	if r := evaluateOverload(snap, bestID, OverloadSignals{GlobalPressure: pressure}); r.Action != OverloadActionShed {
		t.Fatal("best should shed")
	}
	if r := evaluateOverload(snap, criticalID, OverloadSignals{GlobalPressure: pressure}); r.Action != OverloadActionKeep {
		t.Fatal("critical should keep")
	}
}

func TestUpdateOverloadPolicyRejectedWhenStopped(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)
	_ = s.Start(context.Background())
	_ = s.Stop(context.Background(), true)
	_, err := s.UpdateOverloadPolicy(OverloadPolicyInput{
		Default: LaneOverloadPolicyInput{Class: LaneClassNormal, RejectAboveRatio: 0.9, MaxQueueDepth: 10},
	})
	if !errors.Is(err, ErrStopped) {
		t.Errorf("err = %v", err)
	}
}
