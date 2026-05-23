// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestValidateLaneClass(t *testing.T) {
	if err := ValidateLaneClass(LaneClassCritical); err != nil {
		t.Errorf("critical: %v", err)
	}
	if err := ValidateLaneClass("invalid"); !errors.Is(err, ErrInvalidLaneClass) {
		t.Errorf("invalid class: %v", err)
	}
}

func TestBuildAdmissionPolicyRejectsDuplicateLane(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"a": 1, "b": 1})
	_, err := buildAdmissionPolicySnapshot(reg, AdmissionPolicyInput{
		DefaultClass:            LaneClassNormal,
		DefaultRejectAboveRatio: 0.9,
		DefaultMaxQueueDepth:    10,
		Lanes: []LanePolicyInput{
			{Lane: "a", Class: LaneClassNormal, RejectAboveRatio: 0.9, MaxQueueDepth: 10},
			{Lane: "a", Class: LaneClassBackground, RejectAboveRatio: 0.5, MaxQueueDepth: 5},
		},
	})
	if !errors.Is(err, ErrInvalidAdmissionPolicy) {
		t.Errorf("err = %v, want ErrInvalidAdmissionPolicy", err)
	}
}

func TestEvaluateAdmissionDepthBeforePressure(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	snap, err := buildAdmissionPolicySnapshot(reg, AdmissionPolicyInput{
		DefaultClass:            LaneClassNormal,
		DefaultRejectAboveRatio: 0.99,
		DefaultMaxQueueDepth:    2,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Low global pressure but depth at limit.
	r := evaluateAdmission(snap, 0, 0.1, 2)
	if r.Admit {
		t.Fatal("want reject when depth >= max")
	}
	if r.Reason != AdmissionReasonLaneQueueDepthExceeded {
		t.Errorf("reason = %q, want depth exceeded", r.Reason)
	}
}

func TestEvaluateAdmissionBackgroundRejectsBeforeCritical(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"critical": 1, "background": 1})
	snap, err := buildAdmissionPolicySnapshot(reg, AdmissionPolicyInput{
		DefaultClass:            LaneClassNormal,
		DefaultRejectAboveRatio: 0.90,
		DefaultMaxQueueDepth:    100,
		Lanes: []LanePolicyInput{
			{Lane: "critical", Class: LaneClassCritical, RejectAboveRatio: 0.98, MaxQueueDepth: 100},
			{Lane: "background", Class: LaneClassBackground, RejectAboveRatio: 0.70, MaxQueueDepth: 100},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	criticalID, _ := reg.Lookup("critical")
	backgroundID, _ := reg.Lookup("background")
	pressure := 0.75

	rBg := evaluateAdmission(snap, backgroundID, pressure, 0)
	if rBg.Admit {
		t.Fatal("background should reject at 0.75 pressure")
	}
	rCrit := evaluateAdmission(snap, criticalID, pressure, 0)
	if !rCrit.Admit {
		t.Fatal("critical should admit at 0.75 pressure with threshold 0.98")
	}
}

func TestEvaluateAdmissionBestEffortRejectsBeforeNormal(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"normal": 1, "best": 1})
	snap, err := buildAdmissionPolicySnapshot(reg, AdmissionPolicyInput{
		DefaultClass:            LaneClassNormal,
		DefaultRejectAboveRatio: 0.90,
		DefaultMaxQueueDepth:    100,
		Lanes: []LanePolicyInput{
			{Lane: "best", Class: LaneClassBestEffort, RejectAboveRatio: 0.60, MaxQueueDepth: 100},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	normalID, _ := reg.Lookup("normal")
	bestID, _ := reg.Lookup("best")
	pressure := 0.75

	// best_effort rejects at 0.75 (threshold 0.60); normal lane (default threshold 0.90) still admits.
	rBest := evaluateAdmission(snap, bestID, pressure, 0)
	if rBest.Admit {
		t.Fatal("best_effort should reject at 0.75 pressure")
	}
	rNormal := evaluateAdmission(snap, normalID, pressure, 0)
	if !rNormal.Admit {
		t.Fatal("normal should admit at 0.75 pressure with default threshold 0.90")
	}
}

func TestEvaluateAdmissionBestEffortRejectsBeforeCritical(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"critical": 1, "best": 1})
	snap, err := buildAdmissionPolicySnapshot(reg, AdmissionPolicyInput{
		DefaultClass:            LaneClassNormal,
		DefaultRejectAboveRatio: 0.90,
		DefaultMaxQueueDepth:    100,
		Lanes: []LanePolicyInput{
			{Lane: "critical", Class: LaneClassCritical, RejectAboveRatio: 0.98, MaxQueueDepth: 100},
			{Lane: "best", Class: LaneClassBestEffort, RejectAboveRatio: 0.60, MaxQueueDepth: 100},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	criticalID, _ := reg.Lookup("critical")
	bestID, _ := reg.Lookup("best")
	pressure := 0.75

	rBest := evaluateAdmission(snap, bestID, pressure, 0)
	if rBest.Admit {
		t.Fatal("best_effort should reject at 0.75 pressure")
	}
	rCrit := evaluateAdmission(snap, criticalID, pressure, 0)
	if !rCrit.Admit {
		t.Fatal("critical should admit at 0.75 pressure with threshold 0.98")
	}
}

func TestLowQuotaLaneStillMakesProgressUnderAdmission(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"large": 10, "small": 1})
	s, _ := NewScheduler(1, 1, 100, reg)
	ctx := context.Background()

	largeID, _ := reg.Lookup("large")
	smallID, _ := reg.Lookup("small")

	var executedLarge, executedSmall int32
	for i := 0; i < 20; i++ {
		_, _, _ = s.Enqueue(InternalJob{LaneID: largeID, Run: func(ctx context.Context) error {
			atomic.AddInt32(&executedLarge, 1)
			return nil
		}})
	}
	for i := 0; i < 5; i++ {
		_, _, _ = s.Enqueue(InternalJob{LaneID: smallID, Run: func(ctx context.Context) error {
			atomic.AddInt32(&executedSmall, 1)
			return nil
		}})
	}

	s.processShard(ctx, 0)

	if atomic.LoadInt32(&executedLarge) != 10 {
		t.Errorf("executedLarge = %d, want 10", executedLarge)
	}
	if atomic.LoadInt32(&executedSmall) != 1 {
		t.Errorf("executedSmall = %d, want 1", executedSmall)
	}
}

func TestCriticalLaneAdmittedUnderBestEffortFlood(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"critical": 2, "best_effort": 1})
	policy := AdmissionPolicyInput{
		DefaultClass:            LaneClassNormal,
		DefaultRejectAboveRatio: 0.90,
		DefaultMaxQueueDepth:    50,
		Lanes: []LanePolicyInput{
			{Lane: "critical", Class: LaneClassCritical, RejectAboveRatio: 0.98, MaxQueueDepth: 50},
			{Lane: "best_effort", Class: LaneClassBestEffort, RejectAboveRatio: 0.50, MaxQueueDepth: 50},
		},
	}
	snap, err := buildAdmissionPolicySnapshot(reg, policy)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := NewScheduler(1, 1, 100, reg)
	s.admissionPolicy.Store(snap)

	criticalID, _ := reg.Lookup("critical")
	bestID, _ := reg.Lookup("best_effort")

	// Simulate pressure that rejects best_effort but not critical.
	pressure := 0.70
	if r := evaluateAdmission(snap, bestID, pressure, 0); r.Admit {
		t.Fatal("best_effort should be rejected at 0.70")
	}
	if r := evaluateAdmission(snap, criticalID, pressure, 0); !r.Admit {
		t.Fatal("critical should be admitted at 0.70")
	}
}

func TestBuildAdmissionPolicyMissingLaneUsesDefaults(t *testing.T) {
	// Registry has two lanes: "listed" is given an explicit override; "unlisted" is not.
	// The policy override sets a tight threshold (0.50) only for "listed".
	// At pressure 0.70, "unlisted" must still admit (falls back to default 0.90).
	reg, _ := NewLaneRegistry(map[string]int{"listed": 1, "unlisted": 1})
	snap, err := buildAdmissionPolicySnapshot(reg, AdmissionPolicyInput{
		DefaultClass:            LaneClassNormal,
		DefaultRejectAboveRatio: 0.90,
		DefaultMaxQueueDepth:    100,
		Lanes: []LanePolicyInput{
			{Lane: "listed", Class: LaneClassBestEffort, RejectAboveRatio: 0.50, MaxQueueDepth: 10},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	listedID, _ := reg.Lookup("listed")
	unlistedID, _ := reg.Lookup("unlisted")
	pressure := 0.70

	rListed := evaluateAdmission(snap, listedID, pressure, 0)
	if rListed.Admit {
		t.Fatal("listed lane should reject at 0.70 (threshold 0.50)")
	}
	rUnlisted := evaluateAdmission(snap, unlistedID, pressure, 0)
	if !rUnlisted.Admit {
		t.Fatal("unlisted lane should admit at 0.70 using default threshold 0.90")
	}
	if rUnlisted.Threshold != 0.90 {
		t.Errorf("unlisted threshold = %.2f, want 0.90 (default)", rUnlisted.Threshold)
	}
}

func TestUpdateAdmissionPolicyRejectedWhenStopped(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)
	_ = s.Start(context.Background())
	_ = s.Stop(context.Background(), true)

	_, err := s.UpdateAdmissionPolicy(AdmissionPolicyInput{DefaultClass: LaneClassNormal, DefaultRejectAboveRatio: 0.9, DefaultMaxQueueDepth: 10})
	if !errors.Is(err, ErrStopped) {
		t.Errorf("err = %v, want ErrStopped", err)
	}
}
