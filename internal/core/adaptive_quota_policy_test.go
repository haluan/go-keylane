// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "testing"

func TestResolveAdaptiveLanePolicyFixedLane(t *testing.T) {
	reg, err := NewLaneRegistry(map[string]int{"normal": 2})
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewScheduler(1, 1, 10, reg)
	if err != nil {
		t.Fatal(err)
	}
	policies := resolveAdaptiveLanePolicies(reg, s, []LaneAdaptivePolicy{
		{
			Lane: "normal", Class: LaneClassNormal, Enabled: true,
			MinQuota: 1, MaxQuota: 4,
			AllowIncrease: false, AllowDecrease: false,
		},
	}, map[string]int{"normal": 2})

	if len(policies) != 1 {
		t.Fatalf("len = %d, want 1", len(policies))
	}
	p := policies[0]
	if p.AllowIncrease || p.AllowDecrease {
		t.Errorf("fixed lane: AllowIncrease=%v AllowDecrease=%v, want both false", p.AllowIncrease, p.AllowDecrease)
	}
}

func TestResolveAdaptiveLanePolicyPromotesAllowIncrease(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"bg": 1})
	s, _ := NewScheduler(1, 1, 10, reg)
	policies := resolveAdaptiveLanePolicies(reg, s, []LaneAdaptivePolicy{
		{
			Lane: "bg", Class: LaneClassBackground, Enabled: true,
			MinQuota: 1, MaxQuota: 4,
			AllowIncrease: true, AllowDecrease: true,
		},
	}, map[string]int{"bg": 1})
	if len(policies) != 1 {
		t.Fatalf("len = %d", len(policies))
	}
	p := policies[0]
	if !p.AllowIncrease {
		t.Error("explicit AllowIncrease=true should override background class default")
	}
	if !p.AllowDecrease {
		t.Error("explicit AllowDecrease=true should be preserved")
	}
}

func TestResolveAdaptiveLanePolicyPromotesAllowDecreaseFalse(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"normal": 2})
	s, _ := NewScheduler(1, 1, 10, reg)
	policies := resolveAdaptiveLanePolicies(reg, s, []LaneAdaptivePolicy{
		{
			Lane: "normal", Class: LaneClassNormal, Enabled: true,
			MinQuota: 1, MaxQuota: 8,
			AllowIncrease: true, AllowDecrease: false,
		},
	}, map[string]int{"normal": 2})
	if len(policies) != 1 {
		t.Fatalf("len = %d", len(policies))
	}
	p := policies[0]
	if p.AllowDecrease {
		t.Error("explicit AllowDecrease=false should override normal class default")
	}
}

func TestResolveAdaptiveLanePolicyUsesAdmissionClass(t *testing.T) {
	reg, err := NewLaneRegistry(map[string]int{"payment": 2})
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewScheduler(1, 1, 10, reg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.UpdateAdmissionPolicy(AdmissionPolicyInput{
		DefaultClass:            LaneClassNormal,
		DefaultRejectAboveRatio: 0.90,
		DefaultMaxQueueDepth:    100,
		Lanes: []LanePolicyInput{
			{Lane: "payment", Class: LaneClassCritical, RejectAboveRatio: 0.98, MaxQueueDepth: 100},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	policies := resolveAdaptiveLanePolicies(reg, s, nil, map[string]int{"payment": 2})
	if len(policies) != 1 {
		t.Fatalf("len = %d", len(policies))
	}
	p := policies[0]
	if p.Class != LaneClassCritical {
		t.Errorf("class = %q, want critical", p.Class)
	}
	if p.AllowDecrease {
		t.Error("critical lane should not allow decrease by default")
	}
}
