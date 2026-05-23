// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"
)

// BenchmarkEvaluateAdmission is the guardrail for the admission evaluation hot path.
// The admit branch must allocate zero — no struct construction occurs when the request passes.
// Compare allocs/op before and after changes to the admission evaluator.
func BenchmarkEvaluateAdmission(b *testing.B) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1, "critical": 2})
	snap, err := buildAdmissionPolicySnapshot(reg, AdmissionPolicyInput{
		DefaultClass:            LaneClassNormal,
		DefaultRejectAboveRatio: 0.90,
		DefaultMaxQueueDepth:    1000,
		Lanes: []LanePolicyInput{
			{Lane: "critical", Class: LaneClassCritical, RejectAboveRatio: 0.98, MaxQueueDepth: 2000},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	laneID, _ := reg.Lookup("default")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// pressure 0.5 and depth 0 are well below all thresholds; path always admits.
		_ = evaluateAdmission(snap, laneID, 0.5, 0)
	}
}

// BenchmarkEvaluateAdmissionForLane validates that the atomic snapshot load + evaluateAdmission
// path through the public Scheduler method also stays at zero allocs on the admit branch.
func BenchmarkEvaluateAdmissionForLane(b *testing.B) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 1000, reg)
	_, err := s.UpdateAdmissionPolicy(AdmissionPolicyInput{
		DefaultClass:            LaneClassNormal,
		DefaultRejectAboveRatio: 0.90,
		DefaultMaxQueueDepth:    1000,
	})
	if err != nil {
		b.Fatal(err)
	}
	laneID, _ := reg.Lookup("default")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.EvaluateAdmissionForLane(laneID, 0.5, 0)
	}
}
