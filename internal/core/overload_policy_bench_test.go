// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "testing"

// BenchmarkEvaluateOverload is the guardrail for the overload evaluation hot path.
// The keep branch must allocate zero — compare allocs/op before and after changes.
func BenchmarkEvaluateOverload(b *testing.B) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	snap, err := buildOverloadPolicySnapshot(reg, defaultOverloadPolicy(1, 1000))
	if err != nil {
		b.Fatal(err)
	}
	laneID, _ := reg.Lookup("default")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = evaluateOverload(snap, laneID, OverloadSignals{GlobalPressure: 0.5})
	}
}

func BenchmarkEvaluateOverloadForLane(b *testing.B) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 1000, reg)
	laneID, _ := reg.Lookup("default")
	signals := OverloadSignals{GlobalPressure: 0.5}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.EvaluateOverloadForLane(laneID, signals)
	}
}
