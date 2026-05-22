// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"
)

// BenchmarkStatsGCPressure measures scheduler-level StatsGCPressure snapshot cost.
func BenchmarkStatsGCPressure(b *testing.B) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 2, "fast": 1})
	s, _ := NewScheduler(8, 4, 1000, reg)

	for i := 0; i < 50; i++ {
		job, _ := NewInternalJob(dummyRun, uint64(i), 0)
		_, _, _ = s.Enqueue(job)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.StatsGCPressure()
	}
}
