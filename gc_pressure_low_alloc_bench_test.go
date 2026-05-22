// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"testing"
)

const gcPressureBurstSize = 32

// BenchmarkGCPressureLowAllocationMode compares scheduler batch pooling on a SubmitValue burst.
// Lives in package keylane because DisablePooling is internal. For observability mode overhead,
// use BenchmarkKeylaneSubmitLowAllocationObservability in ./benchmarks.
func BenchmarkGCPressureLowAllocationMode(b *testing.B) {
	b.Run("pooling_on", func(b *testing.B) {
		benchmarkGCPressureLowAllocationMode(b, false)
	})
	b.Run("pooling_off", func(b *testing.B) {
		benchmarkGCPressureLowAllocationMode(b, true)
	})
}

func benchmarkGCPressureLowAllocationMode(b *testing.B, disablePooling bool) {
	q, cancel := setupQueue(8, 4, 10000, map[Lane]int{"default": 2})
	defer cancel()
	q.sched.Obs.DisablePooling = disablePooling

	ctx := context.Background()
	job := ValueJob[int]{Key: "k", Lane: "default", Run: dummyValueRun}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < gcPressureBurstSize; j++ {
			_, _ = SubmitValue(ctx, q, job)
		}
	}
}
