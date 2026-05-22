// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks_test

import (
	"context"
	"testing"

	"github.com/haluan/go-keylane"
)

func BenchmarkKeylaneSubmitDefaultObservability(b *testing.B) {
	benchmarkKeylaneSubmitObservability(b, keylane.DefaultObservabilityConfig())
}

func BenchmarkKeylaneSubmitLowAllocationObservability(b *testing.B) {
	benchmarkKeylaneSubmitObservability(b, keylane.LowAllocationObservabilityConfig())
}

func benchmarkKeylaneSubmitObservability(b *testing.B, obs keylane.ObservabilityConfig) {
	cfg := benchConfigSingleLane()
	cfg.Observability = obs
	q, _ := makeBenchmarkQueue(b, cfg)
	job := keylane.Job{Key: "k", Lane: "default", Run: dummyRun}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(ctx, job)
	}
}

func BenchmarkKeylaneSubmitValueDefaultObservability(b *testing.B) {
	benchmarkKeylaneSubmitValueObservability(b, keylane.DefaultObservabilityConfig())
}

func BenchmarkKeylaneSubmitValueLowAllocationObservability(b *testing.B) {
	benchmarkKeylaneSubmitValueObservability(b, keylane.LowAllocationObservabilityConfig())
}

func benchmarkKeylaneSubmitValueObservability(b *testing.B, obs keylane.ObservabilityConfig) {
	cfg := benchConfigSingleLane()
	cfg.Observability = obs
	q, _ := makeBenchmarkQueue(b, cfg)
	job := keylane.ValueJob[int]{Key: "k", Lane: "default", Run: dummyValueRun}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = keylane.SubmitValue(ctx, q, job)
	}
}

func BenchmarkKeylaneDebugSnapshotOnDemand(b *testing.B) {
	cfg := benchConfig()
	cfg.Observability = keylane.DefaultObservabilityConfig()
	q, _ := makeBenchmarkQueue(b, cfg)
	ctx := context.Background()
	_ = q.Submit(ctx, keylane.Job{Key: "warm", Lane: "default", Run: dummyRun})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.DebugSnapshot()
	}
}
