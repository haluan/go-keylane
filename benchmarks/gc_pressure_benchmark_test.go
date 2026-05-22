// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks_test

import (
	"context"
	"testing"

	"github.com/haluan/go-keylane"
)

const gcBurstSize = 32

func BenchmarkGCPressureSubmitBurst(b *testing.B) {
	q, _ := makeBenchmarkQueue(b, benchConfig())
	job := keylane.Job{Key: "k", Lane: "default", Run: dummyRun}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < gcBurstSize; j++ {
			_ = q.Submit(ctx, job)
		}
	}
}

func BenchmarkGCPressureSubmitValueBurst(b *testing.B) {
	q, _ := makeBenchmarkQueue(b, benchConfig())
	job := keylane.ValueJob[int]{Key: "k", Lane: "default", Run: dummyValueRun}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < gcBurstSize; j++ {
			_, _ = keylane.SubmitValue(ctx, q, job)
		}
	}
}

func BenchmarkGCPressureManyKeysBurst(b *testing.B) {
	q, _ := makeBenchmarkQueue(b, benchConfig())
	keys := generateManyKeys(gcBurstSize, 5)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < gcBurstSize; j++ {
			_ = q.Submit(ctx, keylane.Job{
				Key:  keys[j],
				Lane: "default",
				Run:  dummyRun,
			})
		}
	}
}

func BenchmarkGCPressureOneHotKeyBurst(b *testing.B) {
	q, _ := makeBenchmarkQueue(b, benchConfig())
	hot := generateHotKey()
	job := keylane.Job{Key: hot, Lane: "default", Run: dummyRun}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < gcBurstSize; j++ {
			_ = q.Submit(ctx, job)
		}
	}
}

func BenchmarkGCPressureObservabilityEnabled(b *testing.B) {
	cfg := benchConfig()
	cfg.Observability = keylane.ObservabilityConfig{
		TrackQueueWait: true,
		Hooks: keylane.Hooks{
			OnJobTiming: func(keylane.JobTimingEvent) {},
		},
	}
	q, _ := makeBenchmarkQueue(b, cfg)
	job := keylane.Job{Key: "k", Lane: "default", Run: dummyRun}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < gcBurstSize; j++ {
			_ = q.Submit(ctx, job)
		}
	}
}

// BenchmarkGCPressureObservabilityMinimal is the low-overhead baseline for observability GC comparison.
func BenchmarkGCPressureObservabilityMinimal(b *testing.B) {
	BenchmarkGCPressureSubmitBurst(b)
}
