// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks_test

import (
	"context"
	"testing"

	"github.com/haluan/go-keylane"
)

func benchSubmitWithContinuation(b *testing.B, enabled bool) {
	b.Helper()
	cfg := benchConfigSingleLane()
	cfg.Continuation = keylane.ContinuationConfig{Enabled: enabled, MaxPending: 64}
	q, _ := makeBenchmarkQueue(b, cfg)
	job := keylane.Job{Key: "k", Lane: "default", Run: dummyRun}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(context.Background(), job)
	}
}

func BenchmarkKeylaneContinuationDisabledOverhead(b *testing.B) {
	b.Run("disabled", func(b *testing.B) {
		benchSubmitWithContinuation(b, false)
	})
	b.Run("enabled_unused", func(b *testing.B) {
		benchSubmitWithContinuation(b, true)
	})
}
