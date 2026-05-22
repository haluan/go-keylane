// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane_test

import (
	"context"
	"testing"

	"github.com/haluan/go-keylane"
)

func benchmarkQueue(b *testing.B) *keylane.Queue {
	cfg := keylane.Config{
		ShardCount:       4,
		WorkerCount:      4,
		QueueSizePerLane: 64,
		LaneQuotas: map[keylane.Lane]int{
			"laneA": 2,
			"laneB": 2,
			"laneC": 2,
		},
	}
	q, _ := keylane.New(cfg)
	ctx := context.Background()
	_ = q.Start(ctx)
	return q
}

func BenchmarkDebugSnapshot(b *testing.B) {
	q := benchmarkQueue(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.DebugSnapshot()
	}
}

func BenchmarkPressure(b *testing.B) {
	q := benchmarkQueue(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Pressure()
	}
}
