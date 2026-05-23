// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "testing"

func BenchmarkQuotaSnapshot(b *testing.B) {
	q, _ := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 2, "fast": 1},
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.CurrentQuotaPolicy()
	}
}
