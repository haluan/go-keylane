// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "testing"

func BenchmarkRecordFailureKind(b *testing.B) {
	q, _ := New(newTestConfig())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.recordFailureKind(FailureRetryable)
	}
}
