// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"testing"
)

// BenchmarkCheckAdmission is the guardrail for the public admission hot path.
// The queue is started but empty so pressure stays near zero; the path always admits.
// Successful admission must not allocate — no AdmissionRejectedError is constructed
// and no rejection counter is recorded. Compare allocs/op before and after changes
// to CheckAdmission, Pressure, or EvaluateAdmissionForLane.
func BenchmarkCheckAdmission(b *testing.B) {
	cfg := Config{
		ShardCount:       4,
		WorkerCount:      2,
		QueueSizePerLane: 1000,
		LaneQuotas:       map[Lane]int{"default": 2, "critical": 2},
	}
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		b.Fatal(err)
	}
	if _, err := q.UpdateAdmissionPolicy(AdmissionPolicy{
		DefaultClass:            LaneNormal,
		DefaultRejectAboveRatio: 0.90,
		DefaultMaxQueueDepth:    1000,
		Lanes: []LanePolicy{
			{Lane: "critical", Class: LaneCritical, RejectAboveRatio: 0.98, MaxQueueDepth: 2000},
		},
	}); err != nil {
		b.Fatal(err)
	}

	admCfg := AdmissionConfig{Enabled: true}
	meta := RequestMeta{Key: "bench-key", Lane: "default"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CheckAdmission(q, admCfg, meta)
	}
}
