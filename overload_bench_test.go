// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"testing"
)

// BenchmarkCheckOverload is the guardrail for the public overload hot path on successful admit.
func BenchmarkCheckOverload(b *testing.B) {
	cfg := Config{
		ShardCount:       4,
		WorkerCount:      2,
		QueueSizePerLane: 1000,
		LaneQuotas:       map[Lane]int{"default": 2},
	}
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	overloadCfg := OverloadConfig{Enabled: true}
	meta := RequestMeta{Key: "bench-key", Lane: "default"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CheckOverload(q, overloadCfg, meta)
	}
}
