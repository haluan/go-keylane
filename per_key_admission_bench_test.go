// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func benchPerKeySubmitQueue(b *testing.B, cfg Config, keyForIter func(i int) string) {
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	defer func() { _ = q.Stop(context.Background()) }()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(ctx, Job{
			Key: keyForIter(i), Lane: "default",
			Run: func(context.Context) error { return nil },
		})
	}
}

func perKeyBenchConfig() Config {
	return Config{
		ShardCount:       2,
		WorkerCount:      2,
		QueueSizePerLane: 64,
		LaneQuotas:       map[Lane]int{"default": 2},
		HotKey: HotKeyConfig{
			Enabled: true, MaxTrackedKeysPerShard: 64,
			DetectionWindow: 30 * time.Second, HotKeyDepthRatio: 0.4,
		},
		PerKeyAdmission: PerKeyAdmissionConfig{
			Enabled: true, MinStatus: HotKeyStatusCandidate,
			DefaultAction: PerKeyMitigationThrottle, PressureRatioThreshold: 0.4,
		},
	}
}

func benchPerKeyQueue(b *testing.B, perKeyEnabled bool) {
	cfg := perKeyBenchConfig()
	cfg.PerKeyAdmission.Enabled = perKeyEnabled
	benchPerKeySubmitQueue(b, cfg, func(i int) string {
		key := "key"
		if i%8 == 0 {
			key = "hot"
		}
		return key
	})
}

func BenchmarkSubmitPerKeyAdmissionDisabled(b *testing.B) {
	benchPerKeyQueue(b, false)
}

func BenchmarkSubmitPerKeyAdmissionEnabled(b *testing.B) {
	benchPerKeyQueue(b, true)
}

func BenchmarkSubmitPerKeyEnabledColdKeys(b *testing.B) {
	cfg := perKeyBenchConfig()
	benchPerKeySubmitQueue(b, cfg, func(int) string { return "cold-key" })
}

func BenchmarkSubmitPerKeyOneDominantHotKey(b *testing.B) {
	cfg := perKeyBenchConfig()
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	defer func() { _ = q.Stop(context.Background()) }()

	const hot = "dominant-hot"
	for i := 0; i < 40; i++ {
		_ = q.Submit(ctx, Job{
			Key: hot, Lane: "default",
			Run: func(context.Context) error { return nil },
		})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(ctx, Job{
			Key: hot, Lane: "default",
			Run: func(context.Context) error { return nil },
		})
	}
}

func BenchmarkSubmitPerKeyManyUniqueBeyondCap(b *testing.B) {
	cfg := perKeyBenchConfig()
	cfg.HotKey.MaxTrackedKeysPerShard = 8
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	defer func() { _ = q.Stop(context.Background()) }()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(ctx, Job{
			Key: fmt.Sprintf("unique-%d", i), Lane: "default",
			Run: func(context.Context) error { return nil },
		})
	}
}

func benchCheckPerKeyAdmissionAction(b *testing.B, action PerKeyMitigationAction) {
	cfg := perKeyTestConfig()
	cfg.PerKeyAdmission.DefaultAction = action
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	const hot = "bench-hot"
	for i := 0; i < 30; i++ {
		_ = q.Submit(context.Background(), Job{
			Key: hot, Lane: "default",
			Run: func(context.Context) error { return nil },
		})
	}
	meta := RequestMeta{Key: hot, Lane: "default"}
	pkCfg := cfg.PerKeyAdmission
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CheckPerKeyAdmission(q, pkCfg, meta)
	}
}

func BenchmarkCheckPerKeyAdmission(b *testing.B) {
	benchCheckPerKeyAdmissionAction(b, PerKeyMitigationReject)
}

func BenchmarkCheckPerKeyAdmissionThrottle(b *testing.B) {
	benchCheckPerKeyAdmissionAction(b, PerKeyMitigationThrottle)
}

func BenchmarkCheckPerKeyAdmissionReject(b *testing.B) {
	benchCheckPerKeyAdmissionAction(b, PerKeyMitigationReject)
}

func BenchmarkDebugSnapshotPerKeyMitigation(b *testing.B) {
	cfg := perKeyTestConfig()
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	block := make(chan struct{})
	defer close(block)
	run := func(ctx context.Context) error {
		select {
		case <-block:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for i := 0; i < 30; i++ {
		_ = q.Submit(context.Background(), Job{
			Key: "snap-hot", Lane: "default", Run: run,
		})
	}
	_ = CheckPerKeyAdmission(q, cfg.PerKeyAdmission, RequestMeta{Key: "snap-hot", Lane: "default"})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.DebugSnapshot()
	}
}
