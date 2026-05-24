// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"sync"
	"testing"
	"time"
)

func autoscalingTestConfig() Config {
	return Config{
		ShardCount:       4,
		WorkerCount:      2,
		QueueSizePerLane: 32,
		LaneQuotas:       map[Lane]int{"default": 2, "bulk": 2},
		HotKey: HotKeyConfig{
			Enabled: true, MaxTrackedKeysPerShard: 16,
			DetectionWindow: time.Minute, HotKeyDepthRatio: 0.3,
		},
		PerKeyAdmission:   DefaultPerKeyAdmissionConfig(),
		ShardPressure:     DefaultShardPressureConfig(),
		AutoscalingSignal: DefaultAutoscalingSignalConfig(),
	}
}

func TestScaleSignalDisabledReturnsNone(t *testing.T) {
	cfg := autoscalingTestConfig()
	cfg.AutoscalingSignal.Enabled = false
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sig := q.ScaleSignal()
	if sig.Recommended || sig.Reason != ScaleReasonNone {
		t.Fatalf("disabled signal = %+v", sig)
	}
	if sig.DiagnosticsEnabled {
		t.Fatal("expected DiagnosticsEnabled false when disabled")
	}
}

func TestScaleSignalHealthyIdle(t *testing.T) {
	q, err := New(autoscalingTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	sig := q.ScaleSignal()
	if sig.Recommended {
		t.Fatal("idle queue should not recommend scale")
	}
}

func TestDebugSnapshotIncludesScaleSignal(t *testing.T) {
	cfg := autoscalingTestConfig()
	cfg.Observability.EnableDebugSnapshot = true
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	snap := q.DebugSnapshot()
	if snap.Version != DebugSnapshotVersion {
		t.Fatalf("version = %q, want %q", snap.Version, DebugSnapshotVersion)
	}
	if snap.ScaleSignal.Reason == "" && snap.ScaleSignal.Recommended {
		t.Fatal("unexpected scale signal state")
	}
}

func TestScaleSignalHighQueueDepthEventuallyRecommends(t *testing.T) {
	cfg := autoscalingTestConfig()
	cfg.AutoscalingSignal.Window = 10 * time.Millisecond
	cfg.AutoscalingSignal.ConsecutiveWindows = 2
	cfg.AutoscalingSignal.QueueDepthRatioThreshold = 0.20
	cfg.ShardCount = 4
	cfg.HotKey.Enabled = false
	cfg.PerKeyAdmission.Enabled = false
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
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
	for i := 0; i < 80; i++ {
		key := "tenant-" + string(rune('a'+i%26)) + "-" + string(rune('0'+i%10))
		_ = q.Submit(context.Background(), Job{Key: key, Lane: "default", Run: run})
	}
	var recommended bool
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		sig := q.ScaleSignal()
		if sig.Recommended && sig.Reason != ScaleReasonLocalizedHotKey {
			recommended = true
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if !recommended {
		t.Log("ScaleSignal after backlog:", q.ScaleSignal())
		t.Fatal("expected scale recommendation under sustained distributed queue depth")
	}
}

func TestScaleSignalConcurrentRead(t *testing.T) {
	cfg := autoscalingTestConfig()
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	defer func() { _ = q.Stop(context.Background()) }()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = q.ScaleSignal()
			}
		}()
	}
	for i := 0; i < 20; i++ {
		_ = q.Submit(context.Background(), Job{
			Key: "k", Lane: "default",
			Run: func(context.Context) error { return nil },
		})
	}
	wg.Wait()
}

func TestValidateAutoscalingSignalConfig(t *testing.T) {
	t.Parallel()
	if err := ValidateAutoscalingSignalConfig(AutoscalingSignalConfig{Enabled: true, Window: 0}); err == nil {
		t.Fatal("expected error for zero window")
	}
	if err := ValidateAutoscalingSignalConfig(AutoscalingSignalConfig{}); err != nil {
		t.Fatalf("disabled config: %v", err)
	}
}
