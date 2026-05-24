// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"testing"
	"time"

	"github.com/haluan/go-keylane/internal/core"
)

func v05ScenarioConfig() Config {
	cfg := v05EnabledConfig()
	cfg.Observability = DefaultObservabilityConfig()
	cfg.ShardPressure.HotShardPressureRatio = 0.20
	cfg.AutoscalingSignal.Window = 15 * time.Millisecond
	cfg.AutoscalingSignal.ConsecutiveWindows = 2
	cfg.AutoscalingSignal.QueueDepthRatioThreshold = 0.20
	cfg.AutoscalingSignal.LocalizedHotKeyRatioThreshold = 0.35
	cfg.ShardCount = 4
	cfg.WorkerCount = 2
	return cfg
}

func blockedRun(block <-chan struct{}) func(context.Context) error {
	return func(ctx context.Context) error {
		select {
		case <-block:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func startV05ScenarioQueue(t *testing.T, cfg Config) (*Queue, context.CancelFunc, chan struct{}) {
	t.Helper()
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	t.Cleanup(func() {
		close(block)
		cancel()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = q.Stop(stopCtx, WithDrain(false))
	})
	return q, cancel, block
}

func newV05ScenarioQueue(t *testing.T, cfg Config) (*Queue, chan struct{}) {
	t.Helper()
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	return q, block
}

func TestV05ScenarioLocalizedHotKey(t *testing.T) {
	cfg := v05ScenarioConfig()
	cfg.ShardCount = 1
	cfg.WorkerCount = 1
	cfg.QueueSizePerLane = 64
	cfg.PerKeyAdmission.Enabled = false
	q, block := newV05ScenarioQueue(t, cfg)
	run := blockedRun(block)
	for i := 0; i < 80; i++ {
		key := "hot-key-A"
		if i%5 == 0 {
			key = "normal-" + string(rune('a'+i%26))
		}
		_ = q.Submit(context.Background(), Job{Key: key, Lane: "default", Run: run})
	}
	deadline := time.Now().Add(800 * time.Millisecond)
	var sawLocalized bool
	for time.Now().Before(deadline) {
		sig := q.ScaleSignal()
		if sig.Reason == ScaleReasonLocalizedHotKey && !sig.Recommended && sig.LocalizedHotKey {
			if sig.HotKeyCandidateCount < 1 {
				continue
			}
			sawLocalized = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !sawLocalized {
		t.Fatalf("expected localized_hot_key signal, last=%+v", q.ScaleSignal())
	}
	snap := q.DebugSnapshot()
	foundHot := false
	for _, sh := range snap.Shards {
		for _, c := range sh.HotKeyCandidates {
			if c.Key != "" {
				t.Fatal("raw key must not appear in snapshot by default")
			}
			if c.KeyHash == core.HashKey("hot-key-A") {
				foundHot = true
			}
		}
	}
	if !foundHot {
		t.Fatal("expected hot-key-A hash in candidates")
	}
}

func TestV05ScenarioDistributedBacklog(t *testing.T) {
	cfg := v05ScenarioConfig()
	cfg.HotKey.Enabled = false
	cfg.PerKeyAdmission.Enabled = false
	cfg.AutoscalingSignal.QueueDepthRatioThreshold = 0.15
	q, block := newV05ScenarioQueue(t, cfg)
	run := blockedRun(block)
	for i := 0; i < 200; i++ {
		key := "tenant-" + string(rune('a'+i%26)) + "-" + string(rune('0'+i%10))
		_ = q.Submit(context.Background(), Job{Key: key, Lane: "default", Run: run})
	}
	deadline := time.Now().Add(800 * time.Millisecond)
	var recommended bool
	for time.Now().Before(deadline) {
		sig := q.ScaleSignal()
		summary := q.PressureSummary()
		if summary.HotShardCount >= 2 && sig.Recommended && sig.Scope == ScaleScopeGlobal &&
			sig.LocalizedHotKeyRatio < cfg.AutoscalingSignal.LocalizedHotKeyRatioThreshold &&
			(sig.Reason == ScaleReasonDistributedPressure || sig.Reason == ScaleReasonManyHotShards || sig.Reason == ScaleReasonQueueDepthHigh) {
			recommended = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !recommended {
		t.Log("last signal:", q.ScaleSignal())
		t.Log("last summary:", q.PressureSummary())
		t.Fatal("expected global scale recommendation under distributed backlog")
	}
}

func TestV05ScenarioHotLane(t *testing.T) {
	cfg := v05ScenarioConfig()
	cfg.ShardCount = 4
	cfg.HotKey.Enabled = false
	cfg.PerKeyAdmission.Enabled = false
	cfg.ShardPressure.DominantLaneRatio = 0.55
	q, block := newV05ScenarioQueue(t, cfg)
	run := blockedRun(block)
	for i := 0; i < 80; i++ {
		lane := Lane("payment")
		if i%8 == 0 {
			lane = "default"
		}
		_ = q.Submit(context.Background(), Job{
			Key: "key-" + string(rune('a'+i%26)), Lane: lane, Run: run,
		})
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	var satisfied bool
	for time.Now().Before(deadline) {
		summary := q.PressureSummary()
		hotKeyCands := 0
		laneDominant := false
		for _, hs := range summary.HotShards {
			hotKeyCands += len(hs.HotKeyCandidates)
			if hs.Class == ShardPressureLaneDominant {
				laneDominant = true
			}
		}
		if laneDominant && summary.LaneDominanceRatio >= cfg.ShardPressure.DominantLaneRatio && hotKeyCands == 0 {
			satisfied = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !satisfied {
		t.Fatalf("expected lane_dominant without hot keys, summary=%+v", q.PressureSummary())
	}
}

func keyForShard(shardID, shardCount int) string {
	for i := 0; i < 100000; i++ {
		key := "worker-key-" + string(rune('0'+i%10)) + "-" + string(rune('a'+i%26))
		if int(core.HashKey(key)%uint64(shardCount)) == shardID {
			return key
		}
	}
	return "worker-key-fallback"
}

func TestV05ScenarioWorkerSaturation(t *testing.T) {
	cfg := v05ScenarioConfig()
	cfg.ShardCount = 4
	cfg.WorkerCount = 2
	cfg.QueueSizePerLane = 64
	cfg.ShardPressure.HotShardPressureRatio = 0.95
	cfg.HotKey.Enabled = false
	cfg.PerKeyAdmission.Enabled = false
	cfg.AutoscalingSignal.ManyHotShardsThreshold = 99
	cfg.AutoscalingSignal.HotShardRatioThreshold = 0.99
	cfg.AutoscalingSignal.WorkerBusyRatioThreshold = 0.50
	cfg.AutoscalingSignal.QueueWaitMaxThreshold = 5 * time.Millisecond
	q, _, block := startV05ScenarioQueue(t, cfg)
	run := blockedRun(block)
	hotKey := keyForShard(0, cfg.ShardCount)
	for i := 0; i < 12; i++ {
		_ = q.Submit(context.Background(), Job{
			Key: hotKey, Lane: "default", Run: run,
		})
	}
	deadline := time.Now().Add(800 * time.Millisecond)
	var workerPressure bool
	for time.Now().Before(deadline) {
		sig := q.ScaleSignal()
		if sig.WorkerBusyRatio >= 0.5 &&
			(sig.Reason == ScaleReasonWorkerSaturated || sig.Reason == ScaleReasonQueueWaitHigh || sig.Reason == ScaleReasonQueueDepthHigh) {
			workerPressure = true
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if !workerPressure {
		t.Fatalf("expected worker saturation signal, last=%+v", q.ScaleSignal())
	}
}
