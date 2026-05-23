// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"testing"
	"time"

	"github.com/haluan/go-keylane/internal/core"
)

func TestDefaultHotKeyConfigEnabled(t *testing.T) {
	t.Parallel()
	if !DefaultHotKeyConfig().Enabled {
		t.Fatal("DefaultHotKeyConfig().Enabled should be true per KL-1501 spec")
	}
}

func TestValidateHotKeyConfig(t *testing.T) {
	t.Parallel()
	if err := ValidateHotKeyConfig(DefaultHotKeyConfig()); err != nil {
		t.Fatal(err)
	}
	if err := ValidateHotKeyConfig(HotKeyConfig{Enabled: true, MaxTrackedKeysPerShard: 0}); err != nil {
		t.Fatalf("MaxTrackedKeysPerShard=0 no-op: %v", err)
	}
	bad := DefaultHotKeyConfig()
	bad.MaxTrackedKeysPerShard = -1
	if err := ValidateHotKeyConfig(bad); err == nil {
		t.Fatal("expected error for negative max entries")
	}
}

func TestNewQueueHotKeyNormalizeBeforeValidate(t *testing.T) {
	t.Parallel()
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 8,
		LaneQuotas:       map[Lane]int{"default": 1},
		HotKey:           HotKeyConfig{Enabled: true, MaxTrackedKeysPerShard: 64},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !q.config.HotKey.Enabled {
		t.Fatal("hot key should stay enabled")
	}
	if q.config.HotKey.DetectionWindow <= 0 {
		t.Fatal("DetectionWindow should be normalized")
	}
}

func TestNewQueueWithHotKeyEnabled(t *testing.T) {
	cfg := Config{
		ShardCount:       2,
		WorkerCount:      1,
		QueueSizePerLane: 32,
		LaneQuotas:       map[Lane]int{"default": 1},
		HotKey: HotKeyConfig{
			Enabled:                true,
			MaxTrackedKeysPerShard: 16,
			DetectionWindow:        time.Minute,
			HotKeyDepthRatio:       0.35,
		},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	defer func() { _ = q.Stop(context.Background()) }()

	hot := "noisy-tenant"
	for i := 0; i < 30; i++ {
		_ = q.Submit(ctx, Job{
			Key:  hot,
			Lane: "default",
			Run:  func(ctx context.Context) error { return nil },
		})
	}
	snap := q.DebugSnapshot()
	found := false
	for _, sh := range snap.Shards {
		if sh.HotKeyCandidate != nil && sh.HotKeyCandidate.KeyHash == core.HashKey(hot) {
			found = true
		}
	}
	if !found {
		t.Fatal("expected hot key candidate in public DebugSnapshot")
	}
}
