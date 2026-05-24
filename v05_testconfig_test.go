// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"testing"
	"time"
)

func v05BaseConfig() Config {
	return Config{
		ShardCount:       4,
		WorkerCount:      2,
		QueueSizePerLane: 32,
		LaneQuotas: map[Lane]int{
			"default": 2,
			"bulk":    2,
			"payment": 2,
		},
	}
}

func v05EnabledConfig() Config {
	cfg := v05BaseConfig()
	cfg.HotKey = DefaultHotKeyConfig()
	cfg.HotKey.MaxTrackedKeysPerShard = 64
	cfg.HotKey.DetectionWindow = 30 * time.Second
	cfg.HotKey.HotKeyDepthRatio = 0.40
	cfg.HotKey.HotKeyWaitRatio = 0.40
	cfg.HotKey.ExposeRawKey = false
	cfg.PerKeyAdmission = DefaultPerKeyAdmissionConfig()
	cfg.ShardPressure = DefaultShardPressureConfig()
	cfg.AutoscalingSignal = DefaultAutoscalingSignalConfig()
	return cfg
}

func v05DisabledConfig() Config {
	cfg := v05BaseConfig()
	cfg.HotKey = HotKeyConfig{Enabled: false}
	cfg.PerKeyAdmission = PerKeyAdmissionConfig{Enabled: false}
	cfg.ShardPressure = ShardPressureConfig{Enabled: false}
	cfg.AutoscalingSignal = AutoscalingSignalConfig{Enabled: false}
	return cfg
}

// v05HotKeyDisabledConfig enables shard pressure and autoscaling diagnostics but not hot-key
// tracking or per-key admission (per-key requires hot key tracking when enabled).
func v05HotKeyDisabledConfig() Config {
	cfg := v05EnabledConfig()
	cfg.HotKey.Enabled = false
	cfg.PerKeyAdmission.Enabled = false
	return cfg
}

func newV05TestQueue(t *testing.T) *Queue {
	t.Helper()
	q, err := New(v05EnabledConfig())
	if err != nil {
		t.Fatalf("New v0.5 queue: %v", err)
	}
	return q
}

func newV05DisabledQueue(t *testing.T) *Queue {
	t.Helper()
	q, err := New(v05DisabledConfig())
	if err != nil {
		t.Fatalf("New v0.5 disabled queue: %v", err)
	}
	return q
}

func TestV05DisabledFixtureNeutralSnapshot(t *testing.T) {
	q := newV05DisabledQueue(t)
	snap := q.DebugSnapshot()
	if snap.ScaleSignal.DiagnosticsEnabled {
		t.Fatal("expected ScaleSignal.DiagnosticsEnabled false when v0.5 disabled")
	}
	if snap.PressureSummary.DiagnosticsEnabled {
		t.Fatal("expected PressureSummary.DiagnosticsEnabled false when shard pressure disabled")
	}
	for _, sh := range snap.Shards {
		if sh.HotKeyCandidate != nil || len(sh.HotKeyCandidates) > 0 {
			t.Fatalf("shard %d should not expose hot key candidates when disabled", sh.ShardID)
		}
	}
	if len(snap.PerKeyAdmissionSnapshots) != 0 {
		t.Fatalf("expected no per-key snapshots when disabled, got %d", len(snap.PerKeyAdmissionSnapshots))
	}
	sig := q.ScaleSignal()
	if sig.DiagnosticsEnabled || sig.Recommended {
		t.Fatalf("disabled scale signal = %+v", sig)
	}
}
