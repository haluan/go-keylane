// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"reflect"
	"testing"
	"time"
)

func TestNormalizeConfigDeterministic(t *testing.T) {
	cfg := newTestConfig()
	s1 := NormalizeConfig(cfg)
	s2 := NormalizeConfig(cfg)
	if !reflect.DeepEqual(s1, s2) {
		t.Fatalf("snapshots differ:\n%+v\n%+v", s1, s2)
	}
	if s1.Version != ConfigVersionV1 {
		t.Fatalf("version %q", s1.Version)
	}
	if !s1.Valid {
		t.Fatal("expected valid snapshot for good config")
	}
}

func TestNormalizeConfigInvalidShowsRejected(t *testing.T) {
	cfg := newTestConfig()
	cfg.Retry = RetryPolicy{Enabled: true, InitialBackoff: -1}
	snap := NormalizeConfig(cfg)
	if snap.Valid {
		t.Fatal("expected Valid false for rejected config")
	}
	if !hasSnapshotErrorCode(snap, CodeConfigInvalidBackoff) {
		t.Fatalf("expected fatal issue in snapshot Issues: %+v", snap.Issues)
	}
}

func TestNormalizeConfigRetryFieldsComplete(t *testing.T) {
	cfg := newTestConfig()
	cfg.Retry = RetryPolicy{
		Enabled:        true,
		MaxAttempts:    3,
		Jitter:         true,
		RetryableKinds: []FailureKind{FailureRetryable, FailurePermanent},
	}
	snap := NormalizeConfig(cfg)
	if !snap.Retry.Jitter {
		t.Fatal("expected Jitter in snapshot")
	}
	if len(snap.Retry.RetryableKinds) != 2 {
		t.Fatalf("kinds %v", snap.Retry.RetryableKinds)
	}
}

func hasSnapshotErrorCode(snap NormalizedConfig, code string) bool {
	for _, issue := range snap.Issues {
		if issue.Severity == ValidationError && issue.Code == code {
			return true
		}
	}
	return false
}

func TestNormalizeConfigLaneOrderSorted(t *testing.T) {
	cfg := newTestConfig()
	cfg.LaneQuotas = map[Lane]int{"z": 1, "a": 2, "m": 1}
	snap := NormalizeConfig(cfg)
	if len(snap.Lanes.Quotas) != 3 {
		t.Fatalf("lanes %+v", snap.Lanes.Quotas)
	}
	if snap.Lanes.Quotas[0].Lane != "a" || snap.Lanes.Quotas[2].Lane != "z" {
		t.Fatalf("order %+v", snap.Lanes.Quotas)
	}
}

func TestNormalizeConfigAppliedDefaultsContinuation(t *testing.T) {
	cfg := newTestConfig()
	cfg.Continuation = ContinuationConfig{Enabled: true, MaxPending: 0}
	snap := NormalizeConfig(cfg)
	found := false
	for _, d := range snap.AppliedDefaults {
		if d == "continuation.max_pending=256" {
			found = true
		}
	}
	if !found {
		t.Fatalf("defaults %v", snap.AppliedDefaults)
	}
}

func TestNormalizeConfigNoRawKeysInSnapshot(t *testing.T) {
	cfg := newTestConfig()
	cfg.HotKey = HotKeyConfig{Enabled: true, ExposeRawKey: true, MaxTrackedKeysPerShard: 8}
	snap := NormalizeConfig(cfg)
	if !snap.HotKey.ExposeRawKey {
		t.Fatal("expected ExposeRawKey flag true in snapshot")
	}
}

func TestNormalizeConfigSubsystemDisabled(t *testing.T) {
	cfg := newTestConfig()
	snap := NormalizeConfig(cfg)
	if snap.Retry.Enabled {
		t.Fatal("retry should be disabled")
	}
	if snap.Pipeline.ContinuationEnabled {
		t.Fatal("continuation should be disabled")
	}
	if snap.BackendResources.Enabled {
		t.Fatal("backend resources should be disabled")
	}
	if snap.AdaptiveQuota.Enabled {
		t.Fatal("adaptive quota should be disabled")
	}
	if snap.PerKeyAdmission.Enabled {
		t.Fatal("per-key admission should be disabled")
	}
}

func TestNormalizeConfigBackendLaneLimits(t *testing.T) {
	cfg := newTestConfig()
	cfg.BackendResources = BackendResourceConfig{
		Enabled: true,
		Resources: map[BackendResourceName]BackendResourcePolicy{
			"db": {Lanes: map[BackendLane]BackendLanePolicy{
				"read": {MaxInFlight: 8, Admission: BackendAdmissionReject},
			}},
		},
	}
	snap := NormalizeConfig(cfg)
	if len(snap.BackendResources.Lanes) != 1 {
		t.Fatalf("lanes %+v", snap.BackendResources.Lanes)
	}
	if snap.BackendResources.Lanes[0].MaxInFlight != 8 {
		t.Fatalf("got %+v", snap.BackendResources.Lanes[0])
	}
}

func TestNormalizeConfigAdaptiveQuotaEnabled(t *testing.T) {
	cfg := newTestConfig()
	cfg.AdaptiveQuota = AdaptiveQuotaPolicy{
		Config: AdaptiveQuotaConfig{Enabled: true, EvaluationInterval: time.Second},
		Lanes: []LaneAdaptivePolicy{
			{
				Lane:            "payment",
				Class:           LaneCritical,
				Enabled:         true,
				MinQuota:        1,
				MaxQuota:        8,
				AllowIncrease:   true,
				AllowDecrease:   false,
				TargetQueueWait: 20 * time.Millisecond,
				TargetRunTime:   100 * time.Millisecond,
			},
		},
	}
	snap := NormalizeConfig(cfg)
	if !snap.AdaptiveQuota.Enabled {
		t.Fatal("expected adaptive quota enabled in snapshot")
	}
	if snap.AdaptiveQuota.EvaluationInterval != time.Second {
		t.Fatalf("interval %v", snap.AdaptiveQuota.EvaluationInterval)
	}
	if len(snap.AdaptiveQuota.Lanes) != 1 {
		t.Fatalf("lanes %+v", snap.AdaptiveQuota.Lanes)
	}
	lp := snap.AdaptiveQuota.Lanes[0]
	if lp.Lane != "payment" || lp.MaxQuota != 8 || !lp.AllowIncrease || lp.AllowDecrease {
		t.Fatalf("lane policy %+v", lp)
	}
}

func TestNormalizeConfigAppliedDefaultsHotKey(t *testing.T) {
	cfg := newTestConfig()
	cfg.HotKey = HotKeyConfig{Enabled: true, MaxTrackedKeysPerShard: 16}
	snap := NormalizeConfig(cfg)
	found := false
	for _, d := range snap.AppliedDefaults {
		if d == "hotkey.detection_window=30s" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected hotkey defaults in %+v", snap.AppliedDefaults)
	}
}

func TestNormalizeConfigAppliedDefaultsShardPressure(t *testing.T) {
	cfg := newTestConfig()
	cfg.ShardPressure = ShardPressureConfig{Enabled: true}
	snap := NormalizeConfig(cfg)
	if len(snap.AppliedDefaults) == 0 {
		t.Fatal("expected shard pressure applied defaults")
	}
}
