// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"reflect"
	"testing"
	"time"
)

func TestProductionDefaults_IsProductionSafe(t *testing.T) {
	cfg := ProductionDefaults()
	report := ValidateConfig(cfg)
	if report.HasErrors() {
		t.Fatalf("errors: %+v", report.Issues)
	}
	if _, err := New(cfg); err != nil {
		t.Fatalf("New: %v", err)
	}
}

func TestProductionDefaults_RetryDisabled(t *testing.T) {
	if ProductionDefaults().Retry.Enabled {
		t.Fatal("retry must be disabled")
	}
}

func TestProductionDefaults_ContinuationDisabled(t *testing.T) {
	if ProductionDefaults().Continuation.Enabled {
		t.Fatal("continuation must be disabled")
	}
}

func TestProductionDefaults_BackendResourcesDisabled(t *testing.T) {
	if ProductionDefaults().BackendResources.Enabled {
		t.Fatal("backend resources must be disabled")
	}
}

func TestProductionDefaults_RawKeyExposureDisabled(t *testing.T) {
	if ProductionDefaults().HotKey.ExposeRawKey {
		t.Fatal("raw key exposure must be disabled")
	}
}

func TestProductionDefaults_LowAllocationObservability(t *testing.T) {
	cfg := ProductionDefaults()
	obs := ResolveObservabilityConfig(cfg.Observability)
	if !obs.LowAllocationMode {
		t.Fatal("expected low-allocation mode")
	}
	if obs.EnableHooks || obs.EnableQueueWaitTiming || obs.EnableRunTiming {
		t.Fatalf("hot-path observability should be off: %+v", obs)
	}
	if !obs.EnableCounters || !obs.EnableStats {
		t.Fatalf("counters/stats should remain on: %+v", obs)
	}
}

func TestProductionDefaults_PressureAdaptersObservational(t *testing.T) {
	if len(ProductionDefaults().BackendResources.PressureProviders) != 0 {
		t.Fatal("expected no pressure providers by default")
	}
}

func TestZeroValueSubsystemGatesDisabled(t *testing.T) {
	var cfg Config
	if cfg.Retry.Enabled || cfg.Continuation.Enabled || cfg.BackendResources.Enabled || cfg.HotKey.Enabled {
		t.Fatalf("zero subsystems should be disabled: retry=%v cont=%v backend=%v hotkey=%v",
			cfg.Retry.Enabled, cfg.Continuation.Enabled, cfg.BackendResources.Enabled, cfg.HotKey.Enabled)
	}
}

func TestRiskySubsystemRequiresExplicitEnable(t *testing.T) {
	cfg := newTestConfig()
	cfg.Retry = RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if q.retryPolicy.Enabled {
		t.Fatal("MaxAttempts without Retry.Enabled must not enable retry")
	}
}

func TestExplainDefaults_Deterministic(t *testing.T) {
	cfg := ProductionDefaults()
	r1 := ExplainDefaults(cfg)
	r2 := ExplainDefaults(cfg)
	if !reflect.DeepEqual(r1, r2) {
		t.Fatalf("reports differ:\n%+v\n%+v", r1, r2)
	}
	if len(r1.Defaults) == 0 {
		t.Fatal("expected default entries")
	}
}

func TestExplainDefaults_IncludesValidationWarnings(t *testing.T) {
	cfg := ProductionDefaults()
	cfg.Retry = RetryPolicy{Enabled: true, MaxAttempts: 3}
	report := ExplainDefaults(cfg)
	if !hasIssueCodeInIssues(report.Warnings, CodeConfigUnsafeRetryWithoutIdempotency) {
		t.Fatalf("expected unsafe retry warning in %+v", report.Warnings)
	}
}

func TestUnsetObservability_WarnsOnValidate(t *testing.T) {
	cfg := newTestConfig()
	cfg.Observability = ObservabilityConfig{}
	report := ValidateConfig(cfg)
	if !hasIssueCode(report, CodeConfigObservabilityFullDefaultsResolved) {
		t.Fatalf("missing warning %+v", report.Issues)
	}
}

func TestObservabilityHotPathHeavy_Warns(t *testing.T) {
	cfg := newTestConfig()
	cfg.Observability = DefaultObservabilityConfig()
	report := ValidateConfig(cfg)
	if !hasIssueCode(report, CodeConfigDebugSnapshotHotPathHeavy) {
		t.Fatalf("missing hot-path warning %+v", report.Issues)
	}
}

func TestProductionDefaultsMatrix(t *testing.T) {
	cfg := ProductionDefaults()
	matrix := []struct {
		path    string
		enabled bool
	}{
		{"retry", cfg.Retry.Enabled},
		{"continuation", cfg.Continuation.Enabled},
		{"backend", cfg.BackendResources.Enabled},
		{"hotkey", cfg.HotKey.Enabled},
		{"per_key", cfg.PerKeyAdmission.Enabled},
		{"shard_pressure", cfg.ShardPressure.Enabled},
		{"autoscaling", cfg.AutoscalingSignal.Enabled},
		{"adaptive_quota", cfg.AdaptiveQuota.Config.Enabled},
	}
	for _, row := range matrix {
		if row.enabled {
			t.Errorf("subsystem %s must be disabled in ProductionDefaults", row.path)
		}
	}
	if cfg.HotKey.ExposeRawKey {
		t.Error("raw key labels must be disabled")
	}
}

func hasIssueCodeInIssues(issues []ValidationIssue, code string) bool {
	for _, issue := range issues {
		if issue.Code == code {
			return true
		}
	}
	return false
}
