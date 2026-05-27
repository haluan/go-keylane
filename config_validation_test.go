// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestValidateConfigZeroValueHasErrors(t *testing.T) {
	report := ValidateConfig(Config{})
	if !report.HasErrors() {
		t.Fatal("expected errors for zero Config")
	}
	if report.Err() == nil {
		t.Fatal("expected Err() non-nil")
	}
}

func TestValidateConfigValidNoErrors(t *testing.T) {
	report := ValidateConfig(newTestConfig())
	if report.HasErrors() {
		t.Fatalf("unexpected errors: %v", report.Issues)
	}
}

func TestValidateConfigErrMatchesValidate(t *testing.T) {
	cases := []Config{
		{ShardCount: 0, WorkerCount: 1, QueueSizePerLane: 1, LaneQuotas: map[Lane]int{"default": 1}},
		{ShardCount: 1, WorkerCount: 0, QueueSizePerLane: 1, LaneQuotas: map[Lane]int{"default": 1}},
		{ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 0, LaneQuotas: map[Lane]int{"default": 1}},
		{ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 1, LaneQuotas: nil},
	}
	for _, cfg := range cases {
		report := ValidateConfig(cfg)
		if !errors.Is(report.Err(), cfg.Validate()) && (report.Err() != nil || cfg.Validate() != nil) {
			if (report.Err() == nil) != (cfg.Validate() == nil) {
				t.Fatalf("Err mismatch: report=%v validate=%v", report.Err(), cfg.Validate())
			}
		}
	}
}

func TestValidateConfigInvalidWorkerCountCode(t *testing.T) {
	report := ValidateConfig(Config{
		ShardCount: 1, WorkerCount: -1, QueueSizePerLane: 1,
		LaneQuotas: map[Lane]int{"default": 1},
	})
	if !errors.Is(report.Err(), ErrInvalidWorkerCount) {
		t.Fatalf("got %v", report.Err())
	}
	if !hasIssueCode(report, CodeConfigInvalidWorkerCount) {
		t.Fatalf("missing code %s in %+v", CodeConfigInvalidWorkerCount, report.Issues)
	}
}

func TestValidateConfigUnboundedRetry(t *testing.T) {
	cfg := newTestConfig()
	cfg.Retry = RetryPolicy{Enabled: true, MaxAttempts: maxRetryAttemptsCap + 1}
	report := ValidateConfig(cfg)
	if !errors.Is(report.Err(), ErrInvalidRetryPolicy) {
		t.Fatalf("got %v", report.Err())
	}
	if !hasIssueCode(report, CodeConfigUnboundedRetry) {
		t.Fatalf("missing code %s", CodeConfigUnboundedRetry)
	}
}

func TestValidateConfigUnsafeRetryWarning(t *testing.T) {
	cfg := newTestConfig()
	cfg.Retry = RetryPolicy{Enabled: true, MaxAttempts: 3}
	cfg.Idempotency = IdempotencyPolicy{}
	report := ValidateConfig(cfg)
	if !report.HasWarnings() {
		t.Fatal("expected warning for retry without idempotency controls")
	}
	if !hasIssueCode(report, CodeConfigUnsafeRetryWithoutIdempotency) {
		t.Fatalf("missing warning code in %+v", report.Issues)
	}
	if report.HasErrors() {
		t.Fatalf("warnings must not be errors: %+v", report.Issues)
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.ConfigValidationWarnings()) == 0 {
		t.Fatal("expected warnings on Queue")
	}
}

func TestValidateConfigRawKeyExposureWarning(t *testing.T) {
	cfg := newTestConfig()
	cfg.HotKey = HotKeyConfig{Enabled: true, ExposeRawKey: true, MaxTrackedKeysPerShard: 8}
	report := ValidateConfig(cfg)
	if !hasIssueCode(report, CodeConfigRawKeyExposureEnabled) {
		t.Fatalf("missing warning %+v", report.Issues)
	}
}

func TestValidateConfigIssueOrderingDeterministic(t *testing.T) {
	cfg := newTestConfig()
	cfg.WorkerCount = runtime.GOMAXPROCS(0)*workerCountGOMAXPROCSMult + 10
	cfg.QueueSizePerLane = highQueueSizePerLane
	cfg.Retry = RetryPolicy{Enabled: true, MaxAttempts: 3}
	cfg.HotKey = HotKeyConfig{Enabled: true, ExposeRawKey: true, MaxTrackedKeysPerShard: 8}
	r1 := ValidateConfig(cfg)
	r2 := ValidateConfig(cfg)
	if len(r1.Issues) != len(r2.Issues) {
		t.Fatalf("issue count differ: %d vs %d", len(r1.Issues), len(r2.Issues))
	}
	for i := range r1.Issues {
		if r1.Issues[i] != r2.Issues[i] {
			t.Fatalf("issue %d differ: %+v vs %+v", i, r1.Issues[i], r2.Issues[i])
		}
	}
}

func TestNewFailsBeforeStartOnFatalConfig(t *testing.T) {
	_, err := New(Config{})
	if err == nil {
		t.Fatal("expected New error")
	}
}

func TestValidateConfigContinuationWarningNotSilencedByRetention(t *testing.T) {
	cfg := newTestConfig()
	cfg.Continuation = ContinuationConfig{
		Enabled:             true,
		MaxPending:          64,
		CompletionRetention: time.Hour,
	}
	report := ValidateConfig(cfg)
	if !hasIssueCode(report, CodeConfigContinuationTimeoutMissing) {
		t.Fatalf("positive CompletionRetention must not suppress continuation guidance warning: %+v", report.Issues)
	}
	for _, issue := range report.Issues {
		if issue.Code == CodeConfigContinuationTimeoutMissing && issue.Path == "Continuation.CompletionRetention" {
			t.Fatalf("warning must not point at reserved CompletionRetention path")
		}
	}
}

func TestValidateConfigContinuationNegativeRetention(t *testing.T) {
	cfg := newTestConfig()
	cfg.Continuation = ContinuationConfig{Enabled: true, MaxPending: 10, CompletionRetention: -1}
	report := ValidateConfig(cfg)
	if !errors.Is(report.Err(), ErrInvalidContinuation) {
		t.Fatalf("got %v", report.Err())
	}
}

func TestValidateConfigContinuationNegativeMaxPending(t *testing.T) {
	cfg := newTestConfig()
	cfg.Continuation = ContinuationConfig{Enabled: true, MaxPending: -1}
	report := ValidateConfig(cfg)
	if !errors.Is(report.Err(), ErrInvalidContinuation) {
		t.Fatalf("got %v", report.Err())
	}
	if !hasIssueCode(report, CodeConfigInvalid) {
		t.Fatalf("expected continuation error code, got %+v", report.Issues)
	}
}

func TestValidateConfigRetryNegativeBackoffNotMasked(t *testing.T) {
	cfg := newTestConfig()
	cfg.Retry = RetryPolicy{Enabled: true, InitialBackoff: -1}
	report := ValidateConfig(cfg)
	if !errors.Is(report.Err(), ErrInvalidRetryPolicy) {
		t.Fatalf("negative InitialBackoff must fail before normalization, got %v", report.Err())
	}
	if !hasIssueCode(report, CodeConfigInvalidBackoff) {
		t.Fatalf("missing %s in %+v", CodeConfigInvalidBackoff, report.Issues)
	}
}

func TestValidateConfigInvalidQueueCapacityCode(t *testing.T) {
	report := ValidateConfig(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: -1,
		LaneQuotas: map[Lane]int{"default": 1},
	})
	if !hasIssueCode(report, CodeConfigInvalidQueueCapacity) {
		t.Fatalf("missing code %+v", report.Issues)
	}
	if report.Issues[0].Severity != ValidationError {
		t.Fatalf("want error severity")
	}
}

func TestValidateConfigInvalidLaneQuotaCode(t *testing.T) {
	report := ValidateConfig(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 1,
		LaneQuotas: map[Lane]int{"default": 0},
	})
	if !hasIssueCode(report, CodeConfigInvalidLaneQuota) {
		t.Fatalf("missing code %+v", report.Issues)
	}
}

func TestValidateConfigBackendResourcesEnabledWarning(t *testing.T) {
	cfg := newTestConfig()
	cfg.BackendResources = BackendResourceConfig{
		Enabled: true,
		Resources: map[BackendResourceName]BackendResourcePolicy{
			"db": {Lanes: map[BackendLane]BackendLanePolicy{
				"read": {MaxInFlight: 4},
			}},
		},
	}
	report := ValidateConfig(cfg)
	if !hasIssueCode(report, CodeConfigBackendResourcesEnabled) {
		t.Fatalf("missing warning %+v", report.Issues)
	}
}

func TestValidateConfigPressureProviderObservationalWarning(t *testing.T) {
	cfg := newTestConfig()
	cfg.BackendResources = BackendResourceConfig{
		PressureProviders: []BackendPressureProvider{noopPressureProvider{}},
	}
	report := ValidateConfig(cfg)
	if !hasIssueCode(report, CodeConfigPressureProviderObservational) {
		t.Fatalf("missing warning %+v", report.Issues)
	}
}

func TestValidateConfigPressureProviderObservationalWhenCoordinationEnabled(t *testing.T) {
	cfg := newTestConfig()
	cfg.BackendResources = BackendResourceConfig{
		Enabled: true,
		Resources: map[BackendResourceName]BackendResourcePolicy{
			"db": {Lanes: map[BackendLane]BackendLanePolicy{
				"read": {MaxInFlight: 4},
			}},
		},
		PressureProviders: []BackendPressureProvider{noopPressureProvider{}},
	}
	report := ValidateConfig(cfg)
	if !hasIssueCode(report, CodeConfigPressureProviderObservational) {
		t.Fatalf("missing observational pressure warning when coordination enabled: %+v", report.Issues)
	}
	for _, issue := range report.Issues {
		if issue.Code == CodeConfigPressureProviderObservational &&
			strings.Contains(issue.Message, "BackendResources.Enabled is false") {
			t.Fatalf("enabled coordination must not use disabled-only message: %q", issue.Message)
		}
	}
}

func TestValidateConfigHighCardinalityWarning(t *testing.T) {
	cfg := newTestConfig()
	cfg.HotKey = DefaultHotKeyConfig()
	cfg.Observability = ObservabilityConfig{
		EnableHooks:         true,
		EnableDebugSnapshot: true,
		Hooks: Hooks{
			OnJobTiming: func(JobTimingEvent) {},
		},
	}
	report := ValidateConfig(cfg)
	if !hasIssueCode(report, CodeConfigHighCardinalityLabelRisk) {
		t.Fatalf("missing warning %+v", report.Issues)
	}
}

// noopPressureProvider is a minimal observational provider for validation tests.
type noopPressureProvider struct{}

func (noopPressureProvider) BackendPressure(context.Context) BackendPressureSnapshot {
	return BackendPressureSnapshot{}
}

func hasIssueCode(r ValidationReport, code string) bool {
	for _, issue := range r.Issues {
		if issue.Code == code {
			return true
		}
	}
	return false
}
