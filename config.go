// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"time"
)

// Config configures a Queue: shard routing, workers, lane quotas, and optional subsystems.
// Zero values disable optional features (continuations, backend resources, hot keys, retry, etc.).
// Call Validate before New when setting non-default subsystem configs.
type Config struct {
	ShardCount       int
	WorkerCount      int
	QueueSizePerLane int
	LaneQuotas       map[Lane]int

	Observability ObservabilityConfig

	// OverloadEnabled applies overload policy evaluation on Job.Submit before enqueue.
	OverloadEnabled bool

	// AdmissionEnabled applies lane admission policy on Job.Submit before per-key admission and enqueue.
	AdmissionEnabled bool

	// AdaptiveQuota configures the optional adaptive quota controller (disabled by default).
	AdaptiveQuota AdaptiveQuotaPolicy

	// HotKey configures bounded per-shard hot key accounting and detection (zero value disables).
	HotKey HotKeyConfig

	// PerKeyAdmission applies targeted mitigation for hot keys (zero value disables).
	PerKeyAdmission PerKeyAdmissionConfig

	// ShardPressure enables shard pressure diagnostics (zero value disables rich snapshots).
	ShardPressure ShardPressureConfig

	// AutoscalingSignal enables scale signal calculation (zero value disables).
	AutoscalingSignal AutoscalingSignalConfig

	// FailurePolicy configures optional custom failure classification.
	FailurePolicy FailurePolicy

	// Retry configures optional bounded in-worker retry (zero value disables).
	Retry RetryPolicy

	// Idempotency configures duplicate-safety checks when retry is enabled.
	Idempotency IdempotencyPolicy

	// RetrySuppression configures runtime-health checks before scheduling a retry.
	RetrySuppression RetrySuppressionPolicy

	// Continuation configures the non-blocking continuation model (disabled by default).
	Continuation ContinuationConfig

	// BackendResources configures optional backend resource lane coordination (disabled by default).
	BackendResources BackendResourceConfig
}

// ContinuationConfig configures the bounded in-memory continuation registry.
//
// Experimental: may change before v1.0.
//
// Zero value disables continuations; stages using RunContinuation will return ErrContinuationDisabled at submit.
type ContinuationConfig struct {
	// Enabled must be true to allow continuation stages.
	Enabled bool

	// MaxPending is the global cap on pending continuations. When Enabled is true and MaxPending is zero,
	// NormalizeContinuationConfig applies DefaultContinuationMaxPending.
	MaxPending int

	// MaxPendingPerShard is an optional per-shard cap. Zero means no per-shard override.
	MaxPendingPerShard int

	// CompletionRetention is reserved for future completed-continuation diagnostics (currently unused).
	CompletionRetention time.Duration
}

type ObservabilityConfig struct {
	// EnableStats controls StatsGCPressure snapshot assembly (pull API; may allocate).
	EnableStats bool
	// EnableCounters controls cumulative admission and terminal counters on the hot path.
	EnableCounters bool
	// EnableQueueWaitTiming controls AcceptedAt stamping and StatsGCPressure queue-wait samples.
	EnableQueueWaitTiming bool
	// EnableRunTiming controls StatsGCPressure run-duration samples on the worker path.
	EnableRunTiming bool
	// EnableHooks controls OnJobTiming and OnSlowJob dispatch (Hooks are ignored when false).
	EnableHooks bool
	// EnableAdaptiveDecisionTracing emits hold/neutral adaptive decisions via OnAdaptiveQuotaDecision.
	// When false, only successful changes and apply failures invoke the hook.
	EnableAdaptiveDecisionTracing bool
	// EnableDebugSnapshot controls DebugSnapshot (Pressure remains available).
	EnableDebugSnapshot bool
	// LowAllocationMode applies LowAllocationObservabilityConfig at queue construction.
	LowAllocationMode bool
	// ExposeRawRequestIdentifiers includes raw Key and RequestID in hook payloads when true.
	// Default false: hooks receive KeyHash only for correlation; do not use raw values as metric labels.
	ExposeRawRequestIdentifiers bool

	// TrackQueueWait enables v1 Stats() queue-wait counters (EnqueuedAt); independent of EnableQueueWaitTiming.
	TrackQueueWait   bool
	SlowJobThreshold time.Duration
	Hooks            Hooks
}

// Validate ensures the configuration is valid.
// Invalid explicit values are rejected before normalization defaults are applied.
// For structured errors and warnings, use ValidateConfig.
func (c Config) Validate() error {
	if err := validateConfigBeforeNormalize(c); err != nil {
		return err
	}
	cp := c
	normalizeConfigInPlace(&cp)
	return validateNormalizedConfig(cp)
}
