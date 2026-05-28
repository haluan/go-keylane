// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
)

// ConfigVersion identifies the configuration schema contract.
type ConfigVersion string

// ConfigVersionV1 is the current configuration schema version.
const ConfigVersionV1 ConfigVersion = "keylane.config.v1"

// ValidationSeverity classifies a validation issue.
type ValidationSeverity string

const (
	// ValidationError is a fatal issue that prevents Queue construction.
	ValidationError ValidationSeverity = "error"
	// ValidationWarning is a non-fatal risk signal; Queue construction may proceed.
	ValidationWarning ValidationSeverity = "warning"
)

// Stable validation issue codes for automation and tests.
const (
	CodeConfigInvalidShardCount                 = "KL_CONFIG_INVALID_SHARD_COUNT"
	CodeConfigInvalidWorkerCount                = "KL_CONFIG_INVALID_WORKER_COUNT"
	CodeConfigInvalidQueueCapacity              = "KL_CONFIG_INVALID_QUEUE_CAPACITY"
	CodeConfigInvalidLaneQuota                  = "KL_CONFIG_INVALID_LANE_QUOTA"
	CodeConfigMissingLaneQuotas                 = "KL_CONFIG_MISSING_LANE_QUOTAS"
	CodeConfigInvalidLane                       = "KL_CONFIG_INVALID_LANE"
	CodeConfigInvalid                           = "KL_CONFIG_INVALID"
	CodeConfigUnboundedRetry                    = "KL_CONFIG_UNBOUNDED_RETRY"
	CodeConfigInvalidBackoff                    = "KL_CONFIG_INVALID_BACKOFF"
	CodeConfigUnsafeRetryWithoutIdempotency     = "KL_CONFIG_UNSAFE_RETRY_WITHOUT_IDEMPOTENCY"
	CodeConfigContinuationTimeoutMissing        = "KL_CONFIG_CONTINUATION_TIMEOUT_MISSING"
	CodeConfigBackendResourcesEnabled           = "KL_CONFIG_BACKEND_RESOURCES_ENABLED"
	CodeConfigPressureProviderObservational     = "KL_CONFIG_PRESSURE_PROVIDER_OBSERVATIONAL_ONLY"
	CodeConfigRawKeyExposureEnabled             = "KL_CONFIG_RAW_KEY_EXPOSURE_ENABLED"
	CodeConfigHighCardinalityLabelRisk          = "KL_CONFIG_HIGH_CARDINALITY_LABEL_RISK"
	CodeConfigWorkerCountExceedsGOMAXPROCS      = "KL_CONFIG_WORKER_COUNT_EXCEEDS_GOMAXPROCS"
	CodeConfigHighQueueCapacity                 = "KL_CONFIG_HIGH_QUEUE_CAPACITY"
	CodeConfigObservabilityFullDefaultsResolved = "KL_CONFIG_OBSERVABILITY_FULL_DEFAULTS_RESOLVED"
	CodeConfigDebugSnapshotHotPathHeavy         = "KL_CONFIG_DEBUG_SNAPSHOT_HOT_PATH_HEAVY"
	CodeConfigRawRequestIdentifiersInHooks      = "KL_CONFIG_RAW_REQUEST_IDENTIFIERS_IN_HOOKS"
)

const (
	maxRetryAttemptsCap       = 256
	highQueueSizePerLane      = 10_000
	workerCountGOMAXPROCSMult = 4
)

// ValidationIssue is one structured validation finding with a stable Code for automation.
type ValidationIssue struct {
	Severity ValidationSeverity
	Code     string
	Path     string
	Message  string
}

// ValidationReport is the outcome of ValidateConfig.
type ValidationReport struct {
	Version ConfigVersion
	Issues  []ValidationIssue

	firstErr error // retained for Err(); matches validateNormalizedConfig output
}

// HasErrors reports whether the report contains any fatal issues.
func (r ValidationReport) HasErrors() bool {
	for _, issue := range r.Issues {
		if issue.Severity == ValidationError {
			return true
		}
	}
	return false
}

// HasWarnings reports whether the report contains any warnings.
func (r ValidationReport) HasWarnings() bool {
	for _, issue := range r.Issues {
		if issue.Severity == ValidationWarning {
			return true
		}
	}
	return false
}

// Err returns the first fatal validation error for backward-compatible error handling.
// When no errors exist, Err returns nil.
func (r ValidationReport) Err() error {
	return r.firstErr
}

// ValidateConfig validates cfg and returns a structured report.
// Explicitly invalid subsystem values are checked before normalization; remaining rules run after defaults apply.
// Validation is cold-path only; it does not mutate cfg.
func ValidateConfig(cfg Config) ValidationReport {
	report := ValidationReport{Version: ConfigVersionV1}
	report.firstErr = validateConfigBeforeNormalize(cfg)
	if report.firstErr != nil {
		report.Issues = append(report.Issues, validationIssuesFromError(report.firstErr)...)
		sortValidationIssues(report.Issues)
		return report
	}
	c := cfg
	normalizeConfigInPlace(&c)
	report.firstErr = validateNormalizedConfig(c)
	if report.firstErr != nil {
		report.Issues = append(report.Issues, validationIssuesFromError(report.firstErr)...)
	}
	report.Issues = append(report.Issues, collectConfigWarnings(c)...)
	sortValidationIssues(report.Issues)
	return report
}

// validateConfigBeforeNormalize rejects values that normalization would mask (negative durations, caps, etc.).
func validateConfigBeforeNormalize(c Config) error {
	if err := validateCoreConfig(c); err != nil {
		return err
	}
	if err := ValidateAdaptiveQuotaPolicy(c.AdaptiveQuota, c.LaneQuotas); err != nil {
		return err
	}
	if err := validateHotKeyBeforeNormalize(c.HotKey); err != nil {
		return err
	}
	if err := validatePerKeyAdmissionBeforeNormalize(c.PerKeyAdmission); err != nil {
		return err
	}
	if err := validateShardPressureBeforeNormalize(c.ShardPressure); err != nil {
		return err
	}
	if err := validateAutoscalingBeforeNormalize(c.AutoscalingSignal); err != nil {
		return err
	}
	if err := validateRetryPolicyBeforeNormalize(c.Retry); err != nil {
		return err
	}
	if err := validateContinuationBeforeNormalize(c.Continuation); err != nil {
		return err
	}
	return nil
}

func validateCoreConfig(c Config) error {
	if c.ShardCount < 1 {
		return fmt.Errorf("%w: ShardCount must be at least 1", ErrInvalidShardCount)
	}
	if c.WorkerCount < 1 {
		return fmt.Errorf("%w: WorkerCount must be at least 1", ErrInvalidWorkerCount)
	}
	if c.QueueSizePerLane < 1 {
		return fmt.Errorf("%w: QueueSizePerLane must be at least 1", ErrInvalidQueueSize)
	}
	if len(c.LaneQuotas) == 0 {
		return ErrMissingLaneQuotas
	}
	for lane, quota := range c.LaneQuotas {
		if lane == "" {
			return fmt.Errorf("%w: lane name cannot be empty", ErrInvalidLane)
		}
		if quota < 1 {
			return fmt.Errorf("%w: quota for lane %q must be at least 1", ErrInvalidLaneQuota, lane)
		}
	}
	return nil
}

func validateRetryPolicyBeforeNormalize(cfg RetryPolicy) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.MaxAttempts < 0 {
		return fmt.Errorf("%w: MaxAttempts cannot be negative", ErrInvalidRetryPolicy)
	}
	if cfg.InitialBackoff < 0 {
		return fmt.Errorf("%w: InitialBackoff must be non-negative", ErrInvalidRetryPolicy)
	}
	if cfg.MaxBackoff < 0 {
		return fmt.Errorf("%w: MaxBackoff must be non-negative", ErrInvalidRetryPolicy)
	}
	if cfg.Multiplier < 0 {
		return fmt.Errorf("%w: Multiplier cannot be negative", ErrInvalidRetryPolicy)
	}
	if cfg.JitterFraction < 0 {
		return fmt.Errorf("%w: JitterFraction must be between 0 and 1", ErrInvalidRetryPolicy)
	}
	if cfg.MinRemainingBudget < 0 {
		return fmt.Errorf("%w: MinRemainingBudget must be non-negative", ErrInvalidRetryPolicy)
	}
	return nil
}

func validateContinuationBeforeNormalize(cfg ContinuationConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.MaxPending < 0 {
		return fmt.Errorf("%w: MaxPending cannot be negative", ErrInvalidContinuation)
	}
	if cfg.MaxPendingPerShard < 0 {
		return fmt.Errorf("%w: MaxPendingPerShard cannot be negative", ErrInvalidContinuation)
	}
	if cfg.CompletionRetention < 0 {
		return fmt.Errorf("%w: CompletionRetention cannot be negative", ErrInvalidContinuation)
	}
	return nil
}

func validateHotKeyBeforeNormalize(cfg HotKeyConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.MaxTrackedKeysPerShard < 0 {
		return fmt.Errorf("%w: MaxTrackedKeysPerShard cannot be negative", ErrInvalidHotKeyConfig)
	}
	if cfg.DetectionWindow < 0 {
		return fmt.Errorf("%w: DetectionWindow cannot be negative", ErrInvalidHotKeyConfig)
	}
	if cfg.HotKeyDepthRatio < 0 {
		return fmt.Errorf("%w: HotKeyDepthRatio cannot be negative", ErrInvalidHotKeyConfig)
	}
	if cfg.HotKeyWaitRatio < 0 {
		return fmt.Errorf("%w: HotKeyWaitRatio cannot be negative", ErrInvalidHotKeyConfig)
	}
	if cfg.MaxCandidatesPerSnapshot < 0 {
		return fmt.Errorf("%w: MaxCandidatesPerSnapshot cannot be negative", ErrInvalidHotKeyConfig)
	}
	return nil
}

func validatePerKeyAdmissionBeforeNormalize(cfg PerKeyAdmissionConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.MaxQueuedPerKey < 0 {
		return fmt.Errorf("%w: MaxQueuedPerKey cannot be negative", ErrInvalidPerKeyAdmissionConfig)
	}
	if cfg.MaxInflightPerKey < 0 {
		return fmt.Errorf("%w: MaxInflightPerKey cannot be negative", ErrInvalidPerKeyAdmissionConfig)
	}
	if cfg.PressureRatioThreshold < 0 {
		return fmt.Errorf("%w: PressureRatioThreshold cannot be negative", ErrInvalidPerKeyAdmissionConfig)
	}
	if cfg.RejectRatioThreshold < 0 {
		return fmt.Errorf("%w: RejectRatioThreshold cannot be negative", ErrInvalidPerKeyAdmissionConfig)
	}
	if cfg.Cooldown < 0 {
		return fmt.Errorf("%w: Cooldown cannot be negative", ErrInvalidPerKeyAdmissionConfig)
	}
	if cfg.RecoveryWindow < 0 {
		return fmt.Errorf("%w: RecoveryWindow cannot be negative", ErrInvalidPerKeyAdmissionConfig)
	}
	return nil
}

func validateShardPressureBeforeNormalize(cfg ShardPressureConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.Window < 0 {
		return fmt.Errorf("%w: Window cannot be negative", ErrInvalidShardPressureConfig)
	}
	if cfg.HotShardPressureRatio < 0 || cfg.DominantLaneRatio < 0 || cfg.LocalizedHotKeyRatio < 0 ||
		cfg.DistributedShardRatio < 0 || cfg.WorkerBusyRatio < 0 {
		return fmt.Errorf("%w: ratio thresholds cannot be negative", ErrInvalidShardPressureConfig)
	}
	if cfg.MaxHotShards < 0 || cfg.MaxLaneBreakdownPerShard < 0 || cfg.MaxHotKeyCandidatesPerShard < 0 {
		return fmt.Errorf("%w: snapshot limits cannot be negative", ErrInvalidShardPressureConfig)
	}
	return nil
}

func validateAutoscalingBeforeNormalize(cfg AutoscalingSignalConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.Window < 0 {
		return fmt.Errorf("%w: Window cannot be negative", ErrInvalidAutoscalingSignalConfig)
	}
	if cfg.ConsecutiveWindows < 0 {
		return fmt.Errorf("%w: ConsecutiveWindows cannot be negative", ErrInvalidAutoscalingSignalConfig)
	}
	if cfg.QueueDepthRatioThreshold < 0 || cfg.QueueWaitMaxThreshold < 0 ||
		cfg.AdmissionRejectRateThreshold < 0 || cfg.AdmissionShedRateThreshold < 0 ||
		cfg.WorkerBusyRatioThreshold < 0 || cfg.HotShardRatioThreshold < 0 ||
		cfg.ManyHotShardsThreshold < 0 || cfg.LocalizedHotKeyRatioThreshold < 0 {
		return fmt.Errorf("%w: thresholds cannot be negative", ErrInvalidAutoscalingSignalConfig)
	}
	return nil
}

func validateNormalizedConfig(c Config) error {
	if err := validateCoreConfig(c); err != nil {
		return err
	}
	if err := ValidateAdaptiveQuotaPolicy(c.AdaptiveQuota, c.LaneQuotas); err != nil {
		return err
	}
	if err := ValidateHotKeyConfig(c.HotKey); err != nil {
		return err
	}
	if err := ValidatePerKeyAdmissionConfig(c.PerKeyAdmission, c.HotKey); err != nil {
		return err
	}
	if err := ValidateShardPressureConfig(c.ShardPressure); err != nil {
		return err
	}
	if err := ValidateAutoscalingSignalConfig(c.AutoscalingSignal); err != nil {
		return err
	}
	if err := ValidateRetryPolicy(c.Retry); err != nil {
		return err
	}
	if c.Retry.Enabled && c.Retry.MaxAttempts > maxRetryAttemptsCap {
		return fmt.Errorf("%w: MaxAttempts %d exceeds cap %d", ErrInvalidRetryPolicy, c.Retry.MaxAttempts, maxRetryAttemptsCap)
	}
	if err := ValidateIdempotencyPolicy(c.Idempotency); err != nil {
		return err
	}
	if err := ValidateRetrySuppressionPolicy(c.RetrySuppression); err != nil {
		return err
	}
	if err := ValidateContinuationConfig(c.Continuation); err != nil {
		return err
	}
	return ValidateBackendResourceConfig(c.BackendResources)
}

func validationIssuesFromError(err error) []ValidationIssue {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ErrInvalidShardCount):
		return []ValidationIssue{{ValidationError, CodeConfigInvalidShardCount, "ShardCount", err.Error()}}
	case errors.Is(err, ErrInvalidWorkerCount):
		return []ValidationIssue{{ValidationError, CodeConfigInvalidWorkerCount, "WorkerCount", err.Error()}}
	case errors.Is(err, ErrInvalidQueueSize):
		return []ValidationIssue{{ValidationError, CodeConfigInvalidQueueCapacity, "QueueSizePerLane", err.Error()}}
	case errors.Is(err, ErrMissingLaneQuotas):
		return []ValidationIssue{{ValidationError, CodeConfigMissingLaneQuotas, "LaneQuotas", err.Error()}}
	case errors.Is(err, ErrInvalidLane):
		return []ValidationIssue{{ValidationError, CodeConfigInvalidLane, "LaneQuotas", err.Error()}}
	case errors.Is(err, ErrInvalidLaneQuota):
		return []ValidationIssue{{ValidationError, CodeConfigInvalidLaneQuota, "LaneQuotas", err.Error()}}
	case errors.Is(err, ErrInvalidRetryPolicy):
		code := CodeConfigInvalidBackoff
		if containsUnboundedRetryMsg(err.Error()) {
			code = CodeConfigUnboundedRetry
		}
		return []ValidationIssue{{ValidationError, code, "Retry", err.Error()}}
	case errors.Is(err, ErrInvalidContinuation):
		return []ValidationIssue{{ValidationError, CodeConfigInvalid, "Continuation", err.Error()}}
	case errors.Is(err, ErrInvalidHotKeyConfig):
		return []ValidationIssue{{ValidationError, CodeConfigInvalid, "HotKey", err.Error()}}
	case errors.Is(err, ErrInvalidPerKeyAdmissionConfig):
		return []ValidationIssue{{ValidationError, CodeConfigInvalid, "PerKeyAdmission", err.Error()}}
	case errors.Is(err, ErrInvalidShardPressureConfig):
		return []ValidationIssue{{ValidationError, CodeConfigInvalid, "ShardPressure", err.Error()}}
	case errors.Is(err, ErrInvalidAutoscalingSignalConfig):
		return []ValidationIssue{{ValidationError, CodeConfigInvalid, "AutoscalingSignal", err.Error()}}
	case errors.Is(err, ErrInvalidAdaptiveQuotaConfig):
		return []ValidationIssue{{ValidationError, CodeConfigInvalid, "AdaptiveQuota", err.Error()}}
	case errors.Is(err, ErrInvalidRetrySuppressionPolicy):
		return []ValidationIssue{{ValidationError, CodeConfigInvalid, "RetrySuppression", err.Error()}}
	case errors.Is(err, ErrInvalidConfig):
		path := "BackendResources"
		if strings.Contains(err.Error(), "Continuation") {
			path = "Continuation"
		}
		return []ValidationIssue{{ValidationError, CodeConfigInvalid, path, err.Error()}}
	default:
		return []ValidationIssue{{ValidationError, CodeConfigInvalid, "", err.Error()}}
	}
}

func containsUnboundedRetryMsg(s string) bool {
	return strings.Contains(s, "exceeds cap")
}

func collectConfigWarnings(c Config) []ValidationIssue {
	var issues []ValidationIssue
	gomax := runtime.GOMAXPROCS(0)
	if gomax < 1 {
		gomax = 1
	}
	if c.WorkerCount > gomax*workerCountGOMAXPROCSMult {
		issues = append(issues, ValidationIssue{
			Severity: ValidationWarning,
			Code:     CodeConfigWorkerCountExceedsGOMAXPROCS,
			Path:     "WorkerCount",
			Message:  fmt.Sprintf("WorkerCount %d is much larger than GOMAXPROCS(%d)*%d", c.WorkerCount, gomax, workerCountGOMAXPROCSMult),
		})
	}
	if c.QueueSizePerLane >= highQueueSizePerLane {
		issues = append(issues, ValidationIssue{
			Severity: ValidationWarning,
			Code:     CodeConfigHighQueueCapacity,
			Path:     "QueueSizePerLane",
			Message:  fmt.Sprintf("QueueSizePerLane %d may increase memory pressure", c.QueueSizePerLane),
		})
	}
	if c.Retry.Enabled && c.Idempotency.Hook == nil && !c.Idempotency.RequireForRetry {
		issues = append(issues, ValidationIssue{
			Severity: ValidationWarning,
			Code:     CodeConfigUnsafeRetryWithoutIdempotency,
			Path:     "Idempotency",
			Message:  "retry is enabled without RequireForRetry or RetrySafetyHook; mutation workloads may retry unsafely",
		})
	}
	if c.Continuation.Enabled {
		issues = append(issues, ValidationIssue{
			Severity: ValidationWarning,
			Code:     CodeConfigContinuationTimeoutMissing,
			Path:     "Continuation",
			Message:  "continuations enabled; CompletionRetention is reserved and not enforced by the runtime—define timeouts on pipeline stages and request contexts",
		})
	}
	if c.HotKey.Enabled && c.HotKey.ExposeRawKey {
		issues = append(issues, ValidationIssue{
			Severity: ValidationWarning,
			Code:     CodeConfigRawKeyExposureEnabled,
			Path:     "HotKey.ExposeRawKey",
			Message:  "raw key strings may appear in debug snapshots and must not be used as metric or trace labels",
		})
	}
	obsForWarn := ResolveObservabilityConfig(c.Observability)
	if obsForWarn.ExposeRawRequestIdentifiers {
		issues = append(issues, ValidationIssue{
			Severity: ValidationWarning,
			Code:     CodeConfigRawRequestIdentifiersInHooks,
			Path:     "Observability.ExposeRawRequestIdentifiers",
			Message:  "request observations and hook payloads include raw Key and RequestID (including httpkeylane.ObserveFunc); do not export them as metric or trace labels",
		})
	}
	if c.BackendResources.Enabled {
		issues = append(issues, ValidationIssue{
			Severity: ValidationWarning,
			Code:     CodeConfigBackendResourcesEnabled,
			Path:     "BackendResources",
			Message:  "backend resource coordination is enabled; ensure AcquireBackend/ReleaseBackend discipline in handlers",
		})
	}
	if len(c.BackendResources.PressureProviders) > 0 {
		msg := "pressure providers are observational only; pool saturation does not automatically reject backend admission"
		if !c.BackendResources.Enabled {
			msg = "pressure providers are configured but BackendResources.Enabled is false (observational only)"
		}
		issues = append(issues, ValidationIssue{
			Severity: ValidationWarning,
			Code:     CodeConfigPressureProviderObservational,
			Path:     "BackendResources.PressureProviders",
			Message:  msg,
		})
	}
	if isUnsetObservabilityConfig(c.Observability) {
		issues = append(issues, ValidationIssue{
			Severity: ValidationWarning,
			Code:     CodeConfigObservabilityFullDefaultsResolved,
			Path:     "Observability",
			Message:  "unset Observability resolves to DefaultObservabilityConfig at New; use LowAllocationObservabilityConfig or explicit flags for production",
		})
	}
	obs := ResolveObservabilityConfig(c.Observability)
	if !c.Observability.LowAllocationMode &&
		obs.EnableDebugSnapshot && obs.EnableQueueWaitTiming && obs.EnableRunTiming {
		issues = append(issues, ValidationIssue{
			Severity: ValidationWarning,
			Code:     CodeConfigDebugSnapshotHotPathHeavy,
			Path:     "Observability",
			Message:  "debug snapshot with queue-wait and run timing enabled on workers may increase hot-path overhead; prefer LowAllocationObservabilityConfig",
		})
	}
	if obs.EnableHooks && obs.EnableDebugSnapshot && c.HotKey.Enabled {
		issues = append(issues, ValidationIssue{
			Severity: ValidationWarning,
			Code:     CodeConfigHighCardinalityLabelRisk,
			Path:     "Observability",
			Message:  "hooks, debug snapshots, and hot key tracking together may increase metric/log cardinality",
		})
	}
	return issues
}

func sortValidationIssues(issues []ValidationIssue) {
	sort.Slice(issues, func(i, j int) bool {
		if issues[i].Code != issues[j].Code {
			return issues[i].Code < issues[j].Code
		}
		if issues[i].Path != issues[j].Path {
			return issues[i].Path < issues[j].Path
		}
		return issues[i].Message < issues[j].Message
	})
}

func validationWarningsFromReport(r ValidationReport) []ValidationIssue {
	var out []ValidationIssue
	for _, issue := range r.Issues {
		if issue.Severity == ValidationWarning {
			out = append(out, issue)
		}
	}
	return out
}

func validationIssuesCopy(r ValidationReport) []ValidationIssue {
	if len(r.Issues) == 0 {
		return nil
	}
	out := make([]ValidationIssue, len(r.Issues))
	copy(out, r.Issues)
	return out
}
