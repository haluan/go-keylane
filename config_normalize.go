// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"sort"
	"time"
)

// normalizeConfigInPlace applies subsystem defaults to cfg without resolving observability
// (ResolveObservabilityConfig runs at Queue construction).
func normalizeConfigInPlace(cfg *Config) {
	if cfg == nil {
		return
	}
	NormalizeHotKeyConfig(&cfg.HotKey)
	NormalizePerKeyAdmissionConfig(&cfg.PerKeyAdmission)
	NormalizeShardPressureConfig(&cfg.ShardPressure)
	NormalizeAutoscalingSignalConfig(&cfg.AutoscalingSignal)
	NormalizeRetryPolicy(&cfg.Retry)
	NormalizeIdempotencyPolicy(&cfg.Idempotency)
	NormalizeRetrySuppressionPolicy(&cfg.RetrySuppression)
	NormalizeContinuationConfig(&cfg.Continuation)
	NormalizeBackendResourceConfig(&cfg.BackendResources)
	aq := cfg.AdaptiveQuota
	NormalizeAdaptiveQuotaConfig(&aq.Config)
	cfg.AdaptiveQuota = aq
}

// collectAppliedDefaults returns stable tokens for defaults applied during normalization.
func collectAppliedDefaults(before, after Config) []string {
	var out []string
	collectRetryAppliedDefaults(&out, before.Retry, after.Retry)
	collectContinuationAppliedDefaults(&out, before.Continuation, after.Continuation)
	collectHotKeyAppliedDefaults(&out, before.HotKey, after.HotKey)
	collectPerKeyAdmissionAppliedDefaults(&out, before.PerKeyAdmission, after.PerKeyAdmission)
	collectShardPressureAppliedDefaults(&out, before.ShardPressure, after.ShardPressure)
	collectAutoscalingAppliedDefaults(&out, before.AutoscalingSignal, after.AutoscalingSignal)
	collectRetrySuppressionAppliedDefaults(&out, before.RetrySuppression, after.RetrySuppression)
	collectAdaptiveQuotaAppliedDefaults(&out, before.AdaptiveQuota.Config, after.AdaptiveQuota.Config)
	collectBackendResourceAppliedDefaults(&out, before.BackendResources, after.BackendResources)
	sort.Strings(out)
	return out
}

func collectRetryAppliedDefaults(out *[]string, before, after RetryPolicy) {
	if !after.Enabled {
		return
	}
	appendDefaultInt(out, "retry.max_attempts=3", before.MaxAttempts <= 0, after.MaxAttempts == 3)
	appendDefaultDuration(out, "retry.initial_backoff=10ms", before.InitialBackoff <= 0, after.InitialBackoff == 10*time.Millisecond)
	appendDefaultDuration(out, "retry.max_backoff=250ms", before.MaxBackoff <= 0, after.MaxBackoff == 250*time.Millisecond)
	appendDefaultFloat(out, "retry.multiplier=2", before.Multiplier <= 0, after.Multiplier == 2.0)
	appendDefaultFloat(out, "retry.jitter_fraction=0.2", before.JitterFraction <= 0, after.JitterFraction == 0.2)
	if before.JitterFraction <= 0 && after.Jitter {
		*out = append(*out, "retry.jitter=true")
	}
	if before.MinRemainingBudget <= 0 && after.MinRemainingBudget == after.InitialBackoff && after.InitialBackoff > 0 {
		*out = append(*out, "retry.min_remaining_budget=initial_backoff")
	}
}

func collectContinuationAppliedDefaults(out *[]string, before, after ContinuationConfig) {
	if !after.Enabled {
		return
	}
	appendDefaultInt(out, "continuation.max_pending=256", before.MaxPending <= 0, after.MaxPending == DefaultContinuationMaxPending)
}

func collectHotKeyAppliedDefaults(out *[]string, before, after HotKeyConfig) {
	if !after.Enabled {
		return
	}
	def := DefaultHotKeyConfig()
	appendDefaultDuration(out, "hotkey.detection_window=30s", before.DetectionWindow <= 0, after.DetectionWindow == def.DetectionWindow)
	appendDefaultFloat(out, "hotkey.depth_ratio=0.40", before.HotKeyDepthRatio <= 0, after.HotKeyDepthRatio == def.HotKeyDepthRatio)
	appendDefaultFloat(out, "hotkey.wait_ratio=0.40", before.HotKeyWaitRatio <= 0, after.HotKeyWaitRatio == def.HotKeyWaitRatio)
	appendDefaultInt(out, "hotkey.max_candidates_per_snapshot=5", before.MaxCandidatesPerSnapshot <= 0, after.MaxCandidatesPerSnapshot == def.MaxCandidatesPerSnapshot)
}

func collectPerKeyAdmissionAppliedDefaults(out *[]string, before, after PerKeyAdmissionConfig) {
	if !after.Enabled {
		return
	}
	def := DefaultPerKeyAdmissionConfig()
	appendDefaultString(out, "per_key_admission.min_status=candidate", before.MinStatus == "", after.MinStatus == def.MinStatus)
	appendDefaultString(out, "per_key_admission.default_action=throttle", before.DefaultAction == "", string(after.DefaultAction) == string(def.DefaultAction))
	appendDefaultFloat(out, "per_key_admission.pressure_ratio=0.40", before.PressureRatioThreshold <= 0, after.PressureRatioThreshold == def.PressureRatioThreshold)
	appendDefaultFloat(out, "per_key_admission.reject_ratio=0.20", before.RejectRatioThreshold <= 0, after.RejectRatioThreshold == def.RejectRatioThreshold)
	appendDefaultDuration(out, "per_key_admission.cooldown=10s", before.Cooldown <= 0, after.Cooldown == def.Cooldown)
	appendDefaultDuration(out, "per_key_admission.recovery_window=30s", before.RecoveryWindow <= 0, after.RecoveryWindow == def.RecoveryWindow)
	appendDefaultInt(out, "per_key_admission.max_snapshots_per_shard=5", before.MaxSnapshotsPerShard <= 0, after.MaxSnapshotsPerShard == def.MaxSnapshotsPerShard)
	appendDefaultInt(out, "per_key_admission.max_snapshots_total=25", before.MaxSnapshotsTotal <= 0, after.MaxSnapshotsTotal == def.MaxSnapshotsTotal)
}

func collectShardPressureAppliedDefaults(out *[]string, before, after ShardPressureConfig) {
	if !after.Enabled {
		return
	}
	def := DefaultShardPressureConfig()
	appendDefaultDuration(out, "shard_pressure.window=30s", before.Window <= 0, after.Window == def.Window)
	appendDefaultFloat(out, "shard_pressure.hot_shard_ratio=0.70", before.HotShardPressureRatio <= 0, after.HotShardPressureRatio == def.HotShardPressureRatio)
	appendDefaultFloat(out, "shard_pressure.dominant_lane_ratio=0.60", before.DominantLaneRatio <= 0, after.DominantLaneRatio == def.DominantLaneRatio)
	appendDefaultFloat(out, "shard_pressure.localized_hot_key_ratio=0.40", before.LocalizedHotKeyRatio <= 0, after.LocalizedHotKeyRatio == def.LocalizedHotKeyRatio)
	appendDefaultFloat(out, "shard_pressure.distributed_shard_ratio=0.50", before.DistributedShardRatio <= 0, after.DistributedShardRatio == def.DistributedShardRatio)
	appendDefaultFloat(out, "shard_pressure.worker_busy_ratio=0.80", before.WorkerBusyRatio <= 0, after.WorkerBusyRatio == def.WorkerBusyRatio)
	appendDefaultInt(out, "shard_pressure.max_hot_shards=16", before.MaxHotShards <= 0, after.MaxHotShards == def.MaxHotShards)
	appendDefaultInt(out, "shard_pressure.max_lane_breakdown_per_shard=8", before.MaxLaneBreakdownPerShard <= 0, after.MaxLaneBreakdownPerShard == def.MaxLaneBreakdownPerShard)
	appendDefaultInt(out, "shard_pressure.max_hot_key_candidates_per_shard=4", before.MaxHotKeyCandidatesPerShard <= 0, after.MaxHotKeyCandidatesPerShard == def.MaxHotKeyCandidatesPerShard)
}

func collectAutoscalingAppliedDefaults(out *[]string, before, after AutoscalingSignalConfig) {
	if !after.Enabled {
		return
	}
	def := DefaultAutoscalingSignalConfig()
	appendDefaultDuration(out, "autoscaling.window=30s", before.Window <= 0, after.Window == def.Window)
	appendDefaultInt(out, "autoscaling.consecutive_windows=2", before.ConsecutiveWindows <= 0, after.ConsecutiveWindows == def.ConsecutiveWindows)
	appendDefaultFloat(out, "autoscaling.queue_depth_ratio=0.70", before.QueueDepthRatioThreshold <= 0, after.QueueDepthRatioThreshold == def.QueueDepthRatioThreshold)
	appendDefaultDuration(out, "autoscaling.queue_wait_max=50ms", before.QueueWaitMaxThreshold <= 0, after.QueueWaitMaxThreshold == def.QueueWaitMaxThreshold)
	appendDefaultFloat(out, "autoscaling.admission_reject_rate=0.05", before.AdmissionRejectRateThreshold <= 0, after.AdmissionRejectRateThreshold == def.AdmissionRejectRateThreshold)
	appendDefaultFloat(out, "autoscaling.admission_shed_rate=0.01", before.AdmissionShedRateThreshold <= 0, after.AdmissionShedRateThreshold == def.AdmissionShedRateThreshold)
	appendDefaultFloat(out, "autoscaling.worker_busy_ratio=0.85", before.WorkerBusyRatioThreshold <= 0, after.WorkerBusyRatioThreshold == def.WorkerBusyRatioThreshold)
	appendDefaultFloat(out, "autoscaling.hot_shard_ratio=0.70", before.HotShardRatioThreshold <= 0, after.HotShardRatioThreshold == def.HotShardRatioThreshold)
	appendDefaultInt(out, "autoscaling.many_hot_shards=4", before.ManyHotShardsThreshold <= 0, after.ManyHotShardsThreshold == def.ManyHotShardsThreshold)
	appendDefaultFloat(out, "autoscaling.localized_hot_key_ratio=0.40", before.LocalizedHotKeyRatioThreshold <= 0, after.LocalizedHotKeyRatioThreshold == def.LocalizedHotKeyRatioThreshold)
}

func collectRetrySuppressionAppliedDefaults(out *[]string, before, after RetrySuppressionPolicy) {
	if !after.Enabled {
		return
	}
	if !before.SuppressWhenOverloaded && !before.SuppressNonCriticalWhenPressured &&
		!before.SuppressOverloadFailures && !before.SuppressAdmissionFailures &&
		!before.SuppressPerKeyAdmissionFailures && !before.SuppressHotKeyRetry &&
		!before.SuppressWhenScaleOutRecommended && before.SuppressLaneAboveRatio == 0 &&
		before.SuppressShardAboveRatio == 0 && before.Hook == nil &&
		after.SuppressWhenOverloaded && after.SuppressNonCriticalWhenPressured {
		*out = append(*out, "retry_suppression.suppress_flags=defaults")
	}
	appendDefaultFloat(out, "retry_suppression.lane_ratio=0.70", before.SuppressLaneAboveRatio == 0, after.SuppressLaneAboveRatio == PressuredDepthRatio)
	appendDefaultFloat(out, "retry_suppression.shard_ratio=0.70", before.SuppressShardAboveRatio == 0, after.SuppressShardAboveRatio == PressuredDepthRatio)
}

func collectAdaptiveQuotaAppliedDefaults(out *[]string, before, after AdaptiveQuotaConfig) {
	def := DefaultAdaptiveQuotaConfig()
	appendDefaultDuration(out, "adaptive_quota.evaluation_interval=1s", before.EvaluationInterval <= 0, after.EvaluationInterval == def.EvaluationInterval)
	appendDefaultDuration(out, "adaptive_quota.warmup_duration=5s", before.WarmupDuration <= 0, after.WarmupDuration == def.WarmupDuration)
	appendDefaultDuration(out, "adaptive_quota.cooldown_duration=5s", before.CooldownDuration <= 0, after.CooldownDuration == def.CooldownDuration)
	appendDefaultFloat(out, "adaptive_quota.pressure_high=0.85", before.PressureHigh <= 0, after.PressureHigh == def.PressureHigh)
	appendDefaultFloat(out, "adaptive_quota.pressure_low=0.60", before.PressureLow <= 0, after.PressureLow == def.PressureLow)
	appendDefaultDuration(out, "adaptive_quota.queue_wait_high=25ms", before.QueueWaitHigh <= 0, after.QueueWaitHigh == def.QueueWaitHigh)
	appendDefaultDuration(out, "adaptive_quota.run_time_high=250ms", before.RunTimeHigh <= 0, after.RunTimeHigh == def.RunTimeHigh)
	appendDefaultInt(out, "adaptive_quota.increase_step=1", before.IncreaseStep <= 0, after.IncreaseStep == def.IncreaseStep)
	appendDefaultInt(out, "adaptive_quota.decrease_step=1", before.DecreaseStep <= 0, after.DecreaseStep == def.DecreaseStep)
	appendDefaultInt(out, "adaptive_quota.max_adjustments_per_tick=1", before.MaxAdjustmentsPerTick <= 0, after.MaxAdjustmentsPerTick == def.MaxAdjustmentsPerTick)
	if !before.EnableIncrease && !before.EnableDecrease && after.EnableIncrease && after.EnableDecrease {
		*out = append(*out, "adaptive_quota.enable_increase_decrease=true")
	}
}

func collectBackendResourceAppliedDefaults(out *[]string, before, after BackendResourceConfig) {
	if !after.Enabled {
		return
	}
	if before.Resources == nil && after.Resources != nil {
		*out = append(*out, "backend_resources.resources=empty_map")
	}
	if backendAdmissionDefaulted(before.Resources, after.Resources) {
		*out = append(*out, "backend_resources.admission=reject")
	}
}

func backendAdmissionDefaulted(before, after map[BackendResourceName]BackendResourcePolicy) bool {
	for res, pol := range after {
		bpol, ok := before[res]
		if !ok {
			for _, lp := range pol.Lanes {
				if lp.Admission == BackendAdmissionReject {
					return true
				}
			}
			continue
		}
		for lane, lp := range pol.Lanes {
			blp, ok := bpol.Lanes[lane]
			if ok && blp.Admission != "" {
				continue
			}
			if lp.Admission == BackendAdmissionReject {
				return true
			}
		}
	}
	return false
}

func appendDefaultInt(out *[]string, token string, unset, applied bool) {
	if unset && applied {
		*out = append(*out, token)
	}
}

func appendDefaultFloat(out *[]string, token string, unset, applied bool) {
	if unset && applied {
		*out = append(*out, token)
	}
}

func appendDefaultDuration(out *[]string, token string, unset, applied bool) {
	if unset && applied {
		*out = append(*out, token)
	}
}

func appendDefaultString(out *[]string, token string, unset, applied bool) {
	if unset && applied {
		*out = append(*out, token)
	}
}

// NormalizedConfig is a redacted snapshot of configuration after normalization.
// When Valid is false, the runtime cannot be constructed (see Issues); subsystem fields show
// normalized values for support diagnostics only.
type NormalizedConfig struct {
	Version          ConfigVersion
	Valid            bool
	Runtime          RuntimeConfigSnapshot
	Lanes            LaneConfigSnapshot
	AdaptiveQuota    AdaptiveQuotaConfigSnapshot
	HotKey           HotKeyConfigSnapshot
	PerKeyAdmission  PerKeyAdmissionConfigSnapshot
	ShardPressure    ShardPressureConfigSnapshot
	Autoscaling      AutoscalingSignalConfigSnapshot
	FailurePolicy    FailurePolicyConfigSnapshot
	Retry            RetryConfigSnapshot
	Idempotency      IdempotencyConfigSnapshot
	RetrySuppression RetrySuppressionConfigSnapshot
	Pipeline         PipelineConfigSnapshot
	BackendResources BackendResourceConfigSnapshot
	Observability    ObservabilityConfigSnapshot
	Issues           []ValidationIssue
	Warnings         []ValidationIssue
	AppliedDefaults  []string
}

// RuntimeConfigSnapshot captures core scheduler settings.
type RuntimeConfigSnapshot struct {
	ShardCount       int
	WorkerCount      int
	QueueSizePerLane int
	OverloadEnabled  bool
	AdmissionEnabled bool
}

// LaneConfigSnapshot lists lane quotas in sorted order.
type LaneConfigSnapshot struct {
	Quotas []LaneQuotaEntry
}

// LaneQuotaEntry is one lane quota in a normalized snapshot.
type LaneQuotaEntry struct {
	Lane  string
	Quota int
}

// RetryConfigSnapshot captures normalized retry settings.
type RetryConfigSnapshot struct {
	Enabled            bool
	MaxAttempts        int
	InitialBackoff     time.Duration
	MaxBackoff         time.Duration
	Multiplier         float64
	Jitter             bool
	JitterFraction     float64
	MinRemainingBudget time.Duration
	RetryableKinds     []string
}

// PipelineConfigSnapshot captures continuation pipeline settings.
type PipelineConfigSnapshot struct {
	ContinuationEnabled bool
	MaxPending          int
	MaxPendingPerShard  int
	CompletionRetention time.Duration
}

// BackendResourceConfigSnapshot summarizes backend coordination (no provider impls).
type BackendResourceConfigSnapshot struct {
	Enabled               bool
	ResourceCount         int
	BackendLaneCount      int
	PressureProviderCount int
	Lanes                 []BackendResourceLaneSnapshot
}

// ObservabilityConfigSnapshot captures resolved observability flags (no hooks).
type ObservabilityConfigSnapshot struct {
	EnableStats                   bool
	EnableCounters                bool
	EnableQueueWaitTiming         bool
	EnableRunTiming               bool
	EnableHooks                   bool
	EnableAdaptiveDecisionTracing bool
	EnableDebugSnapshot           bool
	LowAllocationMode             bool
	ExposeRawRequestIdentifiers   bool
	TrackQueueWait                bool
	SlowJobThreshold              time.Duration
	HooksConfigured               bool
}

// LaneAdaptivePolicySnapshot captures one per-lane adaptive quota policy.
type LaneAdaptivePolicySnapshot struct {
	Lane            string
	Class           LaneClass
	Enabled         bool
	MinQuota        int
	MaxQuota        int
	AllowIncrease   bool
	AllowDecrease   bool
	TargetQueueWait time.Duration
	TargetRunTime   time.Duration
}

// AdaptiveQuotaConfigSnapshot captures normalized adaptive quota controller and lane policies.
type AdaptiveQuotaConfigSnapshot struct {
	Enabled               bool
	EvaluationInterval    time.Duration
	WarmupDuration        time.Duration
	CooldownDuration      time.Duration
	PressureHigh          float64
	PressureLow           float64
	QueueWaitHigh         time.Duration
	RunTimeHigh           time.Duration
	IncreaseStep          int
	DecreaseStep          int
	MaxAdjustmentsPerTick int
	EnableIncrease        bool
	EnableDecrease        bool
	Lanes                 []LaneAdaptivePolicySnapshot
}

// HotKeyConfigSnapshot captures hot key settings without raw key material.
type HotKeyConfigSnapshot struct {
	Enabled                  bool
	ExposeRawKey             bool
	MaxTrackedKeysPerShard   int
	DetectionWindow          time.Duration
	HotKeyDepthRatio         float64
	HotKeyWaitRatio          float64
	MaxCandidatesPerSnapshot int
}

// PerKeyAdmissionConfigSnapshot captures per-key admission settings.
type PerKeyAdmissionConfigSnapshot struct {
	Enabled                bool
	MinStatus              HotKeyStatus
	DefaultAction          PerKeyMitigationAction
	MaxQueuedPerKey        int
	MaxInflightPerKey      int
	PressureRatioThreshold float64
	RejectRatioThreshold   float64
	Cooldown               time.Duration
	RecoveryWindow         time.Duration
	MaxSnapshotsPerShard   int
	MaxSnapshotsTotal      int
}

// ShardPressureConfigSnapshot captures shard pressure diagnostic settings.
type ShardPressureConfigSnapshot struct {
	Enabled                     bool
	Window                      time.Duration
	HotShardPressureRatio       float64
	DominantLaneRatio           float64
	LocalizedHotKeyRatio        float64
	DistributedShardRatio       float64
	WorkerBusyRatio             float64
	MaxHotShards                int
	MaxLaneBreakdownPerShard    int
	MaxHotKeyCandidatesPerShard int
}

// AutoscalingSignalConfigSnapshot captures autoscaling signal settings.
type AutoscalingSignalConfigSnapshot struct {
	Enabled                       bool
	Window                        time.Duration
	ConsecutiveWindows            int
	QueueDepthRatioThreshold      float64
	QueueWaitMaxThreshold         time.Duration
	AdmissionRejectRateThreshold  float64
	AdmissionShedRateThreshold    float64
	WorkerBusyRatioThreshold      float64
	HotShardRatioThreshold        float64
	ManyHotShardsThreshold        int
	LocalizedHotKeyRatioThreshold float64
}

// FailurePolicyConfigSnapshot captures failure policy (no classifier impl).
type FailurePolicyConfigSnapshot struct {
	ClassifierConfigured bool
}

// IdempotencyConfigSnapshot captures idempotency policy (no hook impl).
type IdempotencyConfigSnapshot struct {
	RequireForRetry bool
	HookConfigured  bool
}

// RetrySuppressionConfigSnapshot captures retry suppression settings (no hook impl).
type RetrySuppressionConfigSnapshot struct {
	Enabled                          bool
	SuppressWhenOverloaded           bool
	SuppressNonCriticalWhenPressured bool
	SuppressLaneAboveRatio           float64
	SuppressShardAboveRatio          float64
	SuppressOverloadFailures         bool
	SuppressAdmissionFailures        bool
	SuppressPerKeyAdmissionFailures  bool
	SuppressHotKeyRetry              bool
	AllowCriticalHotKeyRetry         bool
	SuppressWhenScaleOutRecommended  bool
	HookConfigured                   bool
}

// BackendResourceLaneSnapshot is one backend lane limit in a normalized snapshot.
type BackendResourceLaneSnapshot struct {
	Resource    string
	Lane        string
	MaxInFlight int
	QueueLimit  int
	Admission   BackendAdmissionMode
}

// NormalizeConfig returns a deterministic, log-safe snapshot of cfg after normalization.
// Valid is false when ValidateConfig reports fatal errors; Issues includes errors and warnings.
// Sensitive values are not included.
func NormalizeConfig(cfg Config) NormalizedConfig {
	before := cfg
	report := ValidateConfig(cfg)
	c := cfg
	normalizeConfigInPlace(&c)
	obs := ResolveObservabilityConfig(c.Observability)

	lanes := make([]LaneQuotaEntry, 0, len(c.LaneQuotas))
	for lane, quota := range c.LaneQuotas {
		lanes = append(lanes, LaneQuotaEntry{Lane: string(lane), Quota: quota})
	}
	sort.Slice(lanes, func(i, j int) bool { return lanes[i].Lane < lanes[j].Lane })

	brLanes := buildBackendResourceLaneSnapshots(c.BackendResources)

	return NormalizedConfig{
		Version: ConfigVersionV1,
		Valid:   !report.HasErrors(),
		Runtime: RuntimeConfigSnapshot{
			ShardCount:       c.ShardCount,
			WorkerCount:      c.WorkerCount,
			QueueSizePerLane: c.QueueSizePerLane,
			OverloadEnabled:  c.OverloadEnabled,
			AdmissionEnabled: c.AdmissionEnabled,
		},
		Lanes:         LaneConfigSnapshot{Quotas: lanes},
		AdaptiveQuota: snapshotAdaptiveQuota(c.AdaptiveQuota),
		HotKey: HotKeyConfigSnapshot{
			Enabled:                  c.HotKey.Enabled,
			ExposeRawKey:             c.HotKey.ExposeRawKey,
			MaxTrackedKeysPerShard:   c.HotKey.MaxTrackedKeysPerShard,
			DetectionWindow:          c.HotKey.DetectionWindow,
			HotKeyDepthRatio:         c.HotKey.HotKeyDepthRatio,
			HotKeyWaitRatio:          c.HotKey.HotKeyWaitRatio,
			MaxCandidatesPerSnapshot: c.HotKey.MaxCandidatesPerSnapshot,
		},
		PerKeyAdmission: PerKeyAdmissionConfigSnapshot{
			Enabled:                c.PerKeyAdmission.Enabled,
			MinStatus:              c.PerKeyAdmission.MinStatus,
			DefaultAction:          c.PerKeyAdmission.DefaultAction,
			MaxQueuedPerKey:        c.PerKeyAdmission.MaxQueuedPerKey,
			MaxInflightPerKey:      c.PerKeyAdmission.MaxInflightPerKey,
			PressureRatioThreshold: c.PerKeyAdmission.PressureRatioThreshold,
			RejectRatioThreshold:   c.PerKeyAdmission.RejectRatioThreshold,
			Cooldown:               c.PerKeyAdmission.Cooldown,
			RecoveryWindow:         c.PerKeyAdmission.RecoveryWindow,
			MaxSnapshotsPerShard:   c.PerKeyAdmission.MaxSnapshotsPerShard,
			MaxSnapshotsTotal:      c.PerKeyAdmission.MaxSnapshotsTotal,
		},
		ShardPressure: ShardPressureConfigSnapshot{
			Enabled:                     c.ShardPressure.Enabled,
			Window:                      c.ShardPressure.Window,
			HotShardPressureRatio:       c.ShardPressure.HotShardPressureRatio,
			DominantLaneRatio:           c.ShardPressure.DominantLaneRatio,
			LocalizedHotKeyRatio:        c.ShardPressure.LocalizedHotKeyRatio,
			DistributedShardRatio:       c.ShardPressure.DistributedShardRatio,
			WorkerBusyRatio:             c.ShardPressure.WorkerBusyRatio,
			MaxHotShards:                c.ShardPressure.MaxHotShards,
			MaxLaneBreakdownPerShard:    c.ShardPressure.MaxLaneBreakdownPerShard,
			MaxHotKeyCandidatesPerShard: c.ShardPressure.MaxHotKeyCandidatesPerShard,
		},
		Autoscaling: snapshotAutoscaling(c.AutoscalingSignal),
		FailurePolicy: FailurePolicyConfigSnapshot{
			ClassifierConfigured: c.FailurePolicy.Classifier != nil,
		},
		Retry: snapshotRetry(c.Retry),
		Idempotency: IdempotencyConfigSnapshot{
			RequireForRetry: c.Idempotency.RequireForRetry,
			HookConfigured:  c.Idempotency.Hook != nil,
		},
		RetrySuppression: RetrySuppressionConfigSnapshot{
			Enabled:                          c.RetrySuppression.Enabled,
			SuppressWhenOverloaded:           c.RetrySuppression.SuppressWhenOverloaded,
			SuppressNonCriticalWhenPressured: c.RetrySuppression.SuppressNonCriticalWhenPressured,
			SuppressLaneAboveRatio:           c.RetrySuppression.SuppressLaneAboveRatio,
			SuppressShardAboveRatio:          c.RetrySuppression.SuppressShardAboveRatio,
			SuppressOverloadFailures:         c.RetrySuppression.SuppressOverloadFailures,
			SuppressAdmissionFailures:        c.RetrySuppression.SuppressAdmissionFailures,
			SuppressPerKeyAdmissionFailures:  c.RetrySuppression.SuppressPerKeyAdmissionFailures,
			SuppressHotKeyRetry:              c.RetrySuppression.SuppressHotKeyRetry,
			AllowCriticalHotKeyRetry:         c.RetrySuppression.AllowCriticalHotKeyRetry,
			SuppressWhenScaleOutRecommended:  c.RetrySuppression.SuppressWhenScaleOutRecommended,
			HookConfigured:                   c.RetrySuppression.Hook != nil,
		},
		Pipeline: PipelineConfigSnapshot{
			ContinuationEnabled: c.Continuation.Enabled,
			MaxPending:          c.Continuation.MaxPending,
			MaxPendingPerShard:  c.Continuation.MaxPendingPerShard,
			CompletionRetention: c.Continuation.CompletionRetention,
		},
		BackendResources: BackendResourceConfigSnapshot{
			Enabled:               c.BackendResources.Enabled,
			ResourceCount:         len(c.BackendResources.Resources),
			BackendLaneCount:      len(brLanes),
			PressureProviderCount: len(c.BackendResources.PressureProviders),
			Lanes:                 brLanes,
		},
		Observability: ObservabilityConfigSnapshot{
			EnableStats:                   obs.EnableStats,
			EnableCounters:                obs.EnableCounters,
			EnableQueueWaitTiming:         obs.EnableQueueWaitTiming,
			EnableRunTiming:               obs.EnableRunTiming,
			EnableHooks:                   obs.EnableHooks,
			EnableAdaptiveDecisionTracing: obs.EnableAdaptiveDecisionTracing,
			EnableDebugSnapshot:           obs.EnableDebugSnapshot,
			LowAllocationMode:             obs.LowAllocationMode,
			ExposeRawRequestIdentifiers:   obs.ExposeRawRequestIdentifiers,
			TrackQueueWait:                obs.TrackQueueWait,
			SlowJobThreshold:              c.Observability.SlowJobThreshold,
			HooksConfigured:               observabilityHooksConfigured(c.Observability),
		},
		Issues:          validationIssuesCopy(report),
		Warnings:        validationWarningsFromReport(report),
		AppliedDefaults: collectAppliedDefaults(before, c),
	}
}

func snapshotRetry(cfg RetryPolicy) RetryConfigSnapshot {
	kinds := make([]string, 0, len(cfg.RetryableKinds))
	for _, k := range cfg.RetryableKinds {
		kinds = append(kinds, string(k))
	}
	sort.Strings(kinds)
	return RetryConfigSnapshot{
		Enabled:            cfg.Enabled,
		MaxAttempts:        cfg.MaxAttempts,
		InitialBackoff:     cfg.InitialBackoff,
		MaxBackoff:         cfg.MaxBackoff,
		Multiplier:         cfg.Multiplier,
		Jitter:             cfg.Jitter,
		JitterFraction:     cfg.JitterFraction,
		MinRemainingBudget: cfg.MinRemainingBudget,
		RetryableKinds:     kinds,
	}
}

func snapshotAdaptiveQuota(p AdaptiveQuotaPolicy) AdaptiveQuotaConfigSnapshot {
	cfg := p.Config
	lanes := make([]LaneAdaptivePolicySnapshot, 0, len(p.Lanes))
	for _, lp := range p.Lanes {
		lanes = append(lanes, LaneAdaptivePolicySnapshot{
			Lane:            string(lp.Lane),
			Class:           lp.Class,
			Enabled:         lp.Enabled,
			MinQuota:        lp.MinQuota,
			MaxQuota:        lp.MaxQuota,
			AllowIncrease:   lp.AllowIncrease,
			AllowDecrease:   lp.AllowDecrease,
			TargetQueueWait: lp.TargetQueueWait,
			TargetRunTime:   lp.TargetRunTime,
		})
	}
	sort.Slice(lanes, func(i, j int) bool { return lanes[i].Lane < lanes[j].Lane })
	return AdaptiveQuotaConfigSnapshot{
		Enabled:               cfg.Enabled,
		EvaluationInterval:    cfg.EvaluationInterval,
		WarmupDuration:        cfg.WarmupDuration,
		CooldownDuration:      cfg.CooldownDuration,
		PressureHigh:          cfg.PressureHigh,
		PressureLow:           cfg.PressureLow,
		QueueWaitHigh:         cfg.QueueWaitHigh,
		RunTimeHigh:           cfg.RunTimeHigh,
		IncreaseStep:          cfg.IncreaseStep,
		DecreaseStep:          cfg.DecreaseStep,
		MaxAdjustmentsPerTick: cfg.MaxAdjustmentsPerTick,
		EnableIncrease:        cfg.EnableIncrease,
		EnableDecrease:        cfg.EnableDecrease,
		Lanes:                 lanes,
	}
}

func snapshotAutoscaling(cfg AutoscalingSignalConfig) AutoscalingSignalConfigSnapshot {
	return AutoscalingSignalConfigSnapshot{
		Enabled:                       cfg.Enabled,
		Window:                        cfg.Window,
		ConsecutiveWindows:            cfg.ConsecutiveWindows,
		QueueDepthRatioThreshold:      cfg.QueueDepthRatioThreshold,
		QueueWaitMaxThreshold:         cfg.QueueWaitMaxThreshold,
		AdmissionRejectRateThreshold:  cfg.AdmissionRejectRateThreshold,
		AdmissionShedRateThreshold:    cfg.AdmissionShedRateThreshold,
		WorkerBusyRatioThreshold:      cfg.WorkerBusyRatioThreshold,
		HotShardRatioThreshold:        cfg.HotShardRatioThreshold,
		ManyHotShardsThreshold:        cfg.ManyHotShardsThreshold,
		LocalizedHotKeyRatioThreshold: cfg.LocalizedHotKeyRatioThreshold,
	}
}

func buildBackendResourceLaneSnapshots(cfg BackendResourceConfig) []BackendResourceLaneSnapshot {
	var out []BackendResourceLaneSnapshot
	for res, pol := range cfg.Resources {
		for lane, lp := range pol.Lanes {
			out = append(out, BackendResourceLaneSnapshot{
				Resource:    string(res),
				Lane:        string(lane),
				MaxInFlight: lp.MaxInFlight,
				QueueLimit:  lp.QueueLimit,
				Admission:   lp.Admission,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Resource != out[j].Resource {
			return out[i].Resource < out[j].Resource
		}
		return out[i].Lane < out[j].Lane
	})
	return out
}

func observabilityHooksConfigured(c ObservabilityConfig) bool {
	h := c.Hooks
	if h.OnJobTiming != nil || h.OnSlowJob != nil || h.OnAdaptiveQuotaDecision != nil ||
		h.OnQuotaChange != nil || h.OnOverloadPolicyDecision != nil || h.OnPerKeyAdmissionDecision != nil {
		return true
	}
	r := h.Request
	if r.OnQueued != nil || r.OnStarted != nil || r.OnCompleted != nil || r.OnRejected != nil ||
		r.OnFailure != nil || r.OnStageStarted != nil || r.OnStageCompleted != nil || r.OnStageFailed != nil {
		return true
	}
	ch := r.Continuation
	return ch.OnContinuationYielded != nil || ch.OnContinuationResumed != nil ||
		ch.OnContinuationCompleted != nil || ch.OnContinuationFailed != nil ||
		ch.OnContinuationCancelled != nil || ch.OnContinuationLate != nil
}
