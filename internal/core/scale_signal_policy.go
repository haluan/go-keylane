// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "time"

// ScaleReason explains why a scale signal was emitted.
type ScaleReason string

const (
	ScaleReasonNone                ScaleReason = "none"
	ScaleReasonQueueDepthHigh      ScaleReason = "queue_depth_high"
	ScaleReasonQueueWaitHigh       ScaleReason = "queue_wait_high"
	ScaleReasonAdmissionRejectHigh ScaleReason = "admission_reject_high"
	ScaleReasonAdmissionShedHigh   ScaleReason = "admission_shed_high"
	ScaleReasonWorkerSaturated     ScaleReason = "worker_saturated"
	ScaleReasonManyHotShards       ScaleReason = "many_hot_shards"
	ScaleReasonDistributedPressure ScaleReason = "distributed_pressure"
	ScaleReasonLocalizedHotKey     ScaleReason = "localized_hot_key"
	ScaleReasonInsufficientData    ScaleReason = "insufficient_data"
)

// ScaleScope identifies where pressure is concentrated.
type ScaleScope string

const (
	ScaleScopeNone    ScaleScope = "none"
	ScaleScopeGlobal  ScaleScope = "global"
	ScaleScopeShard   ScaleScope = "shard"
	ScaleScopeLane    ScaleScope = "lane"
	ScaleScopeHotKey  ScaleScope = "hot_key"
	ScaleScopeUnknown ScaleScope = "unknown"
)

// AutoscalingSignalConfig controls scale signal calculation (KL-1504).
type AutoscalingSignalConfig struct {
	Enabled bool

	Window             time.Duration
	ConsecutiveWindows int

	QueueDepthRatioThreshold float64
	QueueWaitMaxThreshold    time.Duration

	AdmissionRejectRateThreshold float64
	AdmissionShedRateThreshold   float64

	WorkerBusyRatioThreshold float64

	HotShardRatioThreshold float64
	ManyHotShardsThreshold int

	LocalizedHotKeyRatioThreshold float64
}

// ScaleSignal is an immutable autoscaling signal snapshot (KL-1504).
type ScaleSignal struct {
	DiagnosticsEnabled bool

	Recommended bool

	PressureRatio float64
	Reason        ScaleReason
	Scope         ScaleScope

	QueueDepthRatio float64
	QueueWaitMax    time.Duration

	AdmissionRejectedRate  float64
	AdmissionShedRate      float64
	AdmissionThrottledRate float64

	WorkerBusyRatio float64

	HotShardCount int
	HotShardRatio float64

	HotKeyCandidateCount int
	LocalizedHotKeyRatio float64
	LocalizedHotKey      bool

	WindowStartedAt time.Time
	WindowEndedAt   time.Time
}

// ScaleSignalSnapshot is a debug-friendly view of ScaleSignal.
type ScaleSignalSnapshot struct {
	DiagnosticsEnabled bool

	Recommended bool

	PressureRatio float64
	Reason        string
	Scope         string

	QueueDepthRatio    float64
	QueueWaitMaxMillis float64

	AdmissionRejectedRate  float64
	AdmissionShedRate      float64
	AdmissionThrottledRate float64

	WorkerBusyRatio float64

	HotShardCount        int
	HotKeyCandidateCount int
	LocalizedHotKeyRatio float64
	LocalizedHotKey      bool
}

func normalizeAutoscalingSignalConfig(cfg *AutoscalingSignalConfig) {
	if cfg == nil || !cfg.Enabled {
		return
	}
	if cfg.Window <= 0 {
		cfg.Window = 30 * time.Second
	}
	if cfg.ConsecutiveWindows <= 0 {
		cfg.ConsecutiveWindows = 2
	}
	if cfg.QueueDepthRatioThreshold <= 0 {
		cfg.QueueDepthRatioThreshold = 0.70
	}
	if cfg.QueueWaitMaxThreshold <= 0 {
		cfg.QueueWaitMaxThreshold = 50 * time.Millisecond
	}
	if cfg.AdmissionRejectRateThreshold <= 0 {
		cfg.AdmissionRejectRateThreshold = 0.05
	}
	if cfg.AdmissionShedRateThreshold <= 0 {
		cfg.AdmissionShedRateThreshold = 0.01
	}
	if cfg.WorkerBusyRatioThreshold <= 0 {
		cfg.WorkerBusyRatioThreshold = 0.85
	}
	if cfg.HotShardRatioThreshold <= 0 {
		cfg.HotShardRatioThreshold = 0.70
	}
	if cfg.ManyHotShardsThreshold <= 0 {
		cfg.ManyHotShardsThreshold = 4
	}
	if cfg.LocalizedHotKeyRatioThreshold <= 0 {
		cfg.LocalizedHotKeyRatioThreshold = 0.40
	}
}

func autoscalingEnabled(cfg AutoscalingSignalConfig) bool {
	return cfg.Enabled
}

func scaleSignalToSnapshot(sig ScaleSignal) ScaleSignalSnapshot {
	return ScaleSignalSnapshot{
		DiagnosticsEnabled:     sig.DiagnosticsEnabled,
		Recommended:            sig.Recommended,
		PressureRatio:          sig.PressureRatio,
		Reason:                 string(sig.Reason),
		Scope:                  string(sig.Scope),
		QueueDepthRatio:        sig.QueueDepthRatio,
		QueueWaitMaxMillis:     float64(sig.QueueWaitMax) / float64(time.Millisecond),
		AdmissionRejectedRate:  sig.AdmissionRejectedRate,
		AdmissionShedRate:      sig.AdmissionShedRate,
		AdmissionThrottledRate: sig.AdmissionThrottledRate,
		WorkerBusyRatio:        sig.WorkerBusyRatio,
		HotShardCount:          sig.HotShardCount,
		HotKeyCandidateCount:   sig.HotKeyCandidateCount,
		LocalizedHotKeyRatio:   sig.LocalizedHotKeyRatio,
		LocalizedHotKey:        sig.LocalizedHotKey,
	}
}
