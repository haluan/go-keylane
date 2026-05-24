// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"errors"
	"fmt"
	"time"

	"github.com/haluan/go-keylane/internal/core"
)

// ScaleReason explains why a scale signal was emitted (KL-1504).
type ScaleReason = core.ScaleReason

const (
	ScaleReasonNone                = core.ScaleReasonNone
	ScaleReasonQueueDepthHigh      = core.ScaleReasonQueueDepthHigh
	ScaleReasonQueueWaitHigh       = core.ScaleReasonQueueWaitHigh
	ScaleReasonAdmissionRejectHigh = core.ScaleReasonAdmissionRejectHigh
	ScaleReasonAdmissionShedHigh   = core.ScaleReasonAdmissionShedHigh
	ScaleReasonWorkerSaturated     = core.ScaleReasonWorkerSaturated
	ScaleReasonManyHotShards       = core.ScaleReasonManyHotShards
	ScaleReasonDistributedPressure = core.ScaleReasonDistributedPressure
	ScaleReasonLocalizedHotKey     = core.ScaleReasonLocalizedHotKey
	ScaleReasonInsufficientData    = core.ScaleReasonInsufficientData
)

// ScaleScope identifies where pressure is concentrated (KL-1504).
type ScaleScope = core.ScaleScope

const (
	ScaleScopeNone    = core.ScaleScopeNone
	ScaleScopeGlobal  = core.ScaleScopeGlobal
	ScaleScopeShard   = core.ScaleScopeShard
	ScaleScopeLane    = core.ScaleScopeLane
	ScaleScopeHotKey  = core.ScaleScopeHotKey
	ScaleScopeUnknown = core.ScaleScopeUnknown
)

// AutoscalingSignalConfig controls scale signal calculation (KL-1504).
type AutoscalingSignalConfig = core.AutoscalingSignalConfig

// ScaleSignal is an immutable autoscaling signal snapshot (KL-1504).
type ScaleSignal = core.ScaleSignal

// ScaleSignalSnapshot is a debug-friendly view of ScaleSignal.
type ScaleSignalSnapshot = core.ScaleSignalSnapshot

// ScaleAdmissionTotals holds cumulative admission counters for scale signal metrics.
type ScaleAdmissionTotals struct {
	Rejected  uint64
	Shed      uint64
	Throttled uint64
}

var ErrInvalidAutoscalingSignalConfig = errors.New("keylane: invalid autoscaling signal config")

// DefaultAutoscalingSignalConfig returns recommended defaults.
func DefaultAutoscalingSignalConfig() AutoscalingSignalConfig {
	return AutoscalingSignalConfig{
		Enabled:                       true,
		Window:                        30 * time.Second,
		ConsecutiveWindows:            2,
		QueueDepthRatioThreshold:      0.70,
		QueueWaitMaxThreshold:         50 * time.Millisecond,
		AdmissionRejectRateThreshold:  0.05,
		AdmissionShedRateThreshold:    0.01,
		WorkerBusyRatioThreshold:      0.85,
		HotShardRatioThreshold:        0.70,
		ManyHotShardsThreshold:        4,
		LocalizedHotKeyRatioThreshold: 0.40,
	}
}

// NormalizeAutoscalingSignalConfig fills zero fields with defaults when enabled.
func NormalizeAutoscalingSignalConfig(cfg *AutoscalingSignalConfig) {
	if cfg == nil || !cfg.Enabled {
		return
	}
	def := DefaultAutoscalingSignalConfig()
	if cfg.Window <= 0 {
		cfg.Window = def.Window
	}
	if cfg.ConsecutiveWindows <= 0 {
		cfg.ConsecutiveWindows = def.ConsecutiveWindows
	}
	if cfg.QueueDepthRatioThreshold <= 0 {
		cfg.QueueDepthRatioThreshold = def.QueueDepthRatioThreshold
	}
	if cfg.QueueWaitMaxThreshold <= 0 {
		cfg.QueueWaitMaxThreshold = def.QueueWaitMaxThreshold
	}
	if cfg.AdmissionRejectRateThreshold <= 0 {
		cfg.AdmissionRejectRateThreshold = def.AdmissionRejectRateThreshold
	}
	if cfg.AdmissionShedRateThreshold <= 0 {
		cfg.AdmissionShedRateThreshold = def.AdmissionShedRateThreshold
	}
	if cfg.WorkerBusyRatioThreshold <= 0 {
		cfg.WorkerBusyRatioThreshold = def.WorkerBusyRatioThreshold
	}
	if cfg.HotShardRatioThreshold <= 0 {
		cfg.HotShardRatioThreshold = def.HotShardRatioThreshold
	}
	if cfg.ManyHotShardsThreshold <= 0 {
		cfg.ManyHotShardsThreshold = def.ManyHotShardsThreshold
	}
	if cfg.LocalizedHotKeyRatioThreshold <= 0 {
		cfg.LocalizedHotKeyRatioThreshold = def.LocalizedHotKeyRatioThreshold
	}
}

// ValidateAutoscalingSignalConfig checks normalized config.
func ValidateAutoscalingSignalConfig(cfg AutoscalingSignalConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.Window <= 0 {
		return fmt.Errorf("%w: Window must be positive when enabled", ErrInvalidAutoscalingSignalConfig)
	}
	if cfg.ConsecutiveWindows < 1 {
		return fmt.Errorf("%w: ConsecutiveWindows must be at least 1", ErrInvalidAutoscalingSignalConfig)
	}
	if cfg.QueueDepthRatioThreshold <= 0 || cfg.QueueDepthRatioThreshold > 1 {
		return fmt.Errorf("%w: QueueDepthRatioThreshold must be in (0,1]", ErrInvalidAutoscalingSignalConfig)
	}
	if cfg.AdmissionRejectRateThreshold < 0 || cfg.AdmissionRejectRateThreshold > 1 {
		return fmt.Errorf("%w: AdmissionRejectRateThreshold must be in [0,1]", ErrInvalidAutoscalingSignalConfig)
	}
	if cfg.AdmissionShedRateThreshold < 0 || cfg.AdmissionShedRateThreshold > 1 {
		return fmt.Errorf("%w: AdmissionShedRateThreshold must be in [0,1]", ErrInvalidAutoscalingSignalConfig)
	}
	if cfg.WorkerBusyRatioThreshold <= 0 || cfg.WorkerBusyRatioThreshold > 1 {
		return fmt.Errorf("%w: WorkerBusyRatioThreshold must be in (0,1]", ErrInvalidAutoscalingSignalConfig)
	}
	if cfg.HotShardRatioThreshold <= 0 || cfg.HotShardRatioThreshold > 1 {
		return fmt.Errorf("%w: HotShardRatioThreshold must be in (0,1]", ErrInvalidAutoscalingSignalConfig)
	}
	if cfg.ManyHotShardsThreshold < 1 {
		return fmt.Errorf("%w: ManyHotShardsThreshold must be at least 1", ErrInvalidAutoscalingSignalConfig)
	}
	if cfg.LocalizedHotKeyRatioThreshold <= 0 || cfg.LocalizedHotKeyRatioThreshold > 1 {
		return fmt.Errorf("%w: LocalizedHotKeyRatioThreshold must be in (0,1]", ErrInvalidAutoscalingSignalConfig)
	}
	return nil
}
