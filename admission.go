// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"errors"
	"fmt"

	"github.com/haluan/go-keylane/internal/core"
)

// AdmissionConfig configures pressure-based admission control.
// Zero value disables admission. When enabled, per-lane thresholds come from the
// queue's admission policy snapshot (see UpdateAdmissionPolicy).
//
// Admission is process-local and best-effort; it uses the current Pressure() snapshot.
type AdmissionConfig struct {
	Enabled          bool
	RejectAboveRatio float64
}

// ErrAdmissionRejected indicates the request was rejected by admission control
// before enqueue due to runtime pressure or per-lane queue depth.
var ErrAdmissionRejected = errors.New("keylane: request rejected by admission control")

// AdmissionRejectedError carries lane, class, and reason details for an admission rejection.
type AdmissionRejectedError struct {
	Lane      Lane
	Class     LaneClass
	Reason    string
	Pressure  float64
	Threshold float64
	Depth     uint32
	MaxDepth  uint32
}

func (e AdmissionRejectedError) Error() string {
	switch e.Reason {
	case AdmissionReasonLaneQueueDepthExceeded:
		return fmt.Sprintf("keylane: request rejected by admission control (lane %s depth %d >= max %d)",
			e.Lane, e.Depth, e.MaxDepth)
	case AdmissionReasonPressureAboveThreshold:
		return fmt.Sprintf("keylane: request rejected by admission control (lane %s pressure %.2f >= threshold %.2f)",
			e.Lane, e.Pressure, e.Threshold)
	default:
		return fmt.Sprintf("keylane: request rejected by admission control (lane %s)", e.Lane)
	}
}

func (e AdmissionRejectedError) Unwrap() error {
	return ErrAdmissionRejected
}

// NormalizeAdmissionConfig applies defaults when admission is enabled.
func NormalizeAdmissionConfig(cfg *AdmissionConfig) {
	if cfg == nil || !cfg.Enabled {
		return
	}
	if cfg.RejectAboveRatio == 0 {
		cfg.RejectAboveRatio = 0.90
	}
}

// ValidateAdmissionConfig validates admission settings after applying defaults
// via NormalizeAdmissionConfig. A zero RejectAboveRatio defaults to 0.90 when enabled.
func ValidateAdmissionConfig(cfg AdmissionConfig) error {
	if !cfg.Enabled {
		return nil
	}
	normalized := cfg
	NormalizeAdmissionConfig(&normalized)
	if normalized.RejectAboveRatio <= 0 {
		return fmt.Errorf("%w: RejectAboveRatio must be > 0 when admission is enabled", ErrInvalidConfig)
	}
	return nil
}

// CheckAdmission rejects the request when enabled and the per-lane admission policy
// rejects based on lane queue depth or global pressure. It records lane admission-rejected
// counters when observability counters are enabled.
func CheckAdmission(q *Queue, cfg AdmissionConfig, meta RequestMeta) error {
	if q == nil {
		return ErrNilQueue
	}
	if !cfg.Enabled {
		return nil
	}
	if err := meta.Lane.Validate(); err != nil {
		return err
	}

	normalized := cfg
	NormalizeAdmissionConfig(&normalized)
	if err := validateNormalizedAdmission(normalized); err != nil {
		return err
	}

	laneID, ok := q.reg.Lookup(string(meta.Lane))
	if !ok {
		return ErrInvalidLane
	}

	pressure := q.Pressure().TotalDepthRatio
	depth := q.sched.LaneQueueDepth(laneID)
	result := q.sched.EvaluateAdmissionForLane(laneID, pressure, depth)
	if result.Admit {
		return nil
	}

	q.sched.RecordPressureAdmissionRejected(laneID)
	if q.config.HotKey.Enabled && meta.Key != "" {
		q.sched.RecordHotKeyReject(core.HashKey(meta.Key), q.ShardIDForKey(meta.Key))
	}

	return AdmissionRejectedError{
		Lane:      meta.Lane,
		Class:     LaneClass(result.Class),
		Reason:    result.Reason,
		Pressure:  pressure,
		Threshold: result.Threshold,
		Depth:     depth,
		MaxDepth:  result.MaxDepth,
	}
}

func validateNormalizedAdmission(cfg AdmissionConfig) error {
	if cfg.RejectAboveRatio <= 0 {
		return fmt.Errorf("%w: RejectAboveRatio must be > 0 when admission is enabled", ErrInvalidConfig)
	}
	return nil
}
