// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"errors"
	"fmt"
)

// AdmissionConfig configures pressure-based admission control.
// Zero value disables admission. When enabled, requests are rejected before enqueue
// when queue depth pressure meets or exceeds RejectAboveRatio.
//
// Admission is process-local and best-effort; it uses the current Pressure() snapshot.
type AdmissionConfig struct {
	Enabled          bool
	RejectAboveRatio float64
}

// ErrAdmissionRejected indicates the request was rejected by admission control
// before enqueue due to runtime pressure.
var ErrAdmissionRejected = errors.New("keylane: request rejected by admission control")

// AdmissionRejectedError carries pressure and threshold details for an admission rejection.
type AdmissionRejectedError struct {
	Pressure  float64
	Threshold float64
}

func (e AdmissionRejectedError) Error() string {
	return fmt.Sprintf("keylane: request rejected by admission control (pressure %.2f >= threshold %.2f)",
		e.Pressure, e.Threshold)
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

// CheckAdmission rejects the request when enabled and current queue pressure
// is at or above the configured threshold. It records lane admission-rejected
// counters when observability counters are enabled. Invalid lanes return ErrInvalidLane.
// Invalid admission config returns ErrInvalidConfig before any pressure check.
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

	pressure := q.Pressure().TotalDepthRatio
	if pressure < normalized.RejectAboveRatio {
		return nil
	}

	laneID, ok := q.reg.Lookup(string(meta.Lane))
	if !ok {
		return ErrInvalidLane
	}
	q.sched.RecordPressureAdmissionRejected(laneID)

	return AdmissionRejectedError{
		Pressure:  pressure,
		Threshold: normalized.RejectAboveRatio,
	}
}

func validateNormalizedAdmission(cfg AdmissionConfig) error {
	if cfg.RejectAboveRatio <= 0 {
		return fmt.Errorf("%w: RejectAboveRatio must be > 0 when admission is enabled", ErrInvalidConfig)
	}
	return nil
}
