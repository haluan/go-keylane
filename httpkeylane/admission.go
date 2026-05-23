// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"fmt"
	"net/http"

	"github.com/haluan/go-keylane"
)

// AdmissionConfig configures pressure-based HTTP admission control.
// Zero value disables admission. When enabled, requests are rejected before enqueue
// when queue pressure meets or exceeds RejectAboveRatio.
type AdmissionConfig struct {
	Enabled          bool
	RejectAboveRatio float64
	RejectStatusCode int
}

// NormalizeAdmissionConfig applies defaults when admission is enabled.
func NormalizeAdmissionConfig(cfg *AdmissionConfig) {
	if cfg == nil || !cfg.Enabled {
		return
	}
	if cfg.RejectAboveRatio == 0 {
		cfg.RejectAboveRatio = 0.90
	}
	if cfg.RejectStatusCode == 0 {
		cfg.RejectStatusCode = http.StatusServiceUnavailable
	}
}

// ValidateAdmissionConfig validates HTTP admission settings after applying defaults
// via NormalizeAdmissionConfig.
func ValidateAdmissionConfig(cfg AdmissionConfig) error {
	if !cfg.Enabled {
		return nil
	}
	normalized := cfg
	NormalizeAdmissionConfig(&normalized)
	if normalized.RejectAboveRatio <= 0 {
		return fmt.Errorf("%w: RejectAboveRatio must be > 0 when admission is enabled", keylane.ErrInvalidConfig)
	}
	if normalized.RejectStatusCode != 0 && (normalized.RejectStatusCode < 100 || normalized.RejectStatusCode > 599) {
		return fmt.Errorf("%w: RejectStatusCode must be a valid HTTP status", keylane.ErrInvalidConfig)
	}
	return nil
}

// CoreConfig returns the transport-agnostic admission config for keylane.CheckAdmission.
func (c AdmissionConfig) CoreConfig() keylane.AdmissionConfig {
	return keylane.AdmissionConfig{
		Enabled:          c.Enabled,
		RejectAboveRatio: c.RejectAboveRatio,
	}
}

// rejectStatusCode returns the HTTP status for admission rejection.
func rejectStatusCode(cfg AdmissionConfig) int {
	if cfg.RejectStatusCode != 0 {
		return cfg.RejectStatusCode
	}
	return http.StatusServiceUnavailable
}
