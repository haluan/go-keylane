// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "fmt"

// BackendResourceConfig configures optional backend resource lane coordination (disabled by default).
type BackendResourceConfig struct {
	Enabled   bool
	Resources map[BackendResourceName]BackendResourcePolicy
}

// BackendResourcePolicy holds per-lane policies for one backend resource.
type BackendResourcePolicy struct {
	Lanes map[BackendLane]BackendLanePolicy
}

// BackendLanePolicy bounds in-flight work for one backend lane.
type BackendLanePolicy struct {
	MaxInFlight int
	QueueLimit  int
	Admission   BackendAdmissionMode
}

// BackendAdmissionMode selects how saturated backend lanes admit work.
type BackendAdmissionMode string

const (
	BackendAdmissionReject BackendAdmissionMode = "reject"
	BackendAdmissionWait   BackendAdmissionMode = "wait"
)

// NormalizeBackendResourceConfig applies safe defaults when coordination is enabled.
func NormalizeBackendResourceConfig(cfg *BackendResourceConfig) {
	if cfg == nil || !cfg.Enabled {
		return
	}
	if cfg.Resources == nil {
		cfg.Resources = make(map[BackendResourceName]BackendResourcePolicy)
	}
	for res, pol := range cfg.Resources {
		if pol.Lanes == nil {
			pol.Lanes = make(map[BackendLane]BackendLanePolicy)
			cfg.Resources[res] = pol
		}
		for lane, lp := range pol.Lanes {
			if lp.Admission == "" {
				lp.Admission = BackendAdmissionReject
				pol.Lanes[lane] = lp
			}
			cfg.Resources[res] = pol
		}
	}
}

// ValidateBackendResourceConfig validates backend resource settings after normalization.
func ValidateBackendResourceConfig(cfg BackendResourceConfig) error {
	if !cfg.Enabled {
		return nil
	}
	normalized := cfg
	NormalizeBackendResourceConfig(&normalized)
	if len(normalized.Resources) == 0 {
		return fmt.Errorf("%w: BackendResources.Enabled requires at least one resource", ErrInvalidConfig)
	}
	for res, pol := range normalized.Resources {
		if res == "" {
			return fmt.Errorf("%w: backend resource name cannot be empty", ErrInvalidConfig)
		}
		if len(pol.Lanes) == 0 {
			return fmt.Errorf("%w: resource %q must define at least one backend lane", ErrInvalidConfig, res)
		}
		for lane, lp := range pol.Lanes {
			if lane == "" {
				return fmt.Errorf("%w: backend lane cannot be empty for resource %q", ErrInvalidConfig, res)
			}
			if lp.MaxInFlight < 1 {
				return fmt.Errorf("%w: resource %q lane %q MaxInFlight must be at least 1", ErrInvalidConfig, res, lane)
			}
			if lp.QueueLimit < 0 {
				return fmt.Errorf("%w: resource %q lane %q QueueLimit cannot be negative", ErrInvalidConfig, res, lane)
			}
			switch lp.Admission {
			case BackendAdmissionReject:
			case BackendAdmissionWait:
				return fmt.Errorf("%w: BackendAdmissionWait is not supported yet (resource %q lane %q)", ErrInvalidConfig, res, lane)
			default:
				return fmt.Errorf("%w: unknown backend admission mode %q for resource %q lane %q", ErrInvalidConfig, lp.Admission, res, lane)
			}
		}
	}
	return nil
}
