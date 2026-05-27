// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"fmt"
	"math"
	"time"
)

// BackendPressureProvider reports downstream pool pressure for one resource/lane pair.
//
// Experimental: may change before v1.0. Observational only; keylane does not reject requests from pool telemetry unless the application gates on snapshots.
type BackendPressureProvider interface {
	BackendPressure(context.Context) BackendPressureSnapshot
}

// BackendPressureSnapshot is a low-cardinality view of external pool pressure.
type BackendPressureSnapshot struct {
	Resource BackendResourceName
	Lane     BackendLane

	InUse    int
	Capacity int
	Idle     int

	WaitCount uint64
	WaitTime  time.Duration

	Saturated bool
	Pressure  float64
}

// BackendPressureDiagnostic is the stable debug/diagnostic view of pool pressure.
type BackendPressureDiagnostic struct {
	Resource BackendResourceName
	Lane     BackendLane

	InUse    int
	Capacity int
	Idle     int

	WaitCount uint64
	WaitTime  time.Duration

	Saturated bool
	Pressure  float64
}

func backendPressureDiagnosticFromSnapshot(s BackendPressureSnapshot) BackendPressureDiagnostic {
	return BackendPressureDiagnostic{
		Resource:  s.Resource,
		Lane:      s.Lane,
		InUse:     s.InUse,
		Capacity:  s.Capacity,
		Idle:      s.Idle,
		WaitCount: s.WaitCount,
		WaitTime:  s.WaitTime,
		Saturated: s.Saturated,
		Pressure:  s.Pressure,
	}
}

func normalizeBackendPressureSnapshot(s BackendPressureSnapshot) BackendPressureSnapshot {
	if s.InUse < 0 {
		s.InUse = 0
	}
	if s.Capacity < 0 {
		s.Capacity = 0
	}
	if s.Idle < 0 {
		s.Idle = 0
	}
	if s.WaitTime < 0 {
		s.WaitTime = 0
	}
	if s.Capacity > 0 {
		if s.Pressure <= 0 {
			s.Pressure = float64(s.InUse) / float64(s.Capacity)
		}
		if !s.Saturated && s.InUse >= s.Capacity {
			s.Saturated = true
		}
	} else {
		s.Pressure = 0
	}
	if s.Pressure < 0 {
		s.Pressure = 0
	}
	if s.Pressure > 1 {
		s.Pressure = 1
	}
	if math.IsNaN(s.Pressure) || math.IsInf(s.Pressure, 0) {
		s.Pressure = 0
	}
	return s
}

// ValidateBackendPressureSnapshot checks identity and label cardinality rules.
func ValidateBackendPressureSnapshot(s BackendPressureSnapshot) error {
	if s.Resource == "" {
		return fmt.Errorf("%w: backend pressure resource is required", ErrInvalidConfig)
	}
	if s.Lane == "" {
		return fmt.Errorf("%w: backend pressure lane is required", ErrInvalidConfig)
	}
	if err := validateBackendLabel(string(s.Resource), "resource"); err != nil {
		return err
	}
	if err := validateBackendLabel(string(s.Lane), "lane"); err != nil {
		return err
	}
	return nil
}

func validateBackendPressureProviderProbe(index int, p BackendPressureProvider) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: pressure provider %d panicked during probe: %v", ErrInvalidConfig, index, r)
		}
	}()
	snap := normalizeBackendPressureSnapshot(p.BackendPressure(context.Background()))
	if err = ValidateBackendPressureSnapshot(snap); err != nil {
		return fmt.Errorf("%w: pressure provider %d: %v", ErrInvalidConfig, index, err)
	}
	return nil
}

func copyBackendPressureProviders(providers []BackendPressureProvider) []BackendPressureProvider {
	if len(providers) == 0 {
		return nil
	}
	out := make([]BackendPressureProvider, len(providers))
	copy(out, providers)
	return out
}
