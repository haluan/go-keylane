// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"errors"
	"testing"
)

func TestBackendResourceConfigDisabledValid(t *testing.T) {
	if err := ValidateBackendResourceConfig(BackendResourceConfig{}); err != nil {
		t.Fatal(err)
	}
}

func TestBackendResourceConfigEnabledRequiresResource(t *testing.T) {
	err := ValidateBackendResourceConfig(BackendResourceConfig{Enabled: true})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("err = %v", err)
	}
}

func TestBackendResourceConfigRejectsWaitMode(t *testing.T) {
	err := ValidateBackendResourceConfig(BackendResourceConfig{
		Enabled: true,
		Resources: map[BackendResourceName]BackendResourcePolicy{
			"db": {
				Lanes: map[BackendLane]BackendLanePolicy{
					BackendLaneDBRead: {MaxInFlight: 1, Admission: BackendAdmissionWait},
				},
			},
		},
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("err = %v", err)
	}
}

func TestBackendResourceConfigNormalizeDefaultsAdmission(t *testing.T) {
	cfg := BackendResourceConfig{
		Enabled: true,
		Resources: map[BackendResourceName]BackendResourcePolicy{
			"db": {
				Lanes: map[BackendLane]BackendLanePolicy{
					BackendLaneDBRead: {MaxInFlight: 1},
				},
			},
		},
	}
	NormalizeBackendResourceConfig(&cfg)
	if cfg.Resources["db"].Lanes[BackendLaneDBRead].Admission != BackendAdmissionReject {
		t.Fatalf("admission = %q", cfg.Resources["db"].Lanes[BackendLaneDBRead].Admission)
	}
}
