// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"testing"
	"time"
)

func TestNormalizeBackendPressureZeroCapacity(t *testing.T) {
	s := normalizeBackendPressureSnapshot(BackendPressureSnapshot{
		Resource: "primary-db",
		Lane:     BackendLaneDBRead,
		InUse:    5,
		Capacity: 0,
		Pressure: 0.9,
	})
	if s.Pressure != 0 {
		t.Fatalf("Pressure = %v", s.Pressure)
	}
	if s.Saturated {
		t.Fatal("Saturated = true")
	}
}

func TestNormalizeBackendPressureClampsRatio(t *testing.T) {
	s := normalizeBackendPressureSnapshot(BackendPressureSnapshot{
		Resource: "primary-db",
		Lane:     BackendLaneDBRead,
		InUse:    12,
		Capacity: 10,
		Pressure: 2,
	})
	if s.Pressure != 1 {
		t.Fatalf("Pressure = %v", s.Pressure)
	}
	if !s.Saturated {
		t.Fatal("Saturated = false")
	}
}

func TestNormalizeBackendPressureDerivesSaturation(t *testing.T) {
	s := normalizeBackendPressureSnapshot(BackendPressureSnapshot{
		Resource: "primary-db",
		Lane:     BackendLaneDBRead,
		InUse:    4,
		Capacity: 4,
	})
	if !s.Saturated {
		t.Fatal("Saturated = false")
	}
	if s.Pressure != 1 {
		t.Fatalf("Pressure = %v", s.Pressure)
	}
}

func TestNormalizeBackendPressureNegativeWaitTime(t *testing.T) {
	s := normalizeBackendPressureSnapshot(BackendPressureSnapshot{
		Resource: "primary-db",
		Lane:     BackendLaneDBRead,
		WaitTime: -time.Second,
	})
	if s.WaitTime != 0 {
		t.Fatalf("WaitTime = %v", s.WaitTime)
	}
}

func TestValidateBackendPressureSnapshotRejectsEmptyResource(t *testing.T) {
	err := ValidateBackendPressureSnapshot(BackendPressureSnapshot{Lane: BackendLaneDBRead})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestBackendResourceConfigRejectsNilPressureProvider(t *testing.T) {
	err := ValidateBackendResourceConfig(BackendResourceConfig{
		PressureProviders: []BackendPressureProvider{nil},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

type staticPressureProvider struct {
	snap BackendPressureSnapshot
}

func (p staticPressureProvider) BackendPressure(context.Context) BackendPressureSnapshot {
	return p.snap
}

func TestQueueBackendPressureCollection(t *testing.T) {
	ctx := testTimeout(t)
	prov := staticPressureProvider{snap: BackendPressureSnapshot{
		Resource:  "primary-db",
		Lane:      BackendLaneDBRead,
		InUse:     3,
		Capacity:  10,
		Saturated: false,
		Pressure:  0.3,
	}}
	cfg := newTestConfig()
	cfg.BackendResources.PressureProviders = []BackendPressureProvider{prov}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	pressure := q.BackendPressure(ctx)
	if len(pressure) != 1 {
		t.Fatalf("len = %d", len(pressure))
	}
	if pressure[0].InUse != 3 || pressure[0].Capacity != 10 {
		t.Fatalf("pressure = %+v", pressure[0])
	}
}
