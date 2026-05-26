// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestBackendPressureHookEnabled(t *testing.T) {
	ctx := testTimeout(t)
	var events atomic.Int32
	prov := staticPressureProvider{snap: BackendPressureSnapshot{
		Resource: "primary-db",
		Lane:     BackendLaneDBRead,
		InUse:    1,
		Capacity: 4,
	}}
	cfg := newTestConfig()
	cfg.Observability.EnableHooks = true
	cfg.BackendResources.PressureProviders = []BackendPressureProvider{prov}
	cfg.Observability.Hooks.Backend.OnBackendPressure = func(ev BackendPressureEvent) {
		if ev.Snapshot.Resource == "primary-db" {
			events.Add(1)
		}
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	_ = q.BackendPressure(ctx)
	if events.Load() != 1 {
		t.Fatalf("events = %d", events.Load())
	}
}

func TestBackendPressureHookDisabled(t *testing.T) {
	ctx := testTimeout(t)
	var events atomic.Int32
	prov := staticPressureProvider{snap: BackendPressureSnapshot{
		Resource: "primary-db",
		Lane:     BackendLaneDBRead,
	}}
	cfg := newTestConfig()
	cfg.Observability.EnableHooks = false
	cfg.BackendResources.PressureProviders = []BackendPressureProvider{prov}
	cfg.Observability.Hooks.Backend.OnBackendPressure = func(BackendPressureEvent) {
		events.Add(1)
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	_ = q.BackendPressure(ctx)
	if events.Load() != 0 {
		t.Fatalf("events = %d", events.Load())
	}
}

type panicAfterProbeProvider struct {
	calls int
	snap  BackendPressureSnapshot
}

func (p *panicAfterProbeProvider) BackendPressure(context.Context) BackendPressureSnapshot {
	p.calls++
	if p.calls > 1 {
		panic("provider panic")
	}
	return p.snap
}

func TestBackendPressureHookPanicRecovered(t *testing.T) {
	ctx := testTimeout(t)
	prov := staticPressureProvider{snap: BackendPressureSnapshot{
		Resource: "primary-db",
		Lane:     BackendLaneDBRead,
		InUse:    2,
		Capacity: 4,
	}}
	cfg := newTestConfig()
	cfg.Observability.EnableHooks = true
	cfg.BackendResources.PressureProviders = []BackendPressureProvider{prov}
	cfg.Observability.Hooks.Backend.OnBackendPressure = func(BackendPressureEvent) {
		panic("hook panic")
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	pressure := q.BackendPressure(ctx)
	if len(pressure) != 1 || pressure[0].InUse != 2 || pressure[0].Capacity != 4 {
		t.Fatalf("pressure = %+v", pressure)
	}
}

func TestBackendPressureProviderPanicRecovered(t *testing.T) {
	ctx := testTimeout(t)
	good := staticPressureProvider{snap: BackendPressureSnapshot{
		Resource: "primary-db",
		Lane:     BackendLaneDBRead,
		InUse:    2,
		Capacity: 4,
	}}
	cfg := newTestConfig()
	cfg.BackendResources.PressureProviders = []BackendPressureProvider{
		&panicAfterProbeProvider{snap: BackendPressureSnapshot{Resource: "wallet-api", Lane: BackendLaneExternalAPI}},
		good,
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	pressure := q.BackendPressure(ctx)
	if len(pressure) != 1 || pressure[0].InUse != 2 {
		t.Fatalf("pressure = %+v", pressure)
	}
	// Second collection: panicking provider skipped, good provider still returned.
	pressure = q.BackendPressure(ctx)
	if len(pressure) != 1 || pressure[0].InUse != 2 {
		t.Fatalf("pressure after panic = %+v", pressure)
	}
}
