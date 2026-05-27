// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"testing"
)

func TestDebugSnapshotBackendResources(t *testing.T) {
	ctx := testTimeout(t)
	cfg := newTestConfig()
	cfg.BackendResources = testBackendResourceConfig()
	cfg.Observability.EnableDebugSnapshot = true
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}

	lease, err := AcquireBackend(ctx, q, BackendOperation{
		Resource: "primary-db",
		Lane:     BackendLaneDBRead,
	})
	if err != nil {
		t.Fatal(err)
	}

	snap := q.DebugSnapshot()
	if len(snap.BackendResources) != 1 {
		t.Fatalf("resources = %d", len(snap.BackendResources))
	}
	if snap.BackendResources[0].Resource != "primary-db" {
		t.Fatalf("resource = %q", snap.BackendResources[0].Resource)
	}
	found := false
	for _, lane := range snap.BackendResources[0].Lanes {
		if lane.Lane == BackendLaneDBRead &&
			lane.InFlight == 1 &&
			lane.Capacity == 2 &&
			lane.Queued == 0 &&
			!lane.Saturated {
			found = true
		}
	}
	if !found {
		t.Fatalf("lanes = %+v", snap.BackendResources[0].Lanes)
	}
	lease.Release()
}

func TestDebugSnapshotBackendLaneFieldsIncludeQueued(t *testing.T) {
	ctx := testTimeout(t)
	cfg := newTestConfig()
	cfg.BackendResources = testBackendResourceConfig()
	cfg.Observability.EnableDebugSnapshot = true
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}

	lease, err := AcquireBackend(ctx, q, BackendOperation{
		Resource: "primary-db", Lane: BackendLaneDBWrite,
	})
	if err != nil {
		t.Fatal(err)
	}

	snap := q.DebugSnapshot()
	var lane BackendLaneSnapshot
	found := false
	for _, res := range snap.BackendResources {
		for _, l := range res.Lanes {
			if l.Lane == BackendLaneDBWrite {
				lane = l
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("lanes = %+v", snap.BackendResources)
	}
	if lane.InFlight != 1 || lane.Capacity != 1 {
		t.Fatalf("inflight/capacity = %+v", lane)
	}
	if lane.Queued != 0 {
		t.Fatalf("Queued = %d, want 0 under reject admission", lane.Queued)
	}
	if !lane.Saturated {
		t.Fatal("expected Saturated when inflight == capacity")
	}
	lease.Release()
}

func TestDebugSnapshotBackendPressure(t *testing.T) {
	ctx := testTimeout(t)
	prov := staticPressureProvider{snap: BackendPressureSnapshot{
		Resource:  "primary-db",
		Lane:      BackendLaneDBRead,
		InUse:     2,
		Capacity:  8,
		Pressure:  0.375,
		Saturated: false,
	}}
	cfg := newTestConfig()
	cfg.BackendResources.PressureProviders = []BackendPressureProvider{prov}
	cfg.Observability.EnableDebugSnapshot = true
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	snap := q.DebugSnapshot()
	if len(snap.BackendPressure) != 1 {
		t.Fatalf("pressure = %+v", snap.BackendPressure)
	}
	bp := snap.BackendPressure[0]
	if bp.Resource != "primary-db" || bp.Lane != BackendLaneDBRead {
		t.Fatalf("pressure identity = %+v", bp)
	}
	if bp.InUse != 2 || bp.Capacity != 8 || bp.Pressure != 0.375 {
		t.Fatalf("pressure ratio/usage = %+v", bp)
	}
	if bp.Saturated {
		t.Fatalf("pressure saturated = %+v", bp)
	}
}

func TestDebugSnapshotBackendDisabledEmpty(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)
	snap := q.DebugSnapshot()
	if len(snap.BackendResources) != 0 {
		t.Fatalf("backend resources = %+v", snap.BackendResources)
	}
}
