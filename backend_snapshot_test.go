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
		if lane.Lane == BackendLaneDBRead && lane.InFlight == 1 && lane.Capacity == 2 {
			found = true
		}
	}
	if !found {
		t.Fatalf("lanes = %+v", snap.BackendResources[0].Lanes)
	}
	lease.Release()
}

func TestDebugSnapshotBackendDisabledEmpty(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)
	snap := q.DebugSnapshot()
	if len(snap.BackendResources) != 0 {
		t.Fatalf("backend resources = %+v", snap.BackendResources)
	}
}
