// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"testing"
	"time"
)

func TestDebugSnapshotPipelineV07Fields(t *testing.T) {
	ctx := testTimeout(t)
	cfg := newTestConfig()
	cfg.Continuation = ContinuationConfig{Enabled: true, MaxPending: 8}
	cfg.BackendResources = testBackendResourceConfig()
	cfg.BackendResources.PressureProviders = []BackendPressureProvider{
		staticPressureProvider{snap: BackendPressureSnapshot{
			Resource: "primary-db", Lane: BackendLaneDBRead,
			InUse: 1, Capacity: 4, Pressure: 0.25,
		}},
	}
	cfg.Observability.EnableDebugSnapshot = true
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}

	yielded := make(chan struct{})
	var completer ContinuationCompleter[pState]
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](context.Background())
					completer = c
					close(yielded)
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, yielded)
	waitUntil(t, func() bool {
		return q.DebugSnapshot().Continuation.Pending == 1
	}, 2*time.Second)
	if q.DebugSnapshot().Continuation.Pending != 1 {
		t.Fatalf("Continuation.Pending at yield = %d", q.DebugSnapshot().Continuation.Pending)
	}

	snap := q.DebugSnapshot()
	if snap.Version != DebugSnapshotVersion {
		t.Fatalf("version = %q", snap.Version)
	}
	if snap.Continuation.MaxPending != 8 {
		t.Fatalf("Continuation.MaxPending = %d", snap.Continuation.MaxPending)
	}
	if len(snap.BackendPressure) != 1 {
		t.Fatalf("BackendPressure = %+v", snap.BackendPressure)
	}
	bp := snap.BackendPressure[0]
	if bp.Resource != "primary-db" || bp.Lane != BackendLaneDBRead {
		t.Fatalf("BackendPressure identity = %+v", bp)
	}
	if bp.Pressure != 0.25 || bp.InUse != 1 || bp.Capacity != 4 {
		t.Fatalf("BackendPressure ratio/usage = %+v", bp)
	}

	completer.Complete(pState{Val: 1})
	if _, err := future.Await(ctx); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool {
		return q.DebugSnapshot().Continuation.Pending == 0
	}, 2*time.Second)
	snap = q.DebugSnapshot()
	if snap.Continuation.Pending != 0 {
		t.Fatalf("pending after complete = %d", snap.Continuation.Pending)
	}
}

func TestDebugSnapshotPipelineFeaturesDisabled(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)
	snap := q.DebugSnapshot()
	if snap.Version != DebugSnapshotVersion {
		t.Fatalf("version = %q", snap.Version)
	}
	if len(snap.BackendResources) != 0 {
		t.Fatalf("BackendResources = %+v", snap.BackendResources)
	}
	if len(snap.BackendPressure) != 0 {
		t.Fatalf("BackendPressure = %+v", snap.BackendPressure)
	}
	if snap.Continuation.Pending != 0 || snap.Continuation.MaxPending != 0 {
		t.Fatalf("Continuation = %+v", snap.Continuation)
	}
}

func TestDebugSnapshotBackendResourcesSaturated(t *testing.T) {
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

	l1, err := AcquireBackend(ctx, q, BackendOperation{
		Resource: "primary-db", Lane: BackendLaneDBWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer l1.Release()

	snap := q.DebugSnapshot()
	saturated := false
	for _, res := range snap.BackendResources {
		for _, lane := range res.Lanes {
			if lane.Lane == BackendLaneDBWrite && lane.Saturated {
				saturated = true
			}
		}
	}
	if !saturated {
		t.Fatalf("lanes = %+v", snap.BackendResources)
	}
}
