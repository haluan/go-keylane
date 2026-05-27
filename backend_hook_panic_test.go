// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haluan/go-keylane/internal/core"
)

func backendHookTestQueue(t *testing.T, hooks BackendResourceHooks) (*Queue, context.Context) {
	t.Helper()
	ctx := testTimeout(t)
	cfg := newTestConfig()
	cfg.BackendResources = testBackendResourceConfig()
	cfg.Observability.EnableHooks = true
	cfg.Observability.Hooks.Backend = hooks
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopTestQueue(t, q) })
	return q, ctx
}

func backendAcquireWithStageContext(t *testing.T, ctx context.Context, q *Queue) BackendLease {
	t.Helper()
	deadline := SnapshotDeadlineBudget(NewDeadlineBudget(ctx, time.Now().Add(time.Minute)), time.Now())
	meta := RequestMeta{RequestID: "req-backend", Key: "k1", Lane: "payment", Transport: "http", Operation: "charge"}
	exec := baseExecutionContext(meta, 0, 0, 1, StageMeta{Name: StageValidate}, 0, 1, deadline)
	stageCtx := ContextWithStageExecution(ctx, exec)
	op := BackendOperationFromStage(stageCtx, "primary-db", BackendLaneDBRead)
	lease, err := AcquireBackend(stageCtx, q, op)
	if err != nil {
		t.Fatal(err)
	}
	return lease
}

func TestBackendAdmissionHookPanicLeaseReleased(t *testing.T) {
	before := HookPanicsRecovered()
	var released atomic.Int32
	q, ctx := backendHookTestQueue(t, BackendResourceHooks{
		OnBackendAdmission: func(dec BackendAdmissionDecision) {
			if dec.Accepted {
				panic("admission hook panic")
			}
		},
		OnBackendReleased: func(BackendReleaseEvent) { released.Add(1) },
	})
	lease := backendAcquireWithStageContext(t, ctx, q)
	if released.Load() != 0 {
		t.Fatal("release hook must not run before Release")
	}
	lease.Release()
	if released.Load() != 1 {
		t.Fatalf("released = %d, want 1", released.Load())
	}
	if HookPanicsRecovered() <= before {
		t.Fatal("expected hook panic diagnostic increment")
	}
}

func TestBackendReleasedHookPanicAfterAcquire(t *testing.T) {
	before := HookPanicsRecovered()
	q, ctx := backendHookTestQueue(t, BackendResourceHooks{
		OnBackendReleased: func(BackendReleaseEvent) { panic("release hook panic") },
	})
	lease := backendAcquireWithStageContext(t, ctx, q)
	lease.Release()
	if HookPanicsRecovered() <= before {
		t.Fatal("expected hook panic diagnostic increment")
	}
	lease2 := backendAcquireWithStageContext(t, ctx, q)
	lease2.Release()
}

func TestBackendAdmissionHookRedactsRequestIDByDefault(t *testing.T) {
	var got BackendAdmissionDecision
	q, ctx := backendHookTestQueue(t, BackendResourceHooks{
		OnBackendAdmission: func(dec BackendAdmissionDecision) {
			if dec.Accepted {
				got = dec
			}
		},
	})
	lease := backendAcquireWithStageContext(t, ctx, q)
	lease.Release()
	if got.RequestID != "" {
		t.Fatalf("RequestID = %q, want empty (redacted)", got.RequestID)
	}
	if got.KeyHash != core.HashKey("k1") {
		t.Fatalf("KeyHash = %d, want %d", got.KeyHash, core.HashKey("k1"))
	}
}
