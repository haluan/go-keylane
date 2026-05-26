// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"testing"
	"time"
)

func testBackendResourceConfig() BackendResourceConfig {
	return BackendResourceConfig{
		Enabled: true,
		Resources: map[BackendResourceName]BackendResourcePolicy{
			"primary-db": {
				Lanes: map[BackendLane]BackendLanePolicy{
					BackendLaneDBRead:  {MaxInFlight: 2, Admission: BackendAdmissionReject},
					BackendLaneDBWrite: {MaxInFlight: 1, Admission: BackendAdmissionReject},
				},
			},
		},
	}
}

func newBackendTestQueue(t *testing.T, ctx context.Context) *Queue {
	t.Helper()
	cfg := newTestConfig()
	cfg.BackendResources = testBackendResourceConfig()
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return q
}

func TestBackendAcquireDisabledNoOpLease(t *testing.T) {
	ctx := testTimeout(t)
	cfg := newTestConfig()
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
	lease.Release()
}

func TestBackendAcquireUnknownResourceRejected(t *testing.T) {
	ctx := testTimeout(t)
	q := newBackendTestQueue(t, ctx)
	_, err := AcquireBackend(ctx, q, BackendOperation{
		Resource: "missing",
		Lane:     BackendLaneDBRead,
	})
	if !errors.Is(err, ErrBackendAdmission) {
		t.Fatalf("err = %v", err)
	}
	var berr BackendAdmissionError
	if !errors.As(err, &berr) || berr.Decision.Reason != BackendAdmissionUnknownResource {
		t.Fatalf("decision = %+v", err)
	}
}

func TestBackendAcquireUnknownLaneRejected(t *testing.T) {
	ctx := testTimeout(t)
	q := newBackendTestQueue(t, ctx)
	_, err := AcquireBackend(ctx, q, BackendOperation{
		Resource: "primary-db",
		Lane:     BackendLaneExternalAPI,
	})
	if !errors.Is(err, ErrBackendAdmission) {
		t.Fatalf("err = %v", err)
	}
}

func TestBackendMaxInFlightEnforced(t *testing.T) {
	ctx := testTimeout(t)
	q := newBackendTestQueue(t, ctx)
	op := BackendOperation{Resource: "primary-db", Lane: BackendLaneDBWrite}
	l1, err := AcquireBackend(ctx, q, op)
	if err != nil {
		t.Fatal(err)
	}
	defer l1.Release()
	_, err = AcquireBackend(ctx, q, op)
	if !errors.Is(err, ErrBackendAdmission) {
		t.Fatalf("second acquire err = %v", err)
	}
}

func TestBackendReleaseDecrementsInFlight(t *testing.T) {
	ctx := testTimeout(t)
	q := newBackendTestQueue(t, ctx)
	op := BackendOperation{Resource: "primary-db", Lane: BackendLaneDBRead}
	l1, err := AcquireBackend(ctx, q, op)
	if err != nil {
		t.Fatal(err)
	}
	l2, err := AcquireBackend(ctx, q, op)
	if err != nil {
		l1.Release()
		t.Fatal(err)
	}
	l1.Release()
	l2.Release()
	lease, err := AcquireBackend(ctx, q, op)
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()
}

func TestBackendDoubleReleaseSafe(t *testing.T) {
	ctx := testTimeout(t)
	q := newBackendTestQueue(t, ctx)
	lease, err := AcquireBackend(ctx, q, BackendOperation{
		Resource: "primary-db",
		Lane:     BackendLaneDBRead,
	})
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()
	lease.Release()
}

func TestBackendAcquireContextCancelled(t *testing.T) {
	ctx := testTimeout(t)
	q := newBackendTestQueue(t, ctx)
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := AcquireBackend(cctx, q, BackendOperation{
		Resource: "primary-db",
		Lane:     BackendLaneDBRead,
	})
	if !errors.Is(err, ErrBackendAdmission) {
		t.Fatalf("ErrBackendAdmission: err = %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("context.Canceled: err = %v", err)
	}
	if f := classifyFailureWithPolicy(err, FailurePolicy{}); f.Kind != FailureCancelled {
		t.Fatalf("failure kind = %v", f.Kind)
	}
}

func TestBackendAcquireDeadlineExhausted(t *testing.T) {
	ctx := testTimeout(t)
	q := newBackendTestQueue(t, ctx)
	bg := context.Background()
	exec := singleRequestExecutionContext(
		RequestMeta{Key: "k", Lane: "default"}, 0, 0, 1,
		SnapshotDeadlineBudget(NewDeadlineBudget(bg, time.Now().Add(time.Minute)), time.Now()),
	)
	exec.Deadline.HasDeadline = true
	exec.Deadline.BudgetExhausted = true
	exec.Deadline.Remaining = 0
	stageCtx := ContextWithStageExecution(bg, exec)
	_, err := AcquireBackend(stageCtx, q, BackendOperation{
		Resource: "primary-db",
		Lane:     BackendLaneDBRead,
	})
	if !errors.Is(err, ErrBackendAdmission) {
		t.Fatalf("err = %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("context.DeadlineExceeded: err = %v", err)
	}
	var berr BackendAdmissionError
	if !errors.As(err, &berr) || berr.Decision.Reason != BackendAdmissionDeadlineExhausted {
		t.Fatalf("reason = %v", berr.Decision.Reason)
	}
	var fail Failure
	if !errors.As(err, &fail) || fail.Kind != FailureDeadlineExhausted {
		t.Fatalf("failure = %+v", fail)
	}
}

func TestBackendAdmissionSaturatedStillErrBackendAdmission(t *testing.T) {
	ctx := testTimeout(t)
	q := newBackendTestQueue(t, ctx)
	op := BackendOperation{Resource: "primary-db", Lane: BackendLaneDBWrite}
	lease, err := AcquireBackend(ctx, q, op)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	_, err = AcquireBackend(ctx, q, op)
	if !errors.Is(err, ErrBackendAdmission) {
		t.Fatalf("ErrBackendAdmission: err = %v", err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("saturated must not unwrap context errors: err = %v", err)
	}
	var berr BackendAdmissionError
	if !errors.As(err, &berr) || berr.Decision.Reason != BackendAdmissionSaturated {
		t.Fatalf("reason = %v", berr.Decision.Reason)
	}
}

func TestBackendAcquireReleaseRace(t *testing.T) {
	ctx := testTimeout(t)
	q := newBackendTestQueue(t, ctx)
	op := BackendOperation{Resource: "primary-db", Lane: BackendLaneDBRead}
	const N = 40
	errCh := make(chan error, N)
	for i := 0; i < N; i++ {
		go func() {
			lease, err := AcquireBackend(ctx, q, op)
			if err != nil {
				errCh <- err
				return
			}
			time.Sleep(time.Millisecond)
			lease.Release()
			errCh <- nil
		}()
	}
	for i := 0; i < N; i++ {
		if err := <-errCh; err != nil && !errors.Is(err, ErrBackendAdmission) {
			t.Fatal(err)
		}
	}
	snap := q.DebugSnapshot().BackendResources
	if len(snap) != 1 || len(snap[0].Lanes) != 2 {
		t.Fatalf("snapshot = %+v", snap)
	}
	for _, lane := range snap[0].Lanes {
		if lane.Lane == BackendLaneDBRead && lane.InFlight != 0 {
			t.Fatalf("inflight = %d after race", lane.InFlight)
		}
	}
}
