// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/haluan/go-keylane/internal/core"
)

func TestBackendHooksEmitAdmissionAndRelease(t *testing.T) {
	ctx := testTimeout(t)
	var admitted atomic.Int32
	var released atomic.Int32
	cfg := newTestConfig()
	cfg.BackendResources = testBackendResourceConfig()
	cfg.Observability.EnableHooks = true
	cfg.Observability.Hooks.Backend.OnBackendAdmission = func(dec BackendAdmissionDecision) {
		if dec.Accepted && dec.Resource == "primary-db" && dec.Lane == BackendLaneDBRead {
			admitted.Add(1)
		}
	}
	cfg.Observability.Hooks.Backend.OnBackendReleased = func(ev BackendReleaseEvent) {
		if ev.Resource == "primary-db" && ev.Lane == BackendLaneDBRead {
			released.Add(1)
		}
	}
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
		Stage:    StageValidate,
	})
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()
	if admitted.Load() != 1 {
		t.Fatalf("admitted = %d", admitted.Load())
	}
	if released.Load() != 1 {
		t.Fatalf("released = %d", released.Load())
	}
}

// Backend hooks expose KeyHash only (no raw routing keys).
func TestBackendHooksAdmissionExecutionMetadata(t *testing.T) {
	ctx := testTimeout(t)
	var admitted BackendAdmissionDecision
	var released BackendReleaseEvent
	cfg := newTestConfig()
	cfg.BackendResources = testBackendResourceConfig()
	cfg.Observability.EnableHooks = true
	cfg.Observability.Hooks.Backend.OnBackendAdmission = func(dec BackendAdmissionDecision) {
		if dec.Accepted {
			admitted = dec
		}
	}
	cfg.Observability.Hooks.Backend.OnBackendReleased = func(ev BackendReleaseEvent) {
		released = ev
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}

	deadline := SnapshotDeadlineBudget(NewDeadlineBudget(ctx, time.Now().Add(time.Minute)), time.Now())
	meta := RequestMeta{
		RequestID: "req-42",
		Key:       "k1",
		Lane:      "payment",
		Transport: "http",
		Operation: "charge",
	}
	exec := baseExecutionContext(meta, 3, 0, 2,
		StageMeta{Name: StageValidate},
		1, 4,
		deadline,
	)
	stageCtx := ContextWithStageExecution(ctx, exec)
	op := BackendOperationFromStage(stageCtx, "primary-db", BackendLaneDBRead)

	lease, err := AcquireBackend(stageCtx, q, op)
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()

	if admitted.RequestID != "" {
		t.Fatalf("RequestID = %q, want empty (redacted)", admitted.RequestID)
	}
	if admitted.KeyHash != core.HashKey("k1") {
		t.Fatalf("KeyHash = %d, want %d", admitted.KeyHash, core.HashKey("k1"))
	}
	if admitted.RequestLane != "payment" {
		t.Fatalf("RequestLane = %q", admitted.RequestLane)
	}
	if admitted.ShardID != 3 {
		t.Fatalf("ShardID = %d", admitted.ShardID)
	}
	if admitted.Stage != StageValidate {
		t.Fatalf("Stage = %q", admitted.Stage)
	}
	if admitted.StageIndex != 1 || admitted.StageCount != 4 {
		t.Fatalf("stage index/count = %d/%d", admitted.StageIndex, admitted.StageCount)
	}
	if admitted.Attempt != 2 {
		t.Fatalf("Attempt = %d", admitted.Attempt)
	}
	if admitted.Operation != "charge" {
		t.Fatalf("Operation = %q", admitted.Operation)
	}
	if admitted.Transport != "http" {
		t.Fatalf("Transport = %q", admitted.Transport)
	}
	if admitted.DeadlineExhausted {
		t.Fatal("DeadlineExhausted = true")
	}
	if admitted.DeadlineRemaining <= 0 {
		t.Fatalf("DeadlineRemaining = %v", admitted.DeadlineRemaining)
	}

	if released.RequestID != admitted.RequestID {
		t.Fatalf("release RequestID = %q", released.RequestID)
	}
	if released.KeyHash != admitted.KeyHash {
		t.Fatalf("release KeyHash = %d, want %d", released.KeyHash, admitted.KeyHash)
	}
	if released.RequestLane != admitted.RequestLane || released.ShardID != admitted.ShardID {
		t.Fatalf("release metadata mismatch: %+v", released)
	}
	if released.HeldFor < 0 {
		t.Fatalf("HeldFor = %v", released.HeldFor)
	}
}

func TestBackendHooksDisabledNoEmit(t *testing.T) {
	ctx := testTimeout(t)
	var admitted atomic.Int32
	cfg := newTestConfig()
	cfg.BackendResources = testBackendResourceConfig()
	cfg.Observability.EnableHooks = false
	cfg.Observability.Hooks.Backend.OnBackendAdmission = func(BackendAdmissionDecision) {
		admitted.Add(1)
	}
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
	if admitted.Load() != 0 {
		t.Fatalf("admitted = %d", admitted.Load())
	}
}
