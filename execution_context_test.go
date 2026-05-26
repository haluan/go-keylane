// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"testing"
	"time"
)

func TestStageExecutionFromContextAbsent(t *testing.T) {
	_, ok := StageExecutionFromContext(context.Background())
	if ok {
		t.Fatal("expected ok=false on plain context")
	}
}

func TestContextWithStageExecutionPreservesCancellation(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	exec := baseExecutionContext(
		RequestMeta{Key: "k", Lane: "default"},
		0, 0, 1,
		StageMeta{Name: StageValidate}, 0, 1,
		DeadlineBudgetSnapshot{},
	)
	child := ContextWithStageExecution(parent, exec)
	cancel()

	if child.Err() == nil {
		t.Fatal("expected cancelled child context")
	}
	got, ok := StageExecutionFromContext(child)
	if !ok {
		t.Fatal("execution metadata missing on cancelled context")
	}
	if got.Stage.Name != StageValidate {
		t.Fatalf("stage = %q", got.Stage.Name)
	}
}

func TestStageExecutionContextImmutabilityAcrossDerivation(t *testing.T) {
	base := baseExecutionContext(
		RequestMeta{Key: "k", Lane: "default"},
		1, time.Millisecond, 1,
		StageMeta{Name: StageValidate}, 0, 2,
		DeadlineBudgetSnapshot{},
	)
	ctxA := ContextWithStageExecution(context.Background(), base)

	execB := withPipelineStage(base, StageMeta{Name: StageDBRead}, 1, 2, time.Second, DeadlineBudgetSnapshot{})
	ctxB := ContextWithStageExecution(context.Background(), execB)

	gotA, _ := StageExecutionFromContext(ctxA)
	gotB, _ := StageExecutionFromContext(ctxB)

	if gotA.Stage.Name != StageValidate || gotA.StageIndex != 0 {
		t.Fatalf("stage A mutated: %+v", gotA)
	}
	if gotB.Stage.Name != StageDBRead || gotB.StageIndex != 1 {
		t.Fatalf("stage B wrong: %+v", gotB)
	}
}

func TestRequestMetaFromExecution(t *testing.T) {
	exec := baseExecutionContext(
		RequestMeta{RequestID: "r1", Key: "k", Lane: "pay", Transport: "http", Operation: "op"},
		3, 0, 1,
		StageMeta{Name: StageBusiness}, 0, 1,
		DeadlineBudgetSnapshot{},
	)
	meta := RequestMetaFromExecution(exec)
	if meta.RequestID != "r1" || meta.Key != "k" || meta.Lane != "pay" {
		t.Fatalf("meta = %+v", meta)
	}
}

func TestSnapshotDeadlineBudgetNoDeadline(t *testing.T) {
	snap := SnapshotDeadlineBudget(NewDeadlineBudget(context.Background(), time.Now()), time.Now())
	if snap.HasDeadline {
		t.Fatal("expected no deadline on plain context")
	}
	if snap.Remaining != 0 {
		t.Fatalf("remaining = %v, want 0", snap.Remaining)
	}
}

func TestSnapshotDeadlineBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	now := time.Now()
	b := NewDeadlineBudget(ctx, now)
	snap := SnapshotDeadlineBudget(b, now)
	if !snap.HasDeadline {
		t.Fatal("expected deadline")
	}
	if snap.Remaining <= 0 {
		t.Fatalf("remaining = %v", snap.Remaining)
	}
}
