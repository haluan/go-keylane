// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"testing"
)

func TestSubmitPipelineStageFailurePreservesFailureKind(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			permanentFailStage(StageBusiness, errors.New("bad input")),
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, awaitErr := future.Await(context.Background())
	if awaitErr == nil {
		t.Fatal("expected error")
	}

	sf, ok := AsStageFailure(awaitErr)
	if !ok {
		t.Fatalf("expected StageFailure, got %T", awaitErr)
	}
	if sf.Stage.Name != StageBusiness {
		t.Fatalf("stage = %q", sf.Stage.Name)
	}

	assertFutureFailureKind(t, future, FailurePermanent)
}

func TestNewStageFailureUnwrap(t *testing.T) {
	root := errors.New("root")
	wrapped := NewStageFailure(StageMeta{Name: StageValidate}, root)
	if !errors.Is(wrapped, root) {
		t.Fatal("unwrap failed")
	}
	sf, ok := AsStageFailure(wrapped)
	if !ok || sf.Stage.Name != StageValidate {
		t.Fatalf("AsStageFailure = %+v ok=%v", sf, ok)
	}
}
