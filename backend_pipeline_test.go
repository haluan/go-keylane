// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"testing"
)

func TestPipelineStageWithBackendSync(t *testing.T) {
	ctx := testTimeout(t)
	q := newBackendTestQueue(t, ctx)

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				Run: func(ctx context.Context, st pState) (pState, error) {
					op := BackendOperationFromStage(ctx, "primary-db", BackendLaneDBRead)
					return WithBackend(ctx, q, op, func(ctx context.Context) (pState, error) {
						st.Val = 7
						return st, nil
					})
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	out, err := future.Await(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if out.Sum != 7 {
		t.Fatalf("sum = %d", out.Sum)
	}
}
