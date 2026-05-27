// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Example: two-stage synchronous SubmitPipeline with Future.Await.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/haluan/go-keylane"
)

type state struct {
	Value int
}

type output struct {
	Result int
}

func main() {
	cfg := keylane.Config{
		ShardCount: 1, WorkerCount: 2, QueueSizePerLane: 32,
		LaneQuotas: map[keylane.Lane]int{"default": 1},
	}
	q, err := keylane.New(cfg)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer func() { _ = q.Stop(context.Background()) }()

	future, err := keylane.SubmitPipeline(ctx, q, keylane.Pipeline[state, output]{
		Meta: keylane.RequestMeta{Key: "demo", Lane: "default", Operation: "pipeline-basics"},
		Stages: []keylane.PipelineStage[state]{
			{
				Meta: keylane.StageMeta{Name: keylane.StageValidate},
				Run: func(_ context.Context, st state) (state, error) {
					st.Value = 10
					return st, nil
				},
			},
			{
				Meta: keylane.StageMeta{Name: keylane.StageBusiness},
				Run: func(_ context.Context, st state) (state, error) {
					st.Value *= 2
					return st, nil
				},
			},
		},
		Complete: func(_ context.Context, st state) (output, error) {
			return output{Result: st.Value}, nil
		},
	})
	if err != nil {
		fmt.Println("submit:", err)
		os.Exit(1)
	}
	out, err := future.Await(ctx)
	if err != nil {
		fmt.Println("await:", err)
		os.Exit(1)
	}
	fmt.Printf("result=%d\n", out.Result)
}
