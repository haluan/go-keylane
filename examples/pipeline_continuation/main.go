// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Example: pipeline stage yields, async work completes the continuation, pipeline resumes.
// Explicit opt-in: Continuation.Enabled must be true (disabled by default in ProductionDefaults).
//
// The continuation model is a handoff primitive only. For backend leases and pool pressure adapters,
// see docs/backend-resource-coordination.md and docs/backend-pressure-adapters.md.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/haluan/go-keylane"
)

type state struct {
	Value int
}

type output struct {
	Sum int
}

func main() {
	cfg := keylane.Config{
		ShardCount: 1, WorkerCount: 2, QueueSizePerLane: 32,
		LaneQuotas: map[keylane.Lane]int{"default": 1},
		Continuation: keylane.ContinuationConfig{
			Enabled: true,
		},
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
		Meta: keylane.RequestMeta{Key: "demo", Lane: "default", Operation: "continuation-demo"},
		Stages: []keylane.PipelineStage[state]{
			{
				Meta: keylane.StageMeta{Name: keylane.StageValidate},
				RunContinuation: func(_ context.Context, st state) (keylane.StageResult[state], error) {
					cont, completer := keylane.NewContinuation[state](context.Background())
					go func() {
						time.Sleep(5 * time.Millisecond)
						completer.Complete(state{Value: 7})
					}()
					return keylane.StageResult[state]{Continuation: cont}, nil
				},
			},
		},
		Complete: func(_ context.Context, s state) (output, error) {
			return output{Sum: s.Value}, nil
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
	fmt.Printf("sum=%d pending=%d\n", out.Sum, q.DebugSnapshot().Continuation.Pending)
}
