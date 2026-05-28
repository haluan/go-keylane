// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Experimental: SubmitPipeline with sync stages and WithBackend lease in one stage.
// Not a distributed workflow — in-process orchestration only.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/haluan/go-keylane"
)

type state struct {
	Validated bool
	Rows      int
}

type output struct {
	OK bool
}

func main() {
	cfg := keylane.ProductionDefaults()
	cfg.ShardCount = 1
	cfg.WorkerCount = 2
	cfg.QueueSizePerLane = 32
	cfg.LaneQuotas = map[keylane.Lane]int{"default": 1}
	cfg.BackendResources = keylane.BackendResourceConfig{
		Enabled: true,
		Resources: map[keylane.BackendResourceName]keylane.BackendResourcePolicy{
			"primary-db": {
				Lanes: map[keylane.BackendLane]keylane.BackendLanePolicy{
					keylane.BackendLaneDBRead: {MaxInFlight: 4, Admission: keylane.BackendAdmissionReject},
				},
			},
		},
	}

	q, err := keylane.New(cfg)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	defer func() { _ = q.Stop(context.Background(), keylane.WithDrain(true)) }()

	future, err := keylane.SubmitPipeline(ctx, q, keylane.Pipeline[state, output]{
		Meta: keylane.RequestMeta{Key: "req-1", Lane: "default", Operation: "pipeline-backend"},
		Stages: []keylane.PipelineStage[state]{
			{
				Meta: keylane.StageMeta{Name: keylane.StageValidate},
				Run: func(_ context.Context, st state) (state, error) {
					st.Validated = true
					return st, nil
				},
			},
			{
				Meta: keylane.StageMeta{Name: keylane.StageDBRead},
				Run: func(ctx context.Context, st state) (state, error) {
					op := keylane.BackendOperationFromStage(ctx, "primary-db", keylane.BackendLaneDBRead)
					return keylane.WithBackend(ctx, q, op, func(context.Context) (state, error) {
						st.Rows = 1
						return st, nil
					})
				},
			},
		},
		Complete: func(_ context.Context, st state) (output, error) {
			return output{OK: st.Validated && st.Rows > 0}, nil
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
	fmt.Printf("ok=%v\n", out.OK)
}
