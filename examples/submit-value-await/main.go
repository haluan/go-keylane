// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// SubmitValue with Await: success path and job failure path (distinct from admission rejection).
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/haluan/go-keylane"
)

func main() {
	cfg := keylane.ProductionDefaults()
	cfg.ShardCount = 1
	cfg.WorkerCount = 1
	cfg.QueueSizePerLane = 16
	cfg.LaneQuotas = map[keylane.Lane]int{"default": 1}

	q, _ := keylane.New(cfg)
	ctx := context.Background()
	_ = q.Start(ctx)
	defer func() { _ = q.Stop(context.Background(), keylane.WithDrain(true)) }()

	okFuture, err := keylane.SubmitValue(ctx, q, keylane.ValueJob[int]{
		Key: "ok", Lane: "default",
		Run: func(context.Context) (int, error) { return 42, nil },
	})
	if err != nil {
		fmt.Println("submit ok:", err)
		os.Exit(1)
	}
	v, err := okFuture.Await(ctx)
	if err != nil || v != 42 {
		fmt.Printf("await ok: val=%d err=%v\n", v, err)
		os.Exit(1)
	}

	failFuture, err := keylane.SubmitValue(ctx, q, keylane.ValueJob[int]{
		Key: "fail", Lane: "default",
		Run: func(context.Context) (int, error) {
			return 0, errors.New("business error")
		},
	})
	if err != nil {
		fmt.Println("submit fail:", err)
		os.Exit(1)
	}
	_, err = failFuture.Await(ctx)
	if err == nil {
		fmt.Println("expected job failure")
		os.Exit(1)
	}
	fmt.Println("success_and_failure=ok")
}
