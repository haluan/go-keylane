// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Context cancellation during Await is distinct from scheduler shutdown.
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
	cfg.QueueSizePerLane = 8
	cfg.LaneQuotas = map[keylane.Lane]int{"default": 1}

	q, _ := keylane.New(cfg)
	runCtx, runCancel := context.WithCancel(context.Background())
	_ = q.Start(runCtx)
	defer func() {
		runCancel()
		_ = q.Stop(context.Background(), keylane.WithDrain(false))
	}()

	block := make(chan struct{})
	future, err := keylane.SubmitValue(runCtx, q, keylane.ValueJob[int]{
		Key: "slow", Lane: "default",
		Run: func(ctx context.Context) (int, error) {
			select {
			case <-block:
				return 1, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		},
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	awaitCtx, awaitCancel := context.WithCancel(context.Background())
	awaitCancel()
	_, err = future.Await(awaitCtx)
	if !errors.Is(err, context.Canceled) {
		fmt.Printf("await err=%v want canceled\n", err)
		os.Exit(1)
	}
	close(block)
	fmt.Println("cancel=ok")
}
