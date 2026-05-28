// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Await with a deadline context while work is still running.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/haluan/go-keylane"
)

func main() {
	cfg := keylane.ProductionDefaults()
	cfg.ShardCount = 1
	cfg.WorkerCount = 1
	cfg.QueueSizePerLane = 8
	cfg.LaneQuotas = map[keylane.Lane]int{"default": 1}

	q, _ := keylane.New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	defer func() { _ = q.Stop(context.Background(), keylane.WithDrain(false)) }()

	block := make(chan struct{})
	future, _ := keylane.SubmitValue(ctx, q, keylane.ValueJob[int]{
		Key: "slow", Lane: "default",
		Run: func(context.Context) (int, error) {
			<-block
			return 1, nil
		},
	})

	awaitCtx, awaitCancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer awaitCancel()
	_, err := future.Await(awaitCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		fmt.Printf("await err=%v want deadline\n", err)
		os.Exit(1)
	}
	close(block)
	fmt.Println("timeout=ok")
}
