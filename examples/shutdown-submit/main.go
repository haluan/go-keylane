// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Submit after Stop returns ErrStopped; distinct from queue full or job failure.
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
	ctx := context.Background()
	_ = q.Start(ctx)
	_ = q.Stop(ctx, keylane.WithDrain(false))

	err := q.Submit(ctx, keylane.Job{
		Key: "late", Lane: "default",
		Run: func(context.Context) error { return nil },
	})
	if !errors.Is(err, keylane.ErrStopped) {
		fmt.Printf("submit err=%v want stopped\n", err)
		os.Exit(1)
	}

	future, err := keylane.SubmitValue(ctx, q, keylane.ValueJob[int]{
		Key: "late", Lane: "default",
		Run: func(context.Context) (int, error) { return 1, nil },
	})
	if !errors.Is(err, keylane.ErrStopped) {
		fmt.Printf("submit value err=%v want stopped\n", err)
		os.Exit(1)
	}
	_, awaitErr := future.Await(ctx)
	if !errors.Is(awaitErr, keylane.ErrStopped) {
		fmt.Printf("await err=%v want stopped\n", awaitErr)
		os.Exit(1)
	}
	fmt.Println("stopped=ok")
}
