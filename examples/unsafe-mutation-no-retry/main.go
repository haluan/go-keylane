// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Unsafe mutation: non-idempotent write — retry stays disabled (default).
// Do NOT enable retry for writes without idempotency protection.
package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/haluan/go-keylane"
)

func main() {
	cfg := keylane.ProductionDefaults()
	cfg.ShardCount = 1
	cfg.WorkerCount = 1
	cfg.QueueSizePerLane = 8
	cfg.LaneQuotas = map[keylane.Lane]int{"default": 1}
	// Retry.Enabled remains false — intentional for mutations.

	q, _ := keylane.New(cfg)
	ctx := context.Background()
	_ = q.Start(ctx)
	defer func() { _ = q.Stop(context.Background(), keylane.WithDrain(true)) }()

	var writes atomic.Int32
	future, _ := keylane.SubmitValue(ctx, q, keylane.ValueJob[int]{
		Key: "write-once", Lane: "default",
		Idempotency: keylane.Idempotency{Safety: keylane.RetrySafetyUnsafe},
		Run: func(context.Context) (int, error) {
			writes.Add(1)
			return 0, keylane.RetryableFailure(errors.New("would retry if enabled"))
		},
	})
	_, err := future.Await(ctx)
	if err == nil {
		fmt.Println("expected failure without retry")
		return
	}
	fmt.Printf("mutation_no_retry_writes=%d\n", writes.Load())
}
