// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Safe retry: idempotent read-like work with Retry + Idempotency opt-in.
// Retry is disabled in ProductionDefaults — enable explicitly after validation.
package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/haluan/go-keylane"
)

func main() {
	cfg := keylane.ProductionDefaults()
	cfg.ShardCount = 1
	cfg.WorkerCount = 1
	cfg.QueueSizePerLane = 8
	cfg.LaneQuotas = map[keylane.Lane]int{"default": 1}
	cfg.Retry = keylane.RetryPolicy{
		Enabled:        true,
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
	}

	q, _ := keylane.New(cfg)
	ctx := context.Background()
	_ = q.Start(ctx)
	defer func() { _ = q.Stop(context.Background(), keylane.WithDrain(true)) }()

	var runs atomic.Int32
	future, _ := keylane.SubmitValue(ctx, q, keylane.ValueJob[int]{
		Key: "read-idempotent", Lane: "default",
		Idempotency: keylane.Idempotency{Safety: keylane.RetrySafetySafe},
		Run: func(context.Context) (int, error) {
			if runs.Add(1) < 2 {
				return 0, keylane.RetryableFailure(errors.New("transient"))
			}
			return 1, nil
		},
	})
	_, err := future.Await(ctx)
	if err != nil {
		fmt.Println("await:", err)
		return
	}
	fmt.Printf("safe_retry_runs=%d\n", runs.Load())
}
