// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Example: bounded retry with SubmitValue and RetryableFailure.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/haluan/go-keylane"
)

func main() {
	cfg := keylane.Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 16,
		LaneQuotas: map[keylane.Lane]int{"default": 1},
		Retry: keylane.RetryPolicy{
			Enabled: true, MaxAttempts: 3,
			InitialBackoff: time.Millisecond, Jitter: false, JitterFraction: 0.01,
			MinRemainingBudget: 0,
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

	var n atomic.Int32
	future, err := keylane.SubmitValue(ctx, q, keylane.ValueJob[int]{
		Key: "demo", Lane: "default",
		Idempotency: keylane.Idempotency{Safety: keylane.RetrySafetySafe},
		Run: func(context.Context) (int, error) {
			if n.Add(1) < 2 {
				return 0, keylane.RetryableFailure(errors.New("transient"))
			}
			return 42, nil
		},
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	val, err := future.Await(ctx)
	if err != nil {
		fmt.Println("await:", err)
		os.Exit(1)
	}
	fmt.Printf("result=%d attempts=%d scheduled=%d\n",
		val, n.Load(), q.RetryFailureSnapshot().RetriesScheduledTotal)
}
