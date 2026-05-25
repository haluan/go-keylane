// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Example: retry suppressed under queue overload (global_overload).
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
	q, err := keylane.New(keylane.Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[keylane.Lane]int{"default": 1},
		Retry: keylane.RetryPolicy{
			Enabled: true, MaxAttempts: 5,
			InitialBackoff: time.Millisecond, Jitter: false, JitterFraction: 0.01, MinRemainingBudget: 0,
		},
		RetrySuppression: keylane.RetrySuppressionPolicy{
			Enabled: true, SuppressWhenOverloaded: true,
		},
	})
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

	hold := make(chan struct{})
	defer close(hold)
	fill := func(context.Context) error { <-hold; return nil }

	var effects atomic.Int32
	future, err := keylane.SubmitValue(ctx, q, keylane.ValueJob[int]{
		Key: "job", Lane: "default",
		Idempotency: keylane.Idempotency{Safety: keylane.RetrySafetySafe},
		Run: func(context.Context) (int, error) {
			if effects.Add(1) == 1 {
				for i := 0; i < 9; i++ {
					_ = q.Submit(ctx, keylane.Job{Key: "fill", Lane: "default", Run: fill})
				}
			}
			return 0, keylane.RetryableFailure(errors.New("t"))
		},
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	_, _ = future.Await(ctx)
	trace, _ := keylane.RetryTraceFromFuture(future)
	fmt.Printf("handler_runs=%d suppression=%s snap_suppressed=%d\n",
		effects.Load(), trace.Final.SuppressionReason,
		q.RetryFailureSnapshot().RetriesSuppressedTotal)
}
