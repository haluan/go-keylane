// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Example: RetryFailureSnapshot and RetryTraceFromFuture after retry.
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
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 16,
		LaneQuotas: map[keylane.Lane]int{"default": 1},
		Retry: keylane.RetryPolicy{
			Enabled: true, MaxAttempts: 3,
			InitialBackoff: time.Millisecond, Jitter: false, JitterFraction: 0.01, MinRemainingBudget: 0,
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

	var n atomic.Int32
	future, err := keylane.SubmitValue(ctx, q, keylane.ValueJob[int]{
		Key: "obs", Lane: "default",
		Idempotency: keylane.Idempotency{Safety: keylane.RetrySafetySafe},
		Run: func(context.Context) (int, error) {
			if n.Add(1) < 2 {
				return 0, keylane.RetryableFailure(errors.New("t"))
			}
			return 1, nil
		},
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	_, _ = future.Await(ctx)

	trace, ok := keylane.RetryTraceFromFuture(future)
	if !ok {
		fmt.Println("no trace")
		os.Exit(1)
	}
	snap := q.RetryFailureSnapshot()
	fmt.Printf("succeeded=%v attempts=%d scheduled=%d failures_total=%d\n",
		trace.Final.Succeeded, snap.AttemptsTotal, snap.RetriesScheduledTotal, snap.FailuresTotal)
}
