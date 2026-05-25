// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Example: safe retry succeeds; unsafe mutation suppresses retry.
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
	base := keylane.Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 8,
		LaneQuotas: map[keylane.Lane]int{"default": 1},
		Retry: keylane.RetryPolicy{
			Enabled: true, MaxAttempts: 3,
			InitialBackoff: time.Millisecond, Jitter: false, JitterFraction: 0.01, MinRemainingBudget: 0,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	run := func(label string, safety keylane.RetrySafety) {
		q, _ := keylane.New(base)
		_ = q.Start(ctx)
		var n atomic.Int32
		future, _ := keylane.SubmitValue(ctx, q, keylane.ValueJob[int]{
			Key: label, Lane: "default",
			Idempotency: keylane.Idempotency{Safety: safety},
			Run: func(context.Context) (int, error) {
				n.Add(1)
				return 0, keylane.RetryableFailure(errors.New("t"))
			},
		})
		_, _ = future.Await(ctx)
		trace, _ := keylane.RetryTraceFromFuture(future)
		fmt.Printf("%s: handler_runs=%d safety_reason=%s suppressed_total=%d\n",
			label, n.Load(), trace.Final.SafetyReason,
			q.RetryFailureSnapshot().RetrySafetySuppressedTotal)
		_ = q.Stop(context.Background())
	}

	run("safe", keylane.RetrySafetySafe)
	run("unsafe", keylane.RetrySafetyUnsafe)
}
