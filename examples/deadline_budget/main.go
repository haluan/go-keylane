// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Example: caller deadline captured as DeadlineBudget on the future.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/haluan/go-keylane"
)

func main() {
	q, err := keylane.New(keylane.Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 8,
		LaneQuotas: map[keylane.Lane]int{"default": 1},
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	startCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(startCtx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer func() { _ = q.Stop(context.Background()) }()

	submitCtx, submitCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer submitCancel()

	future, err := keylane.SubmitValue(submitCtx, q, keylane.ValueJob[int]{
		Key: "k", Lane: "default",
		Run: func(ctx context.Context) (int, error) {
			time.Sleep(5 * time.Millisecond)
			return 1, nil
		},
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	_, _ = future.Await(submitCtx)

	budget, ok := keylane.BudgetFromFuture(future)
	if !ok {
		fmt.Println("no budget on future")
		os.Exit(1)
	}
	fmt.Printf("has_deadline=%v exhausted=%v remaining=%v\n",
		budget.HasDeadline, budget.Exhausted, budget.Remaining)
}
