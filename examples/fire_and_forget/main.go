// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/haluan/go-keylane"
)

func main() {
	// 1. Define standard configuration
	cfg := keylane.Config{
		ShardCount:       4,
		WorkerCount:      2,
		QueueSizePerLane: 10,
		LaneQuotas: map[keylane.Lane]int{
			"payment": 2,
			"webhook": 1,
		},
	}

	// 2. Initialize the queue
	q, err := keylane.New(cfg)
	if err != nil {
		fmt.Printf("failed to initialize keylane: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 3. Submit fire-and-forget jobs before starting or during execution
	var wg sync.WaitGroup

	submitJob := func(key string, lane keylane.Lane, jobNum int) {
		wg.Add(1)
		err := q.Submit(ctx, keylane.Job{
			Key:  key,
			Lane: lane,
			Run: func(ctx context.Context) error {
				defer wg.Done()
				fmt.Printf("[WORKER] Started processing %s job #%d for key %s\n", lane, jobNum, key)
				time.Sleep(50 * time.Millisecond) // Simulate lightweight work
				fmt.Printf("[WORKER] Completed processing %s job #%d for key %s\n", lane, jobNum, key)
				return nil
			},
		})
		if err != nil {
			wg.Done()
			fmt.Printf("failed to submit job: %v\n", err)
		}
	}

	fmt.Println("Submitting fire-and-forget jobs...")
	submitJob("tenant-A", "payment", 1)
	submitJob("tenant-A", "payment", 2)
	submitJob("tenant-B", "payment", 3)
	submitJob("tenant-A", "webhook", 1)
	submitJob("tenant-C", "webhook", 2)

	// 4. Start the workers
	fmt.Println("Starting scheduler workers...")
	if err := q.Start(ctx); err != nil {
		fmt.Printf("failed to start scheduler: %v\n", err)
		os.Exit(1)
	}

	// 5. Wait for all submitted work to complete
	wg.Wait()
	fmt.Println("All submitted jobs completed execution.")

	// 6. Graceful Stop
	fmt.Println("Stopping queue scheduler gracefully...")
	if err := q.Stop(ctx, keylane.WithDrain(true)); err != nil {
		fmt.Printf("failed to stop queue: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Go-Keylane fire-and-forget example successfully completed.")
}
