package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/haluan/go-keylane"
)

func main() {
	// Configuration
	cfg := keylane.Config{
		ShardCount:       8,
		WorkerCount:      2,
		QueueSizePerLane: 100,
		LaneQuotas: map[keylane.Lane]int{
			"payment": 2,
			"audit":   1,
		},
	}

	// Initialize the queue
	q, err := keylane.New(cfg)
	if err != nil {
		fmt.Printf("failed to create queue: %v\n", err)
		os.Exit(1)
	}

	// Start the scheduler
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := q.Start(ctx); err != nil {
		fmt.Printf("failed to start queue: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Queue started. Submitting value jobs...")

	// Submit a value job
	future, err := keylane.SubmitValue(ctx, q, keylane.ValueJob[string]{
		Key:  "user-123",
		Lane: "payment",
		Run: func(ctx context.Context) (string, error) {
			// Simulate some work
			time.Sleep(100 * time.Millisecond)
			return "payment-processed-successfully", nil
		},
	})
	if err != nil {
		fmt.Printf("failed to submit value job: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Job submitted. Waiting for result...")

	// Await with timeout
	awaitCtx, awaitCancel := context.WithTimeout(ctx, 1*time.Second)
	defer awaitCancel()

	result, err := future.Await(awaitCtx)
	if err != nil {
		fmt.Printf("Await failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Result received: %s\n", result)

	// Submit multiple concurrent jobs
	fmt.Println("\nSubmitting concurrent jobs...")
	const count = 5
	futures := make([]keylane.Future[int], count)
	for i := 0; i < count; i++ {
		val := i
		f, _ := keylane.SubmitValue(ctx, q, keylane.ValueJob[int]{
			Key:  fmt.Sprintf("key-%d", i),
			Lane: "audit",
			Run: func(ctx context.Context) (int, error) {
				time.Sleep(time.Duration(val*50) * time.Millisecond)
				return val * val, nil
			},
		})
		futures[i] = f
	}

	for i, f := range futures {
		res, _ := f.Await(ctx)
		fmt.Printf("Job %d result: %d\n", i, res)
	}

	fmt.Println("\nExample completed successfully.")
}
