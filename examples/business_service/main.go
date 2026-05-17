// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/haluan/go-keylane"
)

// Define Lanes
const (
	LanePayment keylane.Lane = "payment"
	LaneAudit   keylane.Lane = "audit"
	LaneWebhook keylane.Lane = "webhook"
)

// MockPaymentService represents a realistic business controller
type MockPaymentService struct {
	queue *keylane.Queue
}

// ProcessTransaction processes a customer transaction and returns a confirmation reference
func (s *MockPaymentService) ProcessTransaction(ctx context.Context, customerID string, amount float64) (string, error) {
	// We submit as a ValueJob to retrieve the transaction confirmation ID back
	future, err := keylane.SubmitValue(ctx, s.queue, keylane.ValueJob[string]{
		Key:  customerID,
		Lane: LanePayment,
		Run: func(ctx context.Context) (string, error) {
			fmt.Printf("[SERVICE] Billing payment of $%.2f for client %s...\n", amount, customerID)

			// Simulating API billing request and respecting context cancellations
			select {
			case <-time.After(80 * time.Millisecond):
				// payment completed successfully
			case <-ctx.Done():
				return "", ctx.Err()
			}

			txRef := fmt.Sprintf("tx_ref_%d", rand.Intn(1000000))
			fmt.Printf("[SERVICE] Billed successfully! Ref: %s\n", txRef)
			return txRef, nil
		},
	})
	if err != nil {
		return "", fmt.Errorf("billing submission failed: %w", err)
	}

	// Wait for execution confirmation (with a timeout)
	awaitCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	ref, err := future.Await(awaitCtx)
	if err != nil {
		return "", fmt.Errorf("failed awaiting payment: %w", err)
	}
	return ref, nil
}

// LogAuditRecord submits an asynchronous fire-and-forget record to the audit queue
func (s *MockPaymentService) LogAuditRecord(ctx context.Context, customerID string, event string) {
	err := s.queue.Submit(ctx, keylane.Job{
		Key:  customerID,
		Lane: LaneAudit,
		Run: func(ctx context.Context) error {
			fmt.Printf("[AUDIT] Writing audit event '%s' for client %s\n", event, customerID)
			return nil
		},
	})
	if err != nil {
		fmt.Printf("[ERROR] Failed enqueuing audit: %v\n", err)
	}
}

// DispatchWebhook sends an event webhook to the user endpoint
func (s *MockPaymentService) DispatchWebhook(ctx context.Context, customerID string, payload string) {
	err := s.queue.Submit(ctx, keylane.Job{
		Key:  customerID,
		Lane: LaneWebhook,
		Run: func(ctx context.Context) error {
			fmt.Printf("[WEBHOOK] Dispatched payload for %s...\n", customerID)
			return nil
		},
	})
	if err != nil {
		fmt.Printf("[ERROR] Failed enqueuing webhook: %v\n", err)
	}
}

func main() {
	// Initialize deterministic seed for mocking
	rand.Seed(time.Now().UnixNano())

	// Configure the isolated scheduling engine
	cfg := keylane.Config{
		ShardCount:       8,
		WorkerCount:      4,
		QueueSizePerLane: 100,
		LaneQuotas: map[keylane.Lane]int{
			LanePayment: 3, // High execution preference
			LaneAudit:   1, // Low execution preference
			LaneWebhook: 1, // Medium-low background execution
		},
	}

	q, err := keylane.New(cfg)
	if err != nil {
		fmt.Printf("failed to initialize queue: %v\n", err)
		os.Exit(1)
	}

	service := &MockPaymentService{queue: q}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start Keylane
	if err := q.Start(ctx); err != nil {
		fmt.Printf("failed to start scheduler: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Business service started successfully.")

	// Launch parallel requests simulating customer activity
	var wg sync.WaitGroup
	customers := []string{"customer-Alpha", "customer-Beta", "customer-Gamma"}

	for _, client := range customers {
		wg.Add(1)
		go func(c string) {
			defer wg.Done()

			// 1. Process payment via synchronous SubmitValue
			ref, err := service.ProcessTransaction(ctx, c, 99.99)
			if err != nil {
				fmt.Printf("[CLIENT ERROR] Payment failed for %s: %v\n", c, err)
				return
			}
			fmt.Printf("[CLIENT CONFIRMED] Payment completed successfully for %s: ref=%s\n", c, ref)

			// 2. Queue fire-and-forget audit trace
			service.LogAuditRecord(ctx, c, "payment_authorized")

			// 3. Dispatch webhooks asynchronously
			service.DispatchWebhook(ctx, c, `{"status":"completed"}`)
		}(client)
	}

	wg.Wait()

	// Submit one more job and stop the queue with drain
	fmt.Println("\nPreparing for graceful shutdown...")
	service.LogAuditRecord(ctx, "system", "service_shutdown_initiated")

	// Call Stop with Drain. Any enqueued audit logs will be processed before Stop returns.
	if err := q.Stop(ctx, keylane.WithDrain(true)); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			fmt.Println("[SHUTDOWN WARNING] Graceful shutdown timed out, some tasks may not have finished.")
		} else {
			fmt.Printf("[SHUTDOWN ERROR] Shutdown returned error: %v\n", err)
		}
	} else {
		fmt.Println("Scheduler shut down successfully, all enqueued tasks fully drained.")
	}
}
