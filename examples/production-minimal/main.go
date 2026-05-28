// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// v0.8 production-minimal: ProductionDefaults, ValidateConfig, SubmitValue, Await, graceful Stop.
// Start here for new integrations. See docs/production-minimal.md.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/haluan/go-keylane"
)

func main() {
	cfg := keylane.ProductionDefaults()
	cfg.LaneQuotas = map[keylane.Lane]int{
		"default": 2,
	}
	cfg.QueueSizePerLane = 64
	cfg.WorkerCount = 2
	cfg.ShardCount = 2

	report := keylane.ValidateConfig(cfg)
	for _, w := range report.Issues {
		if w.Severity == keylane.ValidationWarning {
			fmt.Printf("config warning %s: %s\n", w.Code, w.Message)
		}
	}
	if report.HasErrors() {
		fmt.Println(report.Err())
		os.Exit(1)
	}

	q, err := keylane.New(cfg)
	if err != nil {
		fmt.Println("new:", err)
		os.Exit(1)
	}
	for _, w := range q.ConfigValidationWarnings() {
		fmt.Printf("queue warning %s: %s\n", w.Code, w.Message)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		fmt.Println("start:", err)
		os.Exit(1)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		if err := q.Stop(stopCtx, keylane.WithDrain(true)); err != nil {
			fmt.Println("stop:", err)
		}
	}()

	// Opaque business id — do not use raw keys as metric labels.
	tenantID := "tenant-7f3a"

	future, err := keylane.SubmitValue(ctx, q, keylane.ValueJob[string]{
		Key:  tenantID,
		Lane: "default",
		Run: func(ctx context.Context) (string, error) {
			select {
			case <-time.After(20 * time.Millisecond):
				return "ok", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		},
	})
	if err != nil {
		if errors.Is(err, keylane.ErrQueueFull) {
			fmt.Println("admission rejected: queue full")
			os.Exit(1)
		}
		fmt.Println("submit:", err)
		os.Exit(1)
	}

	awaitCtx, awaitCancel := context.WithTimeout(ctx, time.Second)
	defer awaitCancel()
	result, err := future.Await(awaitCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			fmt.Println("await: deadline exceeded")
			os.Exit(1)
		}
		if errors.Is(err, context.Canceled) {
			fmt.Println("await: canceled")
			os.Exit(1)
		}
		fmt.Println("await:", err)
		os.Exit(1)
	}
	fmt.Printf("result=%s\n", result)
}
