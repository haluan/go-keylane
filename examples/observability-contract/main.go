// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Observability contract: low-allocation hooks with lane/outcome labels only.
// For Prometheus/OTEL adapters see examples/prometheus and examples/otel_hooks.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/haluan/go-keylane"
)

func main() {
	cfg := keylane.ProductionDefaults()
	cfg.Observability = keylane.ObservabilityConfig{
		EnableHooks: true,
		Hooks: keylane.Hooks{
			OnJobTiming: func(e keylane.JobTimingEvent) {
				// Safe: lane, outcome — not raw key or request_id.
				_ = e.Lane
				_ = e.Outcome
			},
		},
	}
	cfg.ShardCount = 2
	cfg.WorkerCount = 1
	cfg.QueueSizePerLane = 16
	cfg.LaneQuotas = map[keylane.Lane]int{"default": 1}

	q, err := keylane.New(cfg)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	defer func() { _ = q.Stop(context.Background(), keylane.WithDrain(true)) }()

	_ = q.Submit(ctx, keylane.Job{
		Key: "tenant-a", Lane: "default",
		Run: func(context.Context) error { return nil },
	})

	stable := len(keylane.StableMetricDescriptors())
	forbidden := keylane.ForbiddenMetricLabelNames()
	fmt.Printf("stable_metrics=%d forbidden_labels=%d hook_panics=%d\n",
		stable, len(forbidden), keylane.HookPanicsRecovered())
}
