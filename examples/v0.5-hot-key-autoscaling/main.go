// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// v0.5 example: hot key detection (observe mode), DebugSnapshot, and ScaleSignal.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/haluan/go-keylane"
)

func main() {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 64,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
		HotKey:           keylane.DefaultHotKeyConfig(),
		// Observe-only: detection on, per-key admission off.
		PerKeyAdmission:   keylane.PerKeyAdmissionConfig{},
		ShardPressure:     keylane.DefaultShardPressureConfig(),
		AutoscalingSignal: keylane.DefaultAutoscalingSignalConfig(),
	}

	q, err := keylane.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = q.Stop(stopCtx, keylane.WithDrain(false))
	}()

	block := make(chan struct{})
	defer close(block)
	run := func(ctx context.Context) error {
		select {
		case <-block:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Synthetic demo key — not real PII.
	const demoKey = "tenant-demo-7"
	for i := 0; i < 40; i++ {
		_ = q.Submit(ctx, keylane.Job{
			Key:  demoKey,
			Lane: "default",
			Run:  run,
		})
	}

	snap := q.DebugSnapshot()
	fmt.Println("=== Hot key candidates (hash only) ===")
	for _, hk := range snap.HotKeys {
		fmt.Printf("  shard=%d key_hash=%x depth_ratio=%.2f status=%s\n",
			hk.ShardID, hk.KeyHash, hk.DepthRatio, hk.Status)
	}

	sig := q.ScaleSignal()
	fmt.Println("\n=== Scale signal ===")
	fmt.Printf("  recommended=%v reason=%s scope=%s pressure_ratio=%.2f diagnostics=%v\n",
		sig.Recommended, sig.Reason, sig.Scope, sig.PressureRatio, sig.DiagnosticsEnabled)

	if os.Getenv("KEYLANE_V05_JSON") == "1" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{
			"hot_key_count": len(snap.HotKeys),
			"scale_signal": map[string]any{
				"recommended":    sig.Recommended,
				"reason":         sig.Reason,
				"scope":          sig.Scope,
				"pressure_ratio": sig.PressureRatio,
			},
		})
	}
}
