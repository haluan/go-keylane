// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Experimental: backend resource leases — acquire, release, cancel, idempotent Release.
// Leases are in-process admission only, not distributed locks.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/haluan/go-keylane"
)

func main() {
	cfg := keylane.ProductionDefaults()
	cfg.ShardCount = 1
	cfg.WorkerCount = 1
	cfg.QueueSizePerLane = 16
	cfg.LaneQuotas = map[keylane.Lane]int{"default": 1}
	cfg.BackendResources = keylane.BackendResourceConfig{
		Enabled: true,
		Resources: map[keylane.BackendResourceName]keylane.BackendResourcePolicy{
			"primary-db": {
				Lanes: map[keylane.BackendLane]keylane.BackendLanePolicy{
					keylane.BackendLaneDBRead: {MaxInFlight: 2, Admission: keylane.BackendAdmissionReject},
				},
			},
		},
	}

	q, err := keylane.New(cfg)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	defer func() { _ = q.Stop(context.Background(), keylane.WithDrain(true)) }()

	op := keylane.BackendOperation{Resource: "primary-db", Lane: keylane.BackendLaneDBRead}
	lease, err := keylane.AcquireBackend(ctx, q, op)
	if err != nil {
		fmt.Println("acquire:", err)
		os.Exit(1)
	}
	lease.Release()
	lease.Release() // idempotent double release

	cctx, ccancel := context.WithCancel(context.Background())
	ccancel()
	_, err = keylane.AcquireBackend(cctx, q, op)
	if err == nil {
		fmt.Println("expected cancel on acquire")
		os.Exit(1)
	}
	if !errors.Is(err, context.Canceled) {
		fmt.Printf("acquire cancel err=%v\n", err)
	}

	fmt.Println("backend_lease=ok")
}
