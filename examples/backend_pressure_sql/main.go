// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Example: connect database/sql pool stats to keylane backend pressure diagnostics.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/haluan/go-keylane"
)

// demoDB implements SQLDBStatsReader (same contract as *sql.DB).
type demoDB struct{}

func (demoDB) Stats() sql.DBStats {
	return sql.DBStats{
		InUse:              2,
		Idle:               1,
		MaxOpenConnections: 8,
		WaitCount:          1,
	}
}

func main() {
	var db demoDB
	cfg := keylane.Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 8,
		LaneQuotas: map[keylane.Lane]int{"default": 1},
		BackendResources: keylane.BackendResourceConfig{
			PressureProviders: []keylane.BackendPressureProvider{
				keylane.SQLDBPressureAdapter{
					Resource: "primary-db",
					Lane:     keylane.BackendLaneDBRead,
					DB:       db,
				},
			},
		},
	}
	q, err := keylane.New(cfg)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	ctx := context.Background()
	if err := q.Start(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer func() { _ = q.Stop(context.Background()) }()

	for _, p := range q.BackendPressure(ctx) {
		fmt.Printf("resource=%s lane=%s in_use=%d capacity=%d pressure=%.2f saturated=%v\n",
			p.Resource, p.Lane, p.InUse, p.Capacity, p.Pressure, p.Saturated)
	}
}
