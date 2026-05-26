// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Example: expose a bounded API semaphore via ResourcePressureReader.
package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/haluan/go-keylane"
)

type semaphorePool struct {
	limit   int64
	inUse   atomic.Int64
	waitCnt atomic.Uint64
	waitNS  atomic.Int64
}

func (s *semaphorePool) InUse() int              { return int(s.inUse.Load()) }
func (s *semaphorePool) Capacity() int           { return int(s.limit) }
func (s *semaphorePool) WaitCount() uint64       { return s.waitCnt.Load() }
func (s *semaphorePool) WaitTime() time.Duration { return time.Duration(s.waitNS.Load()) }
func (s *semaphorePool) Saturated() bool {
	return s.limit > 0 && s.inUse.Load() >= s.limit
}

func main() {
	pool := &semaphorePool{limit: 8}
	cfg := keylane.Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 8,
		LaneQuotas: map[keylane.Lane]int{"default": 1},
		BackendResources: keylane.BackendResourceConfig{
			PressureProviders: []keylane.BackendPressureProvider{
				keylane.APIClientPressureAdapter{
					Resource: "wallet-api",
					Lane:     keylane.BackendLaneExternalAPI,
					Reader:   pool,
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

	pool.inUse.Store(6)
	for _, p := range q.BackendPressure(ctx) {
		fmt.Printf("resource=%s lane=%s in_use=%d capacity=%d pressure=%.2f\n",
			p.Resource, p.Lane, p.InUse, p.Capacity, p.Pressure)
	}
}
