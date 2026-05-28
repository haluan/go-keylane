// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// v0.8 request-runtime: bounded lanes, rejection under pressure, timeouts, graceful shutdown.
// For HTTP integration use httpkeylane.Middleware — see docs/http-middleware.md.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/haluan/go-keylane"
)

const (
	laneRead  keylane.Lane = "read"
	laneWrite keylane.Lane = "write"
)

func main() {
	cfg := keylane.ProductionDefaults()
	cfg.ShardCount = 4
	cfg.WorkerCount = 2
	cfg.QueueSizePerLane = 8
	cfg.LaneQuotas = map[keylane.Lane]int{
		laneRead:  2,
		laneWrite: 1,
	}

	q, err := keylane.New(cfg)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = q.Stop(stopCtx, keylane.WithDrain(true))
	}()

	var accepted, rejected int
	var mu sync.Mutex

	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lane := laneRead
			if i%3 == 0 {
				lane = laneWrite
			}
			reqCtx, reqCancel := context.WithTimeout(ctx, 200*time.Millisecond)
			defer reqCancel()

			future, err := keylane.SubmitValue(reqCtx, q, keylane.ValueJob[int]{
				Key:  fmt.Sprintf("entity-%d", i%6),
				Lane: lane,
				Run: func(runCtx context.Context) (int, error) {
					select {
					case <-time.After(30 * time.Millisecond):
						return i, nil
					case <-runCtx.Done():
						return 0, runCtx.Err()
					}
				},
			})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if errors.Is(err, keylane.ErrQueueFull) {
					rejected++
					return
				}
				return
			}
			accepted++
			_, _ = future.Await(reqCtx)
		}(i)
	}
	wg.Wait()

	fmt.Printf("accepted=%d rejected_queue_full=%d\n", accepted, rejected)
}
