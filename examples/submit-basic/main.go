// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Fire-and-forget Submit with admission rejection vs successful enqueue.
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

func main() {
	cfg := keylane.ProductionDefaults()
	cfg.ShardCount = 1
	cfg.WorkerCount = 1
	cfg.QueueSizePerLane = 2
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

	block := make(chan struct{})
	_ = q.Submit(ctx, keylane.Job{
		Key: "block-1", Lane: "default",
		Run: func(context.Context) error { <-block; return nil },
	})
	_ = q.Submit(ctx, keylane.Job{
		Key: "block-2", Lane: "default",
		Run: func(context.Context) error { <-block; return nil },
	})

	var wg sync.WaitGroup
	wg.Add(1)
	err = q.Submit(ctx, keylane.Job{
		Key: "extra", Lane: "default",
		Run: func(context.Context) error {
			wg.Done()
			return nil
		},
	})
	if errors.Is(err, keylane.ErrQueueFull) {
		fmt.Println("rejected=queue_full")
		close(block)
		wg.Wait()
		os.Exit(0)
	}
	if err != nil {
		fmt.Println("submit:", err)
		close(block)
		os.Exit(1)
	}
	close(block)
	wg.Wait()
	time.Sleep(50 * time.Millisecond)
	fmt.Println("accepted=ok")
}
