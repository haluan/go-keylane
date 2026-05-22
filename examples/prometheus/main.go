// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Minimal Prometheus integration: register a Keylane collector and print one scrape.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/haluan/go-keylane"
	keylaneprom "github.com/haluan/go-keylane/metrics/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

func main() {
	cfg := keylane.Config{
		ShardCount:       4,
		WorkerCount:      2,
		QueueSizePerLane: 100,
		LaneQuotas:       map[keylane.Lane]int{"default": 2},
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

	for i := 0; i < 10; i++ {
		_ = q.Submit(ctx, keylane.Job{
			Key:  fmt.Sprintf("key-%d", i),
			Lane: "default",
			Run:  func(ctx context.Context) error { return nil },
		})
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(keylaneprom.NewCollector(q, keylaneprom.CollectorOptions{
		SchedulerName: "example",
	}))

	mfs, err := reg.Gather()
	if err != nil {
		log.Fatal(err)
	}
	enc := expfmt.NewEncoder(os.Stdout, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, mf := range mfs {
		if err := enc.Encode(mf); err != nil {
			log.Fatal(err)
		}
	}
}
