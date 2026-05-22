// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Minimal OpenTelemetry integration: wire keylane hooks to an in-memory tracer.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/haluan/go-keylane"
	keylaneotel "github.com/haluan/go-keylane/tracing/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func main() {
	sr := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(sr))
	tracer := tp.Tracer("keylane-example")

	obs := keylane.DefaultObservabilityConfig()
	obs.EnableHooks = true
	obs.Hooks = keylaneotel.NewHooks(keylaneotel.Options{
		Tracer:          tracer,
		RecordQueueWait: true,
		RecordRunTime:   true,
	})

	cfg := keylane.Config{
		ShardCount:       2,
		WorkerCount:      1,
		QueueSizePerLane: 50,
		LaneQuotas:       map[keylane.Lane]int{"default": 2},
		Observability:    obs,
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

	done := make(chan struct{}, 3)
	for i := 0; i < 3; i++ {
		if err := q.Submit(ctx, keylane.Job{
			Key:  fmt.Sprintf("k-%d", i),
			Lane: "default",
			Run: func(ctx context.Context) error {
				done <- struct{}{}
				return nil
			},
		}); err != nil {
			log.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		<-done
	}

	fmt.Printf("recorded %d spans\n", len(sr.Ended()))
	for _, sp := range sr.Ended() {
		fmt.Printf("  %s\n", sp.Name())
	}
}
