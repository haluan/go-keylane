// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package otel

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/haluan/go-keylane"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

func TestStableTraceAttributeKeys(t *testing.T) {
	keys := StableTraceAttributeKeys()
	if len(keys) == 0 {
		t.Fatal("expected stable trace attribute keys")
	}
	for _, k := range keys {
		if k == "keylane.raw_key" || k == "keylane.request_id" {
			t.Fatalf("forbidden stable key %q", k)
		}
	}
}

func TestTraceAttributeContract(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(sr))
	tracer := tp.Tracer("contract-test")

	h := NewHooks(Options{
		Tracer:          tracer,
		RecordQueueWait: true,
		RecordRunTime:   true,
	})

	h.OnJobTiming(keylane.JobTimingEvent{
		ShardID:     1,
		Lane:        "default",
		QueueWait:   5 * time.Millisecond,
		RunDuration: 3 * time.Millisecond,
		Outcome:     keylane.JobOutcomeCompleted,
	})

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}

	stable := StableTraceAttributeKeys()
	for _, attr := range spans[0].Attributes() {
		key := string(attr.Key)
		if key == "keylane.raw_key" || key == "keylane.request_id" || key == "keylane.idempotency_key" {
			t.Fatalf("forbidden attribute %q", key)
		}
		if !slices.Contains(stable, key) {
			t.Errorf("attribute %q not in StableTraceAttributeKeys()", key)
		}
	}
}

func TestTraceHookPanicRecovered(t *testing.T) {
	before := keylane.HookPanicsRecovered()
	custom := keylane.Hooks{
		OnJobTiming: func(keylane.JobTimingEvent) { panic("otel timing panic") },
	}
	cfg := keylane.Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 8,
		LaneQuotas: map[keylane.Lane]int{"default": 1},
		Observability: keylane.ObservabilityConfig{
			EnableHooks: true, SlowJobThreshold: time.Millisecond,
			Hooks: custom,
		},
	}
	q, err := keylane.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	done := make(chan struct{})
	_ = q.Submit(context.Background(), keylane.Job{
		Key: "k", Lane: "default",
		Run: func(context.Context) error {
			time.Sleep(2 * time.Millisecond)
			close(done)
			return nil
		},
	})
	<-done
	deadline := time.After(3 * time.Second)
	for {
		if keylane.HookPanicsRecovered() > before {
			return
		}
		select {
		case <-deadline:
			t.Fatal("expected hook panic diagnostic increment from trace hook path")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestNewHooksDisabledContract(t *testing.T) {
	h := NewHooks(Options{Disabled: true, Tracer: nooptrace.NewTracerProvider().Tracer("t")})
	if h.OnJobTiming != nil || h.OnSlowJob != nil {
		t.Error("expected nil hooks when disabled")
	}
}
