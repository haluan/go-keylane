// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package otel

import (
	"context"
	"testing"
	"time"

	"github.com/haluan/go-keylane"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

func TestNewHooksDisabled(t *testing.T) {
	h := NewHooks(Options{Disabled: true, Tracer: nooptrace.NewTracerProvider().Tracer("t")})
	if h.OnJobTiming != nil || h.OnSlowJob != nil {
		t.Error("expected nil hooks when disabled")
	}
}

func TestNewHooksNilTracer(t *testing.T) {
	h := NewHooks(Options{})
	if h.OnJobTiming != nil {
		t.Error("expected nil OnJobTiming when tracer is nil")
	}
}

func TestOnJobTimingRecordsSpan(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(sr))
	tracer := tp.Tracer("test")

	h := NewHooks(Options{
		Tracer:          tracer,
		RecordQueueWait: true,
		RecordRunTime:   true,
	})

	h.OnJobTiming(keylane.JobTimingEvent{
		ShardID:     2,
		Lane:        "payment",
		QueueWait:   10 * time.Millisecond,
		RunDuration: 5 * time.Millisecond,
		Outcome:     keylane.JobOutcomeCompleted,
	})

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}
	span := spans[0]
	if span.Name() != "keylane.job" {
		t.Errorf("name = %q, want keylane.job", span.Name())
	}
	assertAttr(t, span.Attributes(), attrShardID, 2)
	assertAttrString(t, span.Attributes(), attrLane, "payment")
	assertAttrInt64(t, span.Attributes(), attrQueueWaitMS, 10)
	assertAttrInt64(t, span.Attributes(), attrRunMS, 5)
}

func TestOnJobTimingOmitsTimingWhenFlagsOff(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(sr))
	tracer := tp.Tracer("test")

	h := NewHooks(Options{
		Tracer:          tracer,
		RecordQueueWait: false,
		RecordRunTime:   false,
	})

	h.OnJobTiming(keylane.JobTimingEvent{
		ShardID:     0,
		Lane:        "default",
		QueueWait:   10 * time.Millisecond,
		RunDuration: 5 * time.Millisecond,
		Outcome:     keylane.JobOutcomeCompleted,
	})

	attrs := sr.Ended()[0].Attributes()
	for _, a := range attrs {
		if a.Key == attrQueueWaitMS || a.Key == attrRunMS {
			t.Errorf("unexpected timing attribute %q when flags off", a.Key)
		}
	}
}

func TestOnJobTimingWithPressure(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(sr))
	tracer := tp.Tracer("test")

	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, err := keylane.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	h := NewHooks(Options{
		Tracer:         tracer,
		Queue:          q,
		RecordPressure: true,
	})
	h.OnJobTiming(keylane.JobTimingEvent{
		ShardID: 0, Lane: "default",
		Outcome: keylane.JobOutcomeCompleted,
	})

	attrs := sr.Ended()[0].Attributes()
	found := false
	for _, a := range attrs {
		if a.Key == attrPressure {
			found = true
		}
	}
	if !found {
		t.Error("expected keylane.pressure_ratio attribute")
	}
}

func TestOnSlowJobRecordsSpan(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(sr))
	tracer := tp.Tracer("test")

	h := NewHooks(Options{Tracer: tracer, RecordRunTime: true})
	h.OnSlowJob(keylane.SlowJobEvent{
		ShardID:     1,
		Lane:        "audit",
		RunDuration: 200 * time.Millisecond,
		Threshold:   100 * time.Millisecond,
		Outcome:     keylane.JobOutcomeCompleted,
	})

	if len(sr.Ended()) != 1 || sr.Ended()[0].Name() != "keylane.slow_job" {
		t.Errorf("got %+v", sr.Ended())
	}
}

func TestHooksIntegrationWithQueue(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(sr))
	tracer := tp.Tracer("test")

	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 2},
		Observability: keylane.ObservabilityConfig{
			EnableHooks: true,
			Hooks: NewHooks(Options{
				Tracer:          tracer,
				RecordQueueWait: true,
				RecordRunTime:   true,
			}),
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
		Key:  "k",
		Lane: "default",
		Run: func(ctx context.Context) error {
			close(done)
			return nil
		},
	})
	<-done
	time.Sleep(50 * time.Millisecond)

	if len(sr.Ended()) < 1 {
		t.Fatal("expected at least one span from hook")
	}
}

func assertAttr(t *testing.T, attrs []attribute.KeyValue, key string, want int) {
	t.Helper()
	for _, a := range attrs {
		if string(a.Key) == key {
			if int(a.Value.AsInt64()) != want {
				t.Errorf("%s = %v, want %d", key, a.Value.AsInt64(), want)
			}
			return
		}
	}
	t.Errorf("missing attribute %q", key)
}

func assertAttrString(t *testing.T, attrs []attribute.KeyValue, key, want string) {
	t.Helper()
	for _, a := range attrs {
		if string(a.Key) == key {
			if a.Value.AsString() != want {
				t.Errorf("%s = %q, want %q", key, a.Value.AsString(), want)
			}
			return
		}
	}
	t.Errorf("missing attribute %q", key)
}

func assertAttrInt64(t *testing.T, attrs []attribute.KeyValue, key string, want int64) {
	t.Helper()
	for _, a := range attrs {
		if string(a.Key) == key {
			if a.Value.AsInt64() != want {
				t.Errorf("%s = %d, want %d", key, a.Value.AsInt64(), want)
			}
			return
		}
	}
	t.Errorf("missing attribute %q", key)
}
