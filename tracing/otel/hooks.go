// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Package otel provides OpenTelemetry hooks for keylane schedulers.
//
// Stable Candidate optional adapter; span attributes use low-cardinality fields only.
package otel

import (
	"context"

	"github.com/haluan/go-keylane"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Options configures the OpenTelemetry hook adapter.
type Options struct {
	Tracer trace.Tracer
	Queue  *keylane.Queue

	RecordQueueWait bool
	RecordRunTime   bool
	RecordPressure  bool

	Disabled bool
}

// NewHooks returns keylane.Hooks that record scheduler timing as OTEL spans.
// When Disabled is true or Tracer is nil, returns empty hooks (nil funcs).
func NewHooks(opts Options) keylane.Hooks {
	if opts.Disabled || opts.Tracer == nil {
		return keylane.Hooks{}
	}
	tracer := opts.Tracer
	q := opts.Queue
	recordWait := opts.RecordQueueWait
	recordRun := opts.RecordRunTime
	recordPressure := opts.RecordPressure

	return keylane.Hooks{
		OnJobTiming: func(ev keylane.JobTimingEvent) {
			ctx := context.Background()
			_, span := tracer.Start(ctx, "keylane.job",
				trace.WithAttributes(timingAttributes(ev, recordWait, recordRun)...),
			)
			if recordPressure {
				span.SetAttributes(pressureAttributes(q)...)
			}
			setSpanStatus(span, ev.Outcome)
			span.End()
		},
		OnSlowJob: func(ev keylane.SlowJobEvent) {
			ctx := context.Background()
			_, span := tracer.Start(ctx, "keylane.slow_job",
				trace.WithAttributes(slowJobAttributes(ev, recordWait, recordRun)...),
			)
			if recordPressure {
				span.SetAttributes(pressureAttributes(q)...)
			}
			setSpanStatus(span, ev.Outcome)
			span.End()
		},
	}
}

func setSpanStatus(span trace.Span, outcome keylane.JobOutcome) {
	switch outcome {
	case keylane.JobOutcomeFailed:
		span.SetStatus(codes.Error, "job failed")
	case keylane.JobOutcomeCanceled:
		span.SetStatus(codes.Error, "job canceled")
	default:
		span.SetStatus(codes.Ok, "")
	}
}

// MergeAttributes appends extra attributes (advanced use only; avoid high cardinality).
func MergeAttributes(base []attribute.KeyValue, extra ...attribute.KeyValue) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(base)+len(extra))
	out = append(out, base...)
	out = append(out, extra...)
	return out
}
