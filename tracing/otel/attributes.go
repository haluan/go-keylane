// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package otel

import (
	"strconv"

	"github.com/haluan/go-keylane"
	"go.opentelemetry.io/otel/attribute"
)

const (
	attrShardID     = "keylane.shard_id"
	attrLane        = "keylane.lane"
	attrQueueWaitMS = "keylane.queue_wait_ms"
	attrRunMS       = "keylane.run_ms"
	attrQueueDepth  = "keylane.queue_depth"
	attrInflight    = "keylane.inflight_jobs"
	attrPressure    = "keylane.pressure_ratio"
	attrSlowThresh  = "keylane.slow_job_threshold_ms"
	attrOutcome     = "keylane.outcome"
)

func timingAttributes(ev keylane.JobTimingEvent, recordQueueWait, recordRun bool) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.Int(attrShardID, ev.ShardID),
		attribute.String(attrLane, string(ev.Lane)),
		attribute.String(attrOutcome, outcomeString(ev.Outcome)),
	}
	if recordQueueWait {
		attrs = append(attrs, attribute.Int64(attrQueueWaitMS, ev.QueueWait.Milliseconds()))
	}
	if recordRun {
		attrs = append(attrs, attribute.Int64(attrRunMS, ev.RunDuration.Milliseconds()))
	}
	return attrs
}

func slowJobAttributes(ev keylane.SlowJobEvent, recordQueueWait, recordRun bool) []attribute.KeyValue {
	attrs := timingAttributes(keylane.JobTimingEvent{
		ShardID:     ev.ShardID,
		Lane:        ev.Lane,
		QueueWait:   ev.QueueWait,
		RunDuration: ev.RunDuration,
		Outcome:     ev.Outcome,
	}, recordQueueWait, recordRun)
	return append(attrs, attribute.Int64(attrSlowThresh, ev.Threshold.Milliseconds()))
}

func pressureAttributes(q *keylane.Queue) []attribute.KeyValue {
	if q == nil {
		return nil
	}
	p := q.Pressure()
	return []attribute.KeyValue{
		attribute.Float64(attrPressure, p.TotalDepthRatio),
		attribute.Int64(attrQueueDepth, int64(p.TotalDepth)),
		attribute.Int64(attrInflight, int64(p.TotalInFlight)),
	}
}

func outcomeString(o keylane.JobOutcome) string {
	switch o {
	case keylane.JobOutcomeCompleted:
		return "completed"
	case keylane.JobOutcomeFailed:
		return "failed"
	case keylane.JobOutcomeCanceled:
		return "canceled"
	case keylane.JobOutcomePanicked:
		return "panicked"
	default:
		return strconv.Itoa(int(o))
	}
}
