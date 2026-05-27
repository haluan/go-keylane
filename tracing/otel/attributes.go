// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package otel

import (
	"strconv"

	"github.com/haluan/go-keylane"
	"go.opentelemetry.io/otel/attribute"
)

const (
	// AttrShardID is the stable OTEL attribute for shard id.
	AttrShardID = "keylane.shard_id"
	// AttrLane is the stable OTEL attribute for lane name.
	AttrLane = "keylane.lane"
	// AttrQueueWaitMS is the stable OTEL attribute for queue wait milliseconds.
	AttrQueueWaitMS = "keylane.queue_wait_ms"
	// AttrRunMS is the stable OTEL attribute for run duration milliseconds.
	AttrRunMS = "keylane.run_ms"
	// AttrQueueDepth is the stable OTEL attribute for queue depth.
	AttrQueueDepth = "keylane.queue_depth"
	// AttrInflight is the stable OTEL attribute for in-flight job count.
	AttrInflight = "keylane.inflight_jobs"
	// AttrPressure is the stable OTEL attribute for pressure ratio.
	AttrPressure = "keylane.pressure_ratio"
	// AttrSlowThresh is the stable OTEL attribute for slow job threshold milliseconds.
	AttrSlowThresh = "keylane.slow_job_threshold_ms"
	// AttrOutcome is the stable OTEL attribute for job outcome.
	AttrOutcome = "keylane.outcome"
)

// StableTraceAttributeKeys returns documented stable span attribute keys for the OTEL adapter.
func StableTraceAttributeKeys() []string {
	return []string{
		AttrShardID,
		AttrLane,
		AttrQueueWaitMS,
		AttrRunMS,
		AttrQueueDepth,
		AttrInflight,
		AttrPressure,
		AttrSlowThresh,
		AttrOutcome,
	}
}

const (
	attrShardID     = AttrShardID
	attrLane        = AttrLane
	attrQueueWaitMS = AttrQueueWaitMS
	attrRunMS       = AttrRunMS
	attrQueueDepth  = AttrQueueDepth
	attrInflight    = AttrInflight
	attrPressure    = AttrPressure
	attrSlowThresh  = AttrSlowThresh
	attrOutcome     = AttrOutcome
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
