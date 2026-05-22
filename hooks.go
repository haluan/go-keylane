// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "time"

// Hooks contains user-definable callbacks for observability events.
type Hooks struct {
	// OnJobTiming is called after each accepted job finishes Run, with queue wait and run duration.
	OnJobTiming func(JobTimingEvent)
	// OnSlowJob is called when a job's run duration meets or exceeds the slow job threshold.
	OnSlowJob func(SlowJobEvent)
}

// JobTimingEvent contains queue wait and run duration for a completed job.
type JobTimingEvent struct {
	ShardID     int
	LaneID      uint16
	Lane        Lane
	QueueWait   time.Duration
	RunDuration time.Duration
	Outcome     JobOutcome
}

// SlowJobEvent contains details about a slow job execution.
type SlowJobEvent struct {
	ShardID     int
	LaneID      uint16
	Lane        Lane
	QueueWait   time.Duration
	RunDuration time.Duration
	Threshold   time.Duration
	Outcome     JobOutcome
}

// JobOutcome describes how a job finished.
type JobOutcome uint8

const (
	// JobOutcomeCompleted indicates the job returned nil.
	JobOutcomeCompleted JobOutcome = iota
	// JobOutcomeFailed indicates the job returned a non-nil error other than context.Canceled.
	JobOutcomeFailed
	// JobOutcomeCanceled indicates the job returned context.Canceled or was skipped due to worker cancel.
	JobOutcomeCanceled
	// JobOutcomePanicked is reserved for panic recovery; not emitted until panic recovery exists.
	JobOutcomePanicked
)
