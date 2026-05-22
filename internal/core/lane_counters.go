// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"errors"
	"sync/atomic"
)

// laneCounters holds atomic metrics counters for a specific lane.
type laneCounters struct {
	// StatsGCPressure cumulative counters.
	submitted atomic.Uint64
	accepted  atomic.Uint64
	rejected  atomic.Uint64
	canceled  atomic.Uint64
	panicked  atomic.Uint64

	// StatsGCPressure queue wait (always on).
	gcQueueWaitCount      atomic.Uint64
	gcQueueWaitTotalNanos atomic.Uint64
	gcQueueWaitMaxNanos   atomic.Uint64

	// StatsGCPressure run duration (always on).
	gcRunCount      atomic.Uint64
	gcRunTotalNanos atomic.Uint64
	gcRunMaxNanos   atomic.Uint64

	// Stats() counters (successful enqueue semantics for submittedTotal, non-GC Pressure).
	submittedTotal      atomic.Int64
	completedTotal      atomic.Int64
	failedTotal         atomic.Int64
	queueFullTotal      atomic.Int64
	queueWaitTotalNanos atomic.Int64
	queueWaitCount      atomic.Int64
}

func (c *laneCounters) snapshotGCPressureQueueWait() QueueWaitStatsGCPressure {
	return QueueWaitStatsGCPressure{
		Count:      c.gcQueueWaitCount.Load(),
		TotalNanos: c.gcQueueWaitTotalNanos.Load(),
		MaxNanos:   c.gcQueueWaitMaxNanos.Load(),
	}
}

func (c *laneCounters) snapshotGCPressureRun() RunStatsGCPressure {
	return RunStatsGCPressure{
		Count:      c.gcRunCount.Load(),
		TotalNanos: c.gcRunTotalNanos.Load(),
		MaxNanos:   c.gcRunMaxNanos.Load(),
	}
}

// snapshotGCPressure returns a read-only copy of cumulative lane counters for StatsGCPressure.
// Admission fields are loaded with Submitted last so a concurrent enqueue that increments
// Submitted before Accepted/Rejected does not produce Submitted < Accepted+Rejected in
// the snapshot (best-effort; not a global point-in-time atomic snapshot).
func (c *laneCounters) snapshotGCPressure() LaneCountersGCPressure {
	completed := uint64(c.completedTotal.Load())
	failed := uint64(c.failedTotal.Load())
	canceled := c.canceled.Load()
	panicked := c.panicked.Load()
	queueFull := uint64(c.queueFullTotal.Load())
	accepted := c.accepted.Load()
	rejected := c.rejected.Load()
	submitted := c.submitted.Load()
	return LaneCountersGCPressure{
		Submitted: submitted,
		Accepted:  accepted,
		Rejected:  rejected,
		Completed: completed,
		Failed:    failed,
		QueueFull: queueFull,
		Canceled:  canceled,
		Panicked:  panicked,
	}
}

// recordLaneAdmissionAttempt increments Submitted for every enqueue attempt.
func (c *laneCounters) recordLaneAdmissionAttempt() {
	c.submitted.Add(1)
}

// recordLaneAdmissionResult updates Accepted/Rejected and v1 counters after enqueueIntoShard.
func (c *laneCounters) recordLaneAdmissionResult(err error) {
	if err == nil {
		c.accepted.Add(1)
		c.submittedTotal.Add(1)
		return
	}
	c.rejected.Add(1)
	if errors.Is(err, ErrQueueFull) {
		c.queueFullTotal.Add(1)
	}
}

// recordLaneAdmissionRejected increments Rejected without a shard enqueue attempt result.
func (c *laneCounters) recordLaneAdmissionRejected() {
	c.rejected.Add(1)
}
