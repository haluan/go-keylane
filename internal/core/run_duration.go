// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "sync/atomic"

// runDurationAccum holds cumulative run-duration samples for StatsGCPressure.
type runDurationAccum struct {
	count      atomic.Uint64
	totalNanos atomic.Uint64
	maxNanos   atomic.Uint64
}

func (a *runDurationAccum) record(runNanos uint64) {
	a.count.Add(1)
	a.totalNanos.Add(runNanos)
	atomicMaxUint64(&a.maxNanos, runNanos)
}

func (a *runDurationAccum) snapshot() RunStatsGCPressure {
	return RunStatsGCPressure{
		Count:      a.count.Load(),
		TotalNanos: a.totalNanos.Load(),
		MaxNanos:   a.maxNanos.Load(),
	}
}

// recordGCPressureRunDuration records one run-duration sample for StatsGCPressure (always on).
func (s *Scheduler) recordGCPressureRunDuration(shardID int, laneID LaneID, runNanos uint64) {
	s.runDurationGlobal.record(runNanos)
	if shardID >= 0 && shardID < len(s.shardRunDuration) {
		s.shardRunDuration[shardID].record(runNanos)
	}
	if int(laneID) >= 0 && int(laneID) < len(s.laneCounters) {
		c := &s.laneCounters[laneID]
		c.gcRunCount.Add(1)
		c.gcRunTotalNanos.Add(runNanos)
		atomicMaxUint64(&c.gcRunMaxNanos, runNanos)
	}
}
