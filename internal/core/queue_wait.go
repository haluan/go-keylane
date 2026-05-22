// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "sync/atomic"

// queueWaitAccum holds cumulative queue-wait samples for StatsGCPressure.
type queueWaitAccum struct {
	count      atomic.Uint64
	totalNanos atomic.Uint64
	maxNanos   atomic.Uint64
}

func (a *queueWaitAccum) record(waitNanos uint64) {
	a.count.Add(1)
	a.totalNanos.Add(waitNanos)
	atomicMaxUint64(&a.maxNanos, waitNanos)
}

func (a *queueWaitAccum) snapshot() QueueWaitStatsGCPressure {
	return QueueWaitStatsGCPressure{
		Count:      a.count.Load(),
		TotalNanos: a.totalNanos.Load(),
		MaxNanos:   a.maxNanos.Load(),
	}
}

func atomicMaxUint64(target *atomic.Uint64, value uint64) {
	for {
		old := target.Load()
		if value <= old {
			return
		}
		if target.CompareAndSwap(old, value) {
			return
		}
	}
}

// recordGCPressureQueueWait records one queue-wait sample for StatsGCPressure (always on).
func (s *Scheduler) recordGCPressureQueueWait(shardID int, laneID LaneID, waitNanos uint64) {
	s.queueWaitGlobal.record(waitNanos)
	if shardID >= 0 && shardID < len(s.shardQueueWait) {
		s.shardQueueWait[shardID].record(waitNanos)
	}
	if int(laneID) >= 0 && int(laneID) < len(s.laneCounters) {
		c := &s.laneCounters[laneID]
		c.gcQueueWaitCount.Add(1)
		c.gcQueueWaitTotalNanos.Add(waitNanos)
		atomicMaxUint64(&c.gcQueueWaitMaxNanos, waitNanos)
	}
}
