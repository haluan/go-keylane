// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

// StatsGCPressureVersion is the schema version of StatsGCPressureSnapshot.
const StatsGCPressureVersion = "2"

// StatsGCPressureSnapshot is a read-only, best-effort snapshot of scheduler queue
// depth, in-flight pressure, and cumulative per-lane counters. It is safe to read
// concurrently with scheduler activity and intended for diagnostics and lightweight
// observability, not strict accounting. Totals are derived from per-shard values
// collected sequentially and may briefly disagree with a later re-read under heavy
// concurrency.
type StatsGCPressureSnapshot struct {
	Version string

	ShardCount  int
	LaneCount   int
	WorkerCount int

	TotalQueued   uint64
	TotalInFlight uint64

	Shards []ShardStatsGCPressure
	Lanes  []LaneStatsGCPressure
}

// ShardStatsGCPressure reports queued depth, in-flight jobs, and capacity for one shard.
type ShardStatsGCPressure struct {
	ShardID  uint32
	Queued   uint64
	InFlight uint64
	Capacity uint64
	PerLane  []LaneDepthGCPressure
}

// LaneCountersGCPressure holds cumulative per-lane counters since scheduler start.
// Values are best-effort under concurrent updates and intended for diagnostics, not
// durable or exactly-once accounting.
type LaneCountersGCPressure struct {
	// Submitted counts every enqueue attempt to this lane after the lane ID is known,
	// before admission succeeds or fails. Answers: how much traffic targets this lane?
	Submitted uint64
	// Accepted counts jobs successfully admitted into the lane queue. Answers: how much
	// traffic did the scheduler accept for this lane?
	Accepted uint64
	// Rejected counts enqueue attempts not accepted, including queue-full and scheduler
	// stopped/not-started admission failures. Answers: how often does this lane reject work?
	Rejected uint64
	// Completed counts accepted jobs that finished with a nil error. Answers: how much
	// work completed normally for this lane?
	Completed uint64
	// Failed counts accepted jobs that returned a non-nil error other than context.Canceled.
	// Answers: are failures concentrated in this lane?
	Failed uint64
	// QueueFull counts rejections because the lane queue reached its bounded capacity.
	// Each queue-full rejection also increments Rejected. Answers: is this lane under
	// capacity pressure?
	QueueFull uint64
	// Canceled counts accepted jobs canceled before or during execution, including jobs
	// that return context.Canceled and jobs skipped when a worker context is canceled.
	// Answers: how often is work for this lane canceled?
	Canceled uint64
	// Panicked counts jobs recovered from a user panic. Panic recovery is not implemented;
	// this field is always zero.
	Panicked uint64
}

// LaneStatsGCPressure reports aggregated queued depth, in-flight jobs, capacity, and
// cumulative counters for one lane across all shards.
type LaneStatsGCPressure struct {
	LaneID   uint16
	Name     string
	Queued   uint64
	InFlight uint64
	Capacity uint64
	// Counters holds cumulative per-lane admission and terminal-outcome totals since
	// scheduler start. See LaneCountersGCPressure for per-field semantics.
	Counters LaneCountersGCPressure
}

// LaneDepthGCPressure reports queued depth for one lane within a single shard.
type LaneDepthGCPressure struct {
	LaneID uint16
	Queued uint64
}

// StatsGCPressure returns a read-only best-effort snapshot of scheduler GC pressure
// state: queue depths, in-flight jobs, worker/shard/lane configuration, capacity, and
// cumulative per-lane counters in Lanes[].Counters. The snapshot is safe to read
// concurrently with submit and worker activity. Individual fields are collected under
// per-shard locks or atomic loads; totals are summed from those per-shard copies and
// may be briefly inconsistent across shards under heavy load. Counter values are
// cumulative since scheduler start and are not strict accounting records.
func (s *Scheduler) StatsGCPressure() StatsGCPressureSnapshot {
	shardCount := len(s.shards)
	laneCount := s.laneReg.Len()

	shards := make([]ShardStatsGCPressure, shardCount)
	laneQueued := make([]uint64, laneCount)
	laneCapacity := make([]uint64, laneCount)

	var totalQueued, totalInFlight uint64

	for i := 0; i < shardCount; i++ {
		shard := &s.shards[i]
		shard.mu.Lock()

		laneCountLocal := len(shard.Lanes)
		perLane := make([]LaneDepthGCPressure, laneCountLocal)
		var shardQueued, shardCapacity uint64

		for j := 0; j < laneCountLocal; j++ {
			laneID := LaneID(j)
			depth := uint64(shard.Lanes[j].depth())
			capacity := uint64(shard.Lanes[j].capacity())

			perLane[j] = LaneDepthGCPressure{
				LaneID: uint16(laneID),
				Queued: depth,
			}
			shardQueued += depth
			shardCapacity += capacity
			if int(laneID) < laneCount {
				laneQueued[laneID] += depth
				laneCapacity[laneID] += capacity
			}
		}

		shard.mu.Unlock()

		shardInflight := uint64(s.shardInflight[i].Load())
		totalQueued += shardQueued
		totalInFlight += shardInflight

		shards[i] = ShardStatsGCPressure{
			ShardID:  uint32(i),
			Queued:   shardQueued,
			InFlight: shardInflight,
			Capacity: shardCapacity,
			PerLane:  perLane,
		}
	}

	lanes := make([]LaneStatsGCPressure, laneCount)
	for i := 0; i < laneCount; i++ {
		laneID := LaneID(i)
		lanes[i] = LaneStatsGCPressure{
			LaneID:   uint16(laneID),
			Name:     s.laneReg.Name(laneID),
			Queued:   laneQueued[i],
			InFlight: uint64(s.laneInflight[i].Load()),
			Capacity: laneCapacity[i],
			Counters: s.laneCounters[i].snapshotGCPressure(),
		}
	}

	return StatsGCPressureSnapshot{
		Version:       StatsGCPressureVersion,
		ShardCount:    shardCount,
		LaneCount:     laneCount,
		WorkerCount:   s.workerCount,
		TotalQueued:   totalQueued,
		TotalInFlight: totalInFlight,
		Shards:        shards,
		Lanes:         lanes,
	}
}
