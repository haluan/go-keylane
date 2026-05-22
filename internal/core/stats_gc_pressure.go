// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

// StatsGCPressureVersion is the schema version of StatsGCPressureSnapshot.
const StatsGCPressureVersion = "1"

// StatsGCPressureSnapshot is a read-only, best-effort snapshot of scheduler queue
// depth and in-flight pressure. It is safe to read concurrently with scheduler
// activity and intended for diagnostics and lightweight observability, not strict
// accounting. Totals are derived from per-shard values collected sequentially and
// may briefly disagree with a later re-read under heavy concurrency.
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

// LaneStatsGCPressure reports aggregated queued depth, in-flight jobs, and capacity
// for one lane across all shards.
type LaneStatsGCPressure struct {
	LaneID   uint16
	Name     string
	Queued   uint64
	InFlight uint64
	Capacity uint64
}

// LaneDepthGCPressure reports queued depth for one lane within a single shard.
type LaneDepthGCPressure struct {
	LaneID uint16
	Queued uint64
}

// StatsGCPressure returns a read-only best-effort snapshot of scheduler GC pressure
// state: queue depths, in-flight jobs, worker/shard/lane configuration, and capacity.
// The snapshot is safe to read concurrently with submit and worker activity. Individual
// fields are collected under per-shard locks or atomic loads; totals are summed from
// those per-shard copies and may be briefly inconsistent across shards under heavy load.
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
