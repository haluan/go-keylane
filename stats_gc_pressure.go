// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

// StatsGCPressureVersion is the schema version of StatsGCPressureSnapshot.
const StatsGCPressureVersion = "1"

// StatsGCPressureSnapshot is a read-only, best-effort snapshot of queue depth and
// in-flight pressure across shards and lanes. It is safe to read concurrently with
// submit and worker activity and intended for diagnostics and lightweight observability,
// not strict accounting.
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
