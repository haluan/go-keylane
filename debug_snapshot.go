// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "time"

// DebugSnapshotVersion is the schema version of DebugSnapshot.
const DebugSnapshotVersion = "1"

// TopHotShards is the maximum number of hot shards returned in a debug snapshot.
const TopHotShards = 5

// TopHotLanes is the maximum number of hot lanes returned in a debug snapshot.
const TopHotLanes = 5

// DebugSnapshot is a near-time diagnostic view of scheduler queue state.
// It is safe for concurrent reads while workers run, but does not guarantee a
// globally atomic view across all shards. It does not expose mutable internals.
type DebugSnapshot struct {
	Version string

	GeneratedAt time.Time

	// AdmissionPolicyVersion is the version of the active admission policy snapshot.
	AdmissionPolicyVersion uint64
	// OverloadPolicyVersion is the version of the active overload policy snapshot.
	OverloadPolicyVersion uint64

	ShardCount  int
	LaneCount   int
	WorkerCount int

	TotalDepth    uint64
	TotalCapacity uint64
	TotalInFlight uint64

	Pressure Pressure

	HotShards []HotShard
	HotLanes  []HotLane

	Shards []ShardSnapshot
	Lanes  []LaneSnapshot
}

// ShardSnapshot reports current queue depth and in-flight jobs for one shard.
type ShardSnapshot struct {
	ShardID uint32

	Depth    uint64
	Capacity uint64
	InFlight uint64

	DepthRatio float64

	LaneDepths []LaneDepthSnapshot

	HotKeyCandidate  *HotKeyCandidate
	HotKeyCandidates []HotKeyCandidate
}

// LaneSnapshot reports aggregated queue state for one lane across all shards.
type LaneSnapshot struct {
	LaneID uint16
	Name   string

	Depth    uint64
	Capacity uint64
	InFlight uint64

	DepthRatio float64

	Submitted uint64
	Completed uint64
	Failed    uint64
	QueueFull uint64

	QueueWaitNanosTotal uint64
	QueueWaitNanosMax   uint64
	RunNanosTotal       uint64
	RunNanosMax         uint64
}

// LaneDepthSnapshot reports queued depth for one lane within a single shard.
type LaneDepthSnapshot struct {
	LaneID uint16
	Name   string
	Depth  uint64
}

// HotShard identifies a shard with high queue depth relative to others.
type HotShard struct {
	ShardID uint32

	Depth      uint64
	Capacity   uint64
	InFlight   uint64
	DepthRatio float64
}

// HotLane identifies a lane with high aggregate queue depth relative to others.
type HotLane struct {
	LaneID uint16
	Name   string

	Depth      uint64
	Capacity   uint64
	InFlight   uint64
	DepthRatio float64
}
