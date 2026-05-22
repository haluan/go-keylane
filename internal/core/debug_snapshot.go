// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "time"

// DebugSnapshotVersion is the schema version of DebugSnapshot.
const DebugSnapshotVersion = "1"

// HotShard identifies a shard with high queue depth relative to others.
type HotShard struct {
	ShardID    uint32
	Depth      uint64
	Capacity   uint64
	InFlight   uint64
	DepthRatio float64
}

// HotLane identifies a lane with high aggregate queue depth relative to others.
type HotLane struct {
	LaneID     uint16
	Name       string
	Depth      uint64
	Capacity   uint64
	InFlight   uint64
	DepthRatio float64
}

// LaneDepthSnapshot reports queued depth for one lane within a single shard.
type LaneDepthSnapshot struct {
	LaneID uint16
	Name   string
	Depth  uint64
}

// ShardSnapshot reports current queue depth and in-flight jobs for one shard.
type ShardSnapshot struct {
	ShardID    uint32
	Depth      uint64
	Capacity   uint64
	InFlight   uint64
	DepthRatio float64
	LaneDepths []LaneDepthSnapshot
}

// LaneSnapshot reports aggregated queue state for one lane across all shards.
type LaneSnapshot struct {
	LaneID     uint16
	Name       string
	Depth      uint64
	Capacity   uint64
	InFlight   uint64
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

// DebugSnapshot is a near-time diagnostic view of scheduler queue state.
type DebugSnapshot struct {
	Version string

	GeneratedAt time.Time

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

// DebugSnapshot returns a near-time diagnostic snapshot of scheduler queue state.
// It is safe to read concurrently with submit and worker activity but does not
// guarantee a globally atomic view across shards.
func (s *Scheduler) DebugSnapshot() DebugSnapshot {
	view := s.collectSchedulerDebugView()
	totalDepth, totalCapacity, totalInFlight := debugViewTotals(view)

	shards := make([]ShardSnapshot, len(view.shards))
	for i, sh := range view.shards {
		laneDepths := make([]LaneDepthSnapshot, len(sh.laneDeps))
		for j, ld := range sh.laneDeps {
			laneDepths[j] = LaneDepthSnapshot{
				LaneID: uint16(ld.laneID),
				Name:   s.laneReg.Name(ld.laneID),
				Depth:  ld.depth,
			}
		}
		shards[i] = ShardSnapshot{
			ShardID:    sh.shardID,
			Depth:      sh.depth,
			Capacity:   sh.capacity,
			InFlight:   sh.inFlight,
			DepthRatio: safeRatio(sh.depth, sh.capacity),
			LaneDepths: laneDepths,
		}
	}

	lanes := make([]LaneSnapshot, len(view.lanes))
	for i, ln := range view.lanes {
		lanes[i] = LaneSnapshot{
			LaneID:              uint16(ln.laneID),
			Name:                ln.name,
			Depth:               ln.depth,
			Capacity:            ln.capacity,
			InFlight:            ln.inFlight,
			DepthRatio:          safeRatio(ln.depth, ln.capacity),
			Submitted:           ln.submitted,
			Completed:           ln.completed,
			Failed:              ln.failed,
			QueueFull:           ln.queueFull,
			QueueWaitNanosTotal: ln.queueWaitTotal,
			QueueWaitNanosMax:   ln.queueWaitMax,
			RunNanosTotal:       ln.runTotal,
			RunNanosMax:         ln.runMax,
		}
	}

	return DebugSnapshot{
		Version:       DebugSnapshotVersion,
		GeneratedAt:   time.Now(),
		ShardCount:    view.shardCount,
		LaneCount:     view.laneCount,
		WorkerCount:   view.workerCount,
		TotalDepth:    totalDepth,
		TotalCapacity: totalCapacity,
		TotalInFlight: totalInFlight,
		Pressure:      classifyPressure(totalDepth, totalCapacity, totalInFlight),
		HotShards:     rankHotShards(view.shards, topHotShards),
		HotLanes:      rankHotLanes(view.lanes, topHotLanes),
		Shards:        shards,
		Lanes:         lanes,
	}
}
