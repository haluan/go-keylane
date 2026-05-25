// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

// ShardDepthRatio returns queued depth / capacity for one shard without a full debug snapshot.
func (s *Scheduler) ShardDepthRatio(shardID int) float64 {
	if shardID < 0 || shardID >= len(s.shards) {
		return 0
	}
	laneCount := len(s.shards[shardID].Lanes)
	if laneCount == 0 || s.queueSizePerLane <= 0 {
		return 0
	}
	capacity := uint64(laneCount * s.queueSizePerLane)
	sh := &s.shards[shardID]
	sh.mu.Lock()
	depth := uint64(sh.totalDepthLocked())
	sh.mu.Unlock()
	return safeRatio(depth, capacity)
}

// AdmissionClassForLane returns the lane class from the active admission policy snapshot.
func (s *Scheduler) AdmissionClassForLane(laneID LaneID) string {
	snap := s.loadAdmissionPolicy()
	if snap == nil {
		return LaneClassNormal
	}
	if int(laneID) < 0 || int(laneID) >= len(snap.lanes) {
		return snap.defaultClass
	}
	return snap.lanes[laneID].class
}

// LaneMaxQueueDepth returns the per-lane max queue depth from the admission policy snapshot.
func (s *Scheduler) LaneMaxQueueDepth(laneID LaneID) uint32 {
	snap := s.loadAdmissionPolicy()
	if snap == nil {
		return 1
	}
	if int(laneID) < 0 || int(laneID) >= len(snap.lanes) {
		return snap.defaultMaxQueueDepth
	}
	return snap.lanes[laneID].maxQueueDepth
}
