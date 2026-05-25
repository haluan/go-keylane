// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"github.com/haluan/go-keylane/internal/core"
)

// RetrySuppressionSnapshot captures cheap runtime state for retry suppression on the queue.
func (q *Queue) RetrySuppressionSnapshot(key string, lane Lane, shardID int) RetrySuppressionSnapshot {
	if q == nil {
		return RetrySuppressionSnapshot{}
	}
	snap := RetrySuppressionSnapshot{Pressure: q.Pressure()}
	if q.config.AutoscalingSignal.Enabled {
		snap.ScaleSignal = q.ScaleSignal()
	}
	laneID, ok := q.reg.Lookup(string(lane))
	if !ok {
		return snap
	}
	depth := q.sched.LaneQueueDepth(laneID)
	maxDepth := q.sched.LaneMaxQueueDepth(laneID)
	if maxDepth > 0 {
		snap.LaneDepthRatio = float64(depth) / float64(maxDepth)
	}
	snap.ShardDepthRatio = q.sched.ShardDepthRatio(shardID)
	snap.LaneClass = LaneClass(q.sched.AdmissionClassForLane(laneID))
	if err := snap.LaneClass.Validate(); err != nil {
		snap.LaneClass = LaneNormal
	}
	keyHash := core.HashKey(key)
	if q.config.HotKey.Enabled {
		status := q.sched.HotKeyStatusForKey(shardID, keyHash)
		snap.HotKeyCandidate = isHotKeyStatusForSuppression(status)
	}
	if q.perKeyAdmissionEnabled {
		dec := q.sched.ObservePerKeyAdmissionForSuppression(shardID, keyHash, laneID, q.perKeyAdmissionCore)
		if isHotKeyStatusForSuppression(dec.HotKeyStatus) || dec.Action != core.PerKeyMitigationAllow {
			snap.HotKeyCandidate = true
		}
	}
	return snap
}

func isHotKeyStatusForSuppression(status core.HotKeyStatus) bool {
	return status == core.HotKeyStatusCandidate || status == core.HotKeyStatusDominant
}
