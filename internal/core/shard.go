package core

import (
	"sync"
)

type shard struct {
	mu    sync.Mutex
	Ready bool
	Lanes []laneQueue
}

func newShard(laneCount int, queueSizePerLane int) shard {
	lanes := make([]laneQueue, laneCount)
	for i := 0; i < laneCount; i++ {
		lanes[i] = newLaneQueue(queueSizePerLane)
	}
	return shard{
		Lanes: lanes,
	}
}

func (s *shard) totalDepthLocked() int {
	total := 0
	for i := range s.Lanes {
		total += s.Lanes[i].depth()
	}
	return total
}

func (s *shard) hasWorkLocked() bool {
	for i := range s.Lanes {
		if !s.Lanes[i].isEmpty() {
			return true
		}
	}
	return false
}

func (s *shard) laneDepthLocked(laneID LaneID) int {
	if int(laneID) >= len(s.Lanes) {
		return 0
	}
	return s.Lanes[int(laneID)].depth()
}
