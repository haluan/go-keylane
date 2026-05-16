package core

import (
	"sync"
)

type shard struct {
	mu    sync.Mutex
	ready bool
	lanes []laneQueue
}

func newShard(laneCount int, queueSizePerLane int) shard {
	lanes := make([]laneQueue, laneCount)
	for i := 0; i < laneCount; i++ {
		lanes[i] = newLaneQueue(queueSizePerLane)
	}
	return shard{
		lanes: lanes,
	}
}

func (s *shard) totalDepthLocked() int {
	total := 0
	for i := range s.lanes {
		total += s.lanes[i].depth()
	}
	return total
}

func (s *shard) hasWorkLocked() bool {
	for i := range s.lanes {
		if !s.lanes[i].isEmpty() {
			return true
		}
	}
	return false
}

func (s *shard) laneDepthLocked(id LaneID) int {
	if int(id) >= len(s.lanes) {
		return 0
	}
	return s.lanes[int(id)].depth()
}
