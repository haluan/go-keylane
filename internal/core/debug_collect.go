// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

type laneDepthInShard struct {
	laneID LaneID
	depth  uint64
}

type shardDebugView struct {
	shardID  uint32
	depth    uint64
	capacity uint64
	inFlight uint64
	laneDeps []laneDepthInShard
}

type laneDebugView struct {
	laneID   LaneID
	name     string
	depth    uint64
	capacity uint64
	inFlight uint64

	submitted uint64
	completed uint64
	failed    uint64
	queueFull uint64

	queueWaitTotal uint64
	queueWaitMax   uint64
	runTotal       uint64
	runMax         uint64
}

type schedulerDebugView struct {
	shardCount  int
	laneCount   int
	workerCount int
	shards      []shardDebugView
	lanes       []laneDebugView
}

func (s *Scheduler) collectSchedulerDebugView() schedulerDebugView {
	shardCount := len(s.shards)
	laneCount := s.laneReg.Len()

	laneDepth := make([]uint64, laneCount)
	laneCapacity := make([]uint64, laneCount)

	shards := make([]shardDebugView, shardCount)

	for i := 0; i < shardCount; i++ {
		shard := &s.shards[i]
		shard.mu.Lock()

		laneCountLocal := len(shard.Lanes)
		laneDeps := make([]laneDepthInShard, laneCountLocal)
		var shardDepth, shardCapacity uint64

		for j := 0; j < laneCountLocal; j++ {
			laneID := LaneID(j)
			depth := uint64(shard.Lanes[j].depth())
			capacity := uint64(shard.Lanes[j].capacity())

			laneDeps[j] = laneDepthInShard{laneID: laneID, depth: depth}
			shardDepth += depth
			shardCapacity += capacity
			if int(laneID) < laneCount {
				laneDepth[laneID] += depth
				laneCapacity[laneID] += capacity
			}
		}

		shard.mu.Unlock()

		shards[i] = shardDebugView{
			shardID:  uint32(i),
			depth:    shardDepth,
			capacity: shardCapacity,
			inFlight: uint64(s.shardInflight[i].Load()),
			laneDeps: laneDeps,
		}
	}

	lanes := make([]laneDebugView, laneCount)
	for i := 0; i < laneCount; i++ {
		laneID := LaneID(i)
		counters := s.laneCounters[i].snapshotGCPressure()
		qw := s.laneCounters[i].snapshotGCPressureQueueWait()
		run := s.laneCounters[i].snapshotGCPressureRun()
		lanes[i] = laneDebugView{
			laneID:         laneID,
			name:           s.laneReg.Name(laneID),
			depth:          laneDepth[i],
			capacity:       laneCapacity[i],
			inFlight:       uint64(s.laneInflight[i].Load()),
			submitted:      counters.Submitted,
			completed:      counters.Completed,
			failed:         counters.Failed,
			queueFull:      counters.QueueFull,
			queueWaitTotal: qw.TotalNanos,
			queueWaitMax:   qw.MaxNanos,
			runTotal:       run.TotalNanos,
			runMax:         run.MaxNanos,
		}
	}

	return schedulerDebugView{
		shardCount:  shardCount,
		laneCount:   laneCount,
		workerCount: s.workerCount,
		shards:      shards,
		lanes:       lanes,
	}
}

func debugViewTotals(v schedulerDebugView) (depth, capacity, inFlight uint64) {
	for _, sh := range v.shards {
		depth += sh.depth
		capacity += sh.capacity
		inFlight += sh.inFlight
	}
	return depth, capacity, inFlight
}
