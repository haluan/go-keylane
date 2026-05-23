// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"sync"
	"sync/atomic"
)

// Scheduler manages the processing of jobs across shards and lanes.
type Scheduler struct {
	shards            []shard
	ReadyCh           chan int
	workerCount       int
	laneQuotas        []int // indexed by LaneID
	laneReg           *LaneRegistry
	mu                sync.RWMutex
	state             lifecycleState
	stopDone          chan struct{}
	workerCancel      context.CancelFunc
	workerWG          sync.WaitGroup
	inflight          atomic.Int64
	shardInflight     []atomic.Int64
	laneInflight      []atomic.Int64
	Obs               ObservabilityConfig
	laneCounters      []laneCounters
	queueWaitGlobal   queueWaitAccum
	shardQueueWait    []queueWaitAccum
	runDurationGlobal runDurationAccum
	shardRunDuration  []runDurationAccum
}

// NewScheduler creates a new Scheduler with the specified parameters.
func NewScheduler(shardCount, workerCount, queueSizePerLane int, reg *LaneRegistry) (*Scheduler, error) {
	shards := make([]shard, shardCount)
	laneCount := reg.Len()
	for i := 0; i < shardCount; i++ {
		shards[i] = newShard(laneCount, queueSizePerLane)
	}

	quotas := make([]int, laneCount)
	for i := 0; i < laneCount; i++ {
		quotas[i] = reg.Quota(LaneID(i))
	}

	shardInflight := make([]atomic.Int64, shardCount)
	laneInflight := make([]atomic.Int64, laneCount)
	shardQueueWait := make([]queueWaitAccum, shardCount)
	shardRunDuration := make([]runDurationAccum, shardCount)

	return &Scheduler{
		shards:           shards,
		ReadyCh:          make(chan int, shardCount),
		workerCount:      workerCount,
		laneQuotas:       quotas,
		laneReg:          reg,
		shardInflight:    shardInflight,
		laneInflight:     laneInflight,
		Obs:              defaultObservabilityConfig(),
		laneCounters:     make([]laneCounters, laneCount),
		shardQueueWait:   shardQueueWait,
		shardRunDuration: shardRunDuration,
	}, nil
}

// Start launches the worker goroutines.
func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.state == stateRunning {
		s.mu.Unlock()
		return ErrQueueAlreadyStarted
	}
	if s.state != stateNew {
		s.mu.Unlock()
		return ErrStopped
	}
	s.state = stateRunning
	workerCtx, workerCancel := context.WithCancel(ctx)
	s.workerCancel = workerCancel
	for i := 0; i < s.workerCount; i++ {
		s.workerWG.Add(1)
		go func() {
			defer s.workerWG.Done()
			s.WorkerLoop(workerCtx)
		}()
	}
	s.mu.Unlock()
	return nil
}

// Enqueue routes the job to the correct shard and enqueues it.
func (s *Scheduler) Enqueue(job InternalJob) (int, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	c := &s.laneCounters[job.LaneID]
	if s.Obs.EnableCounters {
		c.recordLaneAdmissionAttempt()
	}
	if s.state == stateStopping || s.state == stateStopped {
		if s.Obs.EnableCounters {
			c.recordLaneAdmissionRejected()
		}
		return 0, false, ErrStopped
	}

	shardID := routeShardID(job.KeyHash, len(s.shards))
	becameReady, err := enqueueIntoShard(&s.shards[shardID], job, s.Obs.EnableQueueWaitTiming, s.Obs.TrackQueueWait)
	if s.Obs.EnableCounters {
		c.recordLaneAdmissionResult(err)
	}
	return shardID, becameReady, err
}

// TryEnqueue routes the job to the correct shard and enqueues it if possible.
func (s *Scheduler) TryEnqueue(job InternalJob) (int, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	c := &s.laneCounters[job.LaneID]
	if s.Obs.EnableCounters {
		c.recordLaneAdmissionAttempt()
	}
	if s.state == stateNew {
		if s.Obs.EnableCounters {
			c.recordLaneAdmissionRejected()
		}
		return 0, false, ErrNotStarted
	}
	if s.state != stateRunning {
		if s.Obs.EnableCounters {
			c.recordLaneAdmissionRejected()
		}
		return 0, false, ErrStopped
	}

	shardID := routeShardID(job.KeyHash, len(s.shards))
	becameReady, err := enqueueIntoShard(&s.shards[shardID], job, s.Obs.EnableQueueWaitTiming, s.Obs.TrackQueueWait)
	if s.Obs.EnableCounters {
		c.recordLaneAdmissionResult(err)
	}
	return shardID, becameReady, err
}

// RecordPressureAdmissionRejected increments the lane rejected counter for a
// pressure-based admission rejection before enqueue.
func (s *Scheduler) RecordPressureAdmissionRejected(laneID LaneID) {
	if !s.Obs.EnableCounters {
		return
	}
	if int(laneID) < 0 || int(laneID) >= len(s.laneCounters) {
		return
	}
	s.laneCounters[laneID].recordPressureAdmissionRejected()
}

// Stats returns a snapshot of the scheduler's stats.
func (s *Scheduler) Stats() ([]ShardStats, int) {
	shardCount := len(s.shards)
	shards := make([]ShardStats, shardCount)
	totalDepth := 0

	for i := 0; i < shardCount; i++ {
		shard := &s.shards[i]
		shard.mu.Lock()

		ready := shard.Ready
		shardDepth := 0
		laneCount := len(shard.Lanes)
		lanes := make([]LaneStats, laneCount)

		for j := 0; j < laneCount; j++ {
			laneID := LaneID(j)
			depth := shard.Lanes[j].depth()
			capacity := shard.Lanes[j].capacity()
			quota := s.laneQuotas[j]
			laneName := s.laneReg.Name(laneID)

			counters := &s.laneCounters[j]

			lanes[j] = LaneStats{
				LaneName:            laneName,
				Depth:               depth,
				Capacity:            capacity,
				Quota:               quota,
				SubmittedTotal:      counters.submittedTotal.Load(),
				CompletedTotal:      counters.completedTotal.Load(),
				FailedTotal:         counters.failedTotal.Load(),
				QueueFullTotal:      counters.queueFullTotal.Load(),
				QueueWaitTotalNanos: counters.queueWaitTotalNanos.Load(),
				QueueWaitCount:      counters.queueWaitCount.Load(),
			}
			shardDepth += depth
		}

		shard.mu.Unlock()

		shards[i] = ShardStats{
			ShardID:    i,
			Ready:      ready,
			TotalDepth: shardDepth,
			Lanes:      lanes,
		}
		totalDepth += shardDepth
	}

	return shards, totalDepth
}
