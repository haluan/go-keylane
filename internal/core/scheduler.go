// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Scheduler manages the processing of jobs across shards and lanes.
type Scheduler struct {
	shards            []shard
	ReadyCh           chan int
	workerCount       int
	quotaPolicy       atomic.Pointer[quotaPolicySnapshot]
	quotaVersion      atomic.Uint64
	quotaMu           sync.Mutex
	admissionPolicy   atomic.Pointer[admissionPolicySnapshot]
	admissionVersion  atomic.Uint64
	admissionMu       sync.Mutex
	overloadPolicy    atomic.Pointer[overloadPolicySnapshot]
	overloadVersion   atomic.Uint64
	overloadMu        sync.Mutex
	laneReg           *LaneRegistry
	queueSizePerLane  int
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
	hotKeyCfg         HotKeyConfig
	hotKeyTrackers    []*hotKeyTracker
}

// NewScheduler creates a new Scheduler with the specified parameters.
func NewScheduler(shardCount, workerCount, queueSizePerLane int, reg *LaneRegistry) (*Scheduler, error) {
	shards := make([]shard, shardCount)
	laneCount := reg.Len()
	for i := 0; i < shardCount; i++ {
		shards[i] = newShard(laneCount, queueSizePerLane)
	}

	shardInflight := make([]atomic.Int64, shardCount)
	laneInflight := make([]atomic.Int64, laneCount)
	shardQueueWait := make([]queueWaitAccum, shardCount)
	shardRunDuration := make([]runDurationAccum, shardCount)

	s := &Scheduler{
		shards:           shards,
		ReadyCh:          make(chan int, shardCount),
		workerCount:      workerCount,
		laneReg:          reg,
		queueSizePerLane: queueSizePerLane,
		shardInflight:    shardInflight,
		laneInflight:     laneInflight,
		Obs:              defaultObservabilityConfig(),
		laneCounters:     make([]laneCounters, laneCount),
		shardQueueWait:   shardQueueWait,
		shardRunDuration: shardRunDuration,
	}
	s.initQuotaPolicy(reg)
	s.initAdmissionPolicy(reg, shardCount, queueSizePerLane)
	s.initOverloadPolicy(reg, shardCount, queueSizePerLane)
	return s, nil
}

// ConfigureHotKey applies hot key tracking configuration and preallocates per-shard trackers.
func (s *Scheduler) ConfigureHotKey(cfg HotKeyConfig) {
	s.hotKeyCfg = cfg
	if !cfg.Enabled || cfg.MaxTrackedKeysPerShard <= 0 {
		s.hotKeyTrackers = nil
		return
	}
	s.hotKeyTrackers = make([]*hotKeyTracker, len(s.shards))
	for i := range s.shards {
		s.hotKeyTrackers[i] = newHotKeyTracker(cfg)
	}
}

func (s *Scheduler) hotKeyTrackerForShard(shardID int) *hotKeyTracker {
	if s.hotKeyTrackers == nil || shardID < 0 || shardID >= len(s.hotKeyTrackers) {
		return nil
	}
	return s.hotKeyTrackers[shardID]
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
	hk := s.hotKeyTrackerForShard(shardID)
	if hk != nil {
		hk.observeSubmit(job.KeyHash, job.LaneID, job.RawKey, time.Now())
	}
	becameReady, err := enqueueIntoShard(&s.shards[shardID], job, s.Obs.EnableQueueWaitTiming, s.Obs.TrackQueueWait)
	if err == nil && hk != nil {
		hk.observeEnqueue(job.KeyHash, job.LaneID, job.RawKey, time.Now())
	}
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
	hk := s.hotKeyTrackerForShard(shardID)
	if hk != nil {
		hk.observeSubmit(job.KeyHash, job.LaneID, job.RawKey, time.Now())
	}
	becameReady, err := enqueueIntoShard(&s.shards[shardID], job, s.Obs.EnableQueueWaitTiming, s.Obs.TrackQueueWait)
	if err == nil && hk != nil {
		hk.observeEnqueue(job.KeyHash, job.LaneID, job.RawKey, time.Now())
	}
	if s.Obs.EnableCounters {
		c.recordLaneAdmissionResult(err)
	}
	return shardID, becameReady, err
}

// RecordHotKeyReject records a rejection for an existing hot key tracker slot.
func (s *Scheduler) RecordHotKeyReject(keyHash uint64, shardID int) {
	if hk := s.hotKeyTrackerForShard(shardID); hk != nil {
		hk.observeReject(keyHash, time.Now())
	}
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
			quota := s.loadQuotaPolicy().laneQuotas[j]
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
