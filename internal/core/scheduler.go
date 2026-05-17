package core

import (
	"context"
	"sync"
	"sync/atomic"
)

// Scheduler manages the processing of jobs across shards and lanes.
type Scheduler struct {
	shards       []shard
	ReadyCh      chan int
	workerCount  int
	laneQuotas   []int // indexed by LaneID
	laneReg      *LaneRegistry
	mu           sync.RWMutex
	state        lifecycleState
	stopDone     chan struct{}
	workerCancel context.CancelFunc
	workerWG     sync.WaitGroup
	inflight     atomic.Int64
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

	return &Scheduler{
		shards:      shards,
		ReadyCh:     make(chan int, shardCount),
		workerCount: workerCount,
		laneQuotas:  quotas,
		laneReg:     reg,
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
	if s.state == stateStopping || s.state == stateStopped {
		return 0, false, ErrStopped
	}

	shardID := routeShardID(job.KeyHash, len(s.shards))
	becameReady, err := enqueueIntoShard(&s.shards[shardID], job)
	return shardID, becameReady, err
}

// TryEnqueue routes the job to the correct shard and enqueues it if possible.
func (s *Scheduler) TryEnqueue(job InternalJob) (int, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.state == stateNew {
		return 0, false, ErrNotStarted
	}
	if s.state != stateRunning {
		return 0, false, ErrStopped
	}

	shardID := routeShardID(job.KeyHash, len(s.shards))
	becameReady, err := enqueueIntoShard(&s.shards[shardID], job)
	return shardID, becameReady, err
}
