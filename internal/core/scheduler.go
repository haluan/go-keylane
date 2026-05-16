package core

// Scheduler manages the processing of jobs across shards and lanes.
type Scheduler struct {
	shards      []shard
	ReadyCh     chan int
	workerCount int
	laneQuotas  []int         // indexed by LaneID
	laneReg     *LaneRegistry
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

// Enqueue routes the job to the correct shard and enqueues it.
// It returns the shardID and becameReady=true if the shard transitioned to Ready.
func (s *Scheduler) Enqueue(job InternalJob) (int, bool, error) {
	shardID := routeShardID(job.KeyHash, len(s.shards))
	becameReady, err := enqueueIntoShard(&s.shards[shardID], job)
	return shardID, becameReady, err
}

