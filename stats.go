package keylane

import "time"

// Stats represents a snapshot of the queue's internal state.
type Stats struct {
	ShardCount  int
	WorkerCount int
	TotalDepth  int
	Shards      []ShardStats
}

// ShardStats represents a snapshot of a single shard.
type ShardStats struct {
	ShardID    int
	Ready      bool
	TotalDepth int
	Lanes      []LaneStats
}

// LaneStats represents a snapshot of a single lane's queue within a shard.
type LaneStats struct {
	Lane                Lane
	Depth               int
	Capacity            int
	Quota               int
	SubmittedTotal      int64
	CompletedTotal      int64
	FailedTotal         int64
	QueueFullTotal      int64
	QueueWaitTotalNanos int64
	QueueWaitCount      int64
}

// AverageQueueWait computes the average duration a job in this lane has spent waiting in the queue.
func (s LaneStats) AverageQueueWait() time.Duration {
	if s.QueueWaitCount == 0 {
		return 0
	}
	return time.Duration(s.QueueWaitTotalNanos / s.QueueWaitCount)
}
