package core

// ShardStats represents a snapshot of a single shard's internal state.
type ShardStats struct {
	ShardID    int
	Ready      bool
	TotalDepth int
	Lanes      []LaneStats
}

// LaneStats represents a snapshot of a single lane queue's metrics and state inside a shard.
type LaneStats struct {
	LaneName            string
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
