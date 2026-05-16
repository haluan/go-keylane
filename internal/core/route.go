package core

// routeShardID deterministically maps a key hash to a shard ID.
func routeShardID(keyHash uint64, shardCount int) int {
	return int(keyHash % uint64(shardCount))
}

// routeJob deterministically maps an internal job to a shard ID.
func routeJob(job InternalJob, shardCount int) int {
	return routeShardID(job.KeyHash, shardCount)
}
