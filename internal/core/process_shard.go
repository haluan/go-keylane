package core

import (
	"context"
)

// processShard processes jobs from all lanes of a single shard.
func (s *Scheduler) processShard(ctx context.Context, shardID int) {
	shard := &s.shards[shardID]
	shard.mu.Lock()

	// 1. Determine total possible batch size to pre-allocate
	totalQuota := 0
	for _, q := range s.laneQuotas {
		totalQuota += q
	}

	// 2. Collect batch of jobs across all lanes according to quotas
	batch := make([]InternalJob, 0, totalQuota)
	for laneID, quota := range s.laneQuotas {
		batch = shard.Lanes[laneID].popN(quota, batch)
	}

	// 3. Check if shard still has work
	hasMore := shard.hasWorkLocked()
	if !hasMore {
		shard.Ready = false
	}
	shard.mu.Unlock()

	// 4. Run jobs outside of shard lock
	for _, job := range batch {
		if err := ctx.Err(); err != nil {
			break
		}
		// Run the job function. Errors are ignored as per fire-and-forget semantics.
		_ = job.Run(ctx)
	}

	// 5. Requeue if more work remains
	if hasMore {
		select {
		case s.ReadyCh <- shardID:
		case <-ctx.Done():
			// Context cancelled, worker will exit.
		}
	}
}
