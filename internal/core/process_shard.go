package core

import (
	"context"
	"time"
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
	var batch []InternalJob
	if s.Obs.DisablePooling {
		batch = make([]InternalJob, 0, totalQuota)
		for laneID, quota := range s.laneQuotas {
			batch = shard.Lanes[laneID].popN(quota, batch)
		}
	} else {
		batchObj := acquireJobBatch(totalQuota)
		defer releaseJobBatch(batchObj)
		for laneID, quota := range s.laneQuotas {
			batchObj.jobs = shard.Lanes[laneID].popN(quota, batchObj.jobs)
		}
		batch = batchObj.jobs
	}

	// 3. Check if shard still has work
	hasMore := shard.hasWorkLocked()
	if !hasMore {
		shard.Ready = false
	}
	s.inflight.Add(int64(len(batch)))
	shard.mu.Unlock()

	// 4. Run jobs outside of shard lock
	for i, job := range batch {
		if err := ctx.Err(); err != nil {
			s.inflight.Add(-int64(len(batch) - i))
			break
		}
		if s.Obs.TrackQueueWait && job.EnqueuedAt > 0 {
			waitNanos := time.Now().UnixNano() - job.EnqueuedAt
			s.laneCounters[job.LaneID].queueWaitTotalNanos.Add(waitNanos)
			s.laneCounters[job.LaneID].queueWaitCount.Add(1)
		}
		func() {
			defer s.inflight.Add(-1)

			var start time.Time
			if s.Obs.SlowJobThreshold > 0 {
				start = time.Now()
			}

			err := job.Run(ctx)

			if err == nil {
				s.laneCounters[job.LaneID].completedTotal.Add(1)
			} else {
				s.laneCounters[job.LaneID].failedTotal.Add(1)
			}

			if s.Obs.SlowJobThreshold > 0 && s.Obs.OnSlowJob != nil {
				dur := time.Since(start)
				if dur >= s.Obs.SlowJobThreshold {
					laneName := s.laneReg.Name(job.LaneID)
					s.Obs.OnSlowJob(laneName, shardID, dur)
				}
			}
		}()
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
