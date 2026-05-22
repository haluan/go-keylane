// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"errors"
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
	batchLen := len(batch)
	if batchLen > 0 {
		s.inflight.Add(int64(batchLen))
		s.shardInflight[shardID].Add(int64(batchLen))
		for _, job := range batch {
			s.laneInflight[job.LaneID].Add(1)
		}
	}
	shard.mu.Unlock()

	// 4. Run jobs outside of shard lock
	for i, job := range batch {
		if err := ctx.Err(); err != nil {
			remaining := batchLen - i
			if remaining > 0 {
				s.inflight.Add(-int64(remaining))
				s.shardInflight[shardID].Add(-int64(remaining))
				for j := i; j < batchLen; j++ {
					laneID := batch[j].LaneID
					s.laneInflight[laneID].Add(-1)
					if s.Obs.EnableCounters {
						s.laneCounters[laneID].canceled.Add(1)
					}
				}
			}
			break
		}
		func() {
			defer func() {
				s.inflight.Add(-1)
				s.shardInflight[shardID].Add(-1)
				s.laneInflight[job.LaneID].Add(-1)
			}()

			needQueueWait, needRunDuration := s.jobNeedsWorkerTimestamps(job)
			var (
				startedAt   time.Time
				queueWait   time.Duration
				runDuration time.Duration
			)
			if needQueueWait || needRunDuration {
				startedAt = time.Now()
				if s.Obs.EnableQueueWaitTiming && !job.AcceptedAt.IsZero() {
					queueWait = startedAt.Sub(job.AcceptedAt)
					s.recordGCPressureQueueWait(shardID, job.LaneID, uint64(queueWait.Nanoseconds()))
				}
				if s.Obs.TrackQueueWait && !job.EnqueuedAt.IsZero() {
					waitNanos := startedAt.Sub(job.EnqueuedAt).Nanoseconds()
					s.laneCounters[job.LaneID].queueWaitTotalNanos.Add(waitNanos)
					s.laneCounters[job.LaneID].queueWaitCount.Add(1)
				}
			}

			err := job.Run(ctx)
			if needRunDuration {
				runDuration = time.Since(startedAt)
				if s.Obs.EnableRunTiming {
					s.recordGCPressureRunDuration(shardID, job.LaneID, uint64(runDuration.Nanoseconds()))
				}
			}

			if s.Obs.EnableCounters {
				counters := &s.laneCounters[job.LaneID]
				if err == nil {
					counters.completedTotal.Add(1)
				} else if errors.Is(err, context.Canceled) {
					counters.canceled.Add(1)
				} else {
					counters.failedTotal.Add(1)
				}
			}

			s.emitObservabilityHooks(shardID, job.LaneID, queueWait, runDuration, err)
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
