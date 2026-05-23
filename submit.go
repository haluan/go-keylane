// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"github.com/haluan/go-keylane/internal/core"
)

// Submit adds a job to the queue.
func (q *Queue) Submit(ctx context.Context, job Job) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := job.Validate(); err != nil {
		return err
	}

	if q.config.OverloadEnabled {
		if err := CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{
			Key:  job.Key,
			Lane: job.Lane,
		}); err != nil {
			return err
		}
	}

	laneID, ok := q.reg.Lookup(string(job.Lane))
	if !ok {
		return ErrInvalidLane
	}

	keyHash := core.HashKey(job.Key)
	iJob, err := core.NewInternalJob(job.Run, keyHash, laneID)
	if err != nil {
		return err
	}
	iJob.UseWorkerTiming = job.UseWorkerTiming

	shardID, becameReady, err := q.sched.Enqueue(iJob)
	if err != nil {
		return err
	}

	if becameReady {
		select {
		case q.sched.ReadyCh <- shardID:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
