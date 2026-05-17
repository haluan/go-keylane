package keylane

import (
	"github.com/haluan/go-keylane/internal/core"
)

// TrySubmit attempts to add a job to the queue without blocking.
// It returns false if the job is invalid, the lane is unknown, the queue is full,
// or if the queue is stopped/not started.
func (q *Queue) TrySubmit(job Job) bool {
	if err := job.Validate(); err != nil {
		return false
	}

	laneID, ok := q.reg.Lookup(string(job.Lane))
	if !ok {
		return false
	}

	keyHash := core.HashKey(job.Key)
	iJob, err := core.NewInternalJob(job.Run, keyHash, laneID)
	if err != nil {
		return false
	}

	shardID, becameReady, err := q.sched.TryEnqueue(iJob)
	if err != nil {
		return false
	}

	if becameReady {
		select {
		case q.sched.ReadyCh <- shardID:
		default:
			// shard already queued — work will be seen
		}
	}

	return true
}
