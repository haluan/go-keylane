package core

import (
	"context"

	"github.com/haluan/go-keylane"
)

// InternalJob is the hot-path representation of a job.
// It is optimized for routing and execution.
type InternalJob struct {
	// KeyHash is the hash of the job's key, used for shard routing.
	KeyHash uint64
	// LaneID is the internal ID of the processing lane.
	LaneID LaneID
	// Run is the function that will be executed.
	Run func(context.Context) error
}

// NewInternalJob converts a public keylane.Job to an InternalJob.
// It validates the job before conversion.
func NewInternalJob(job keylane.Job, keyHash uint64, laneID LaneID) (InternalJob, error) {
	if err := job.Validate(); err != nil {
		return InternalJob{}, err
	}

	return InternalJob{
		KeyHash: keyHash,
		LaneID:  laneID,
		Run:     job.Run,
	}, nil
}
