package core

import (
	"context"
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

// NewInternalJob creates an InternalJob from its components.
// It returns an error if the run function is nil.
func NewInternalJob(run func(context.Context) error, keyHash uint64, laneID LaneID) (InternalJob, error) {
	if run == nil {
		return InternalJob{}, ErrNilJobRun
	}

	return InternalJob{
		KeyHash: keyHash,
		LaneID:  laneID,
		Run:     run,
	}, nil
}
