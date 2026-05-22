// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"time"
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
	// AcceptedAt is when the job was admitted to the lane queue (set immediately before
	// a successful push). Zero means not admitted. Used for StatsGCPressure queue-wait timing.
	AcceptedAt time.Time
	// EnqueuedAt is when v1 queue-wait tracking is enabled (TrackQueueWait). Set on the
	// same successful admission as AcceptedAt. Zero means not set.
	EnqueuedAt time.Time
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
