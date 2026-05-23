// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"fmt"
)

// Job represents a unit of work to be executed.
type Job struct {
	// Key is used for shard routing. Jobs with the same key are routed to the same shard.
	Key string
	// Lane specifies the processing lane for this job.
	Lane Lane
	// Run is the function that will be executed.
	Run func(context.Context) error
	// UseWorkerTiming requests scheduler AcceptedAt/StartedAt in Run context (SubmitRequest).
	UseWorkerTiming bool
}

// Validate ensures the job is valid.
func (j Job) Validate() error {
	if j.Key == "" {
		return ErrInvalidKey
	}
	if err := j.Lane.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidJob, err)
	}
	if j.Run == nil {
		return ErrNilJobRun
	}
	return nil
}
