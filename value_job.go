// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "context"

// ValueJob is a job that returns a value of type T and an error.
type ValueJob[T any] struct {
	Key         string
	Lane        Lane
	Retry       RetryPolicy
	Idempotency Idempotency
	// RetrySuppression is an optional per-job override (nil uses queue policy).
	RetrySuppression *RetrySuppressionPolicy
	Run              func(context.Context) (T, error)
}

func validateValueJob[T any](job ValueJob[T]) error {
	if job.Key == "" {
		return ErrInvalidKey
	}
	if err := job.Lane.Validate(); err != nil {
		return err
	}
	if job.Run == nil {
		return ErrNilJobRun
	}
	return nil
}
