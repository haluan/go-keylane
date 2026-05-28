// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"errors"
	"fmt"
)

// ErrJobPanicked is returned when a user Job.Run panics and the worker recovers.
var ErrJobPanicked = errors.New("keylane: job panicked")

// JobPanicError wraps a recovered panic value as a job failure error.
func JobPanicError(v any) error {
	if v == nil {
		return fmt.Errorf("%w", ErrJobPanicked)
	}
	return fmt.Errorf("%w: %v", ErrJobPanicked, v)
}

type jobRunFunc func(context.Context) error

// runJobRecoveringPanic executes fn and converts panics into ErrJobPanicked.
func runJobRecoveringPanic(fn jobRunFunc, ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = JobPanicError(r)
		}
	}()
	return fn(ctx)
}
