// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"

	"github.com/haluan/go-keylane/internal/core"
)

func jobPanicError(v any) error {
	return core.JobPanicError(v)
}

// recoverValueJobRun executes fn and converts panics into ErrJobPanicked.
// Used by SubmitValue/SubmitRequest wrappers so futures complete when user code panics.
func recoverValueJobRun[T any](fn func(context.Context) (T, error), ctx context.Context) (val T, err error) {
	defer func() {
		if r := recover(); r != nil {
			var zero T
			val = zero
			err = jobPanicError(r)
		}
	}()
	return fn(ctx)
}
