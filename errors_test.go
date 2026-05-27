// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"errors"
	"fmt"
	"testing"
)

func TestErrQueueFullIsComparableWithErrorsIs(t *testing.T) {
	err := ErrQueueFull
	if !errors.Is(err, ErrQueueFull) {
		t.Errorf("expected ErrQueueFull to be checkable with errors.Is")
	}
}

func TestErrStoppedIsComparableWithErrorsIs(t *testing.T) {
	err := ErrStopped
	if !errors.Is(err, ErrStopped) {
		t.Errorf("expected ErrStopped to be checkable with errors.Is")
	}
}

func TestErrJobPanickedIsComparableWithErrorsIs(t *testing.T) {
	err := fmt.Errorf("%w: test", ErrJobPanicked)
	if !errors.Is(err, ErrJobPanicked) {
		t.Errorf("expected ErrJobPanicked to be checkable with errors.Is")
	}
}

func TestErrNotStartedIsComparableWithErrorsIs(t *testing.T) {
	err := ErrNotStarted
	if !errors.Is(err, ErrNotStarted) {
		t.Errorf("expected ErrNotStarted to be checkable with errors.Is")
	}
	if !errors.Is(err, ErrQueueNotStarted) {
		t.Errorf("expected ErrNotStarted to be checkable with ErrQueueNotStarted")
	}
}
