package keylane

import (
	"errors"
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

func TestErrNotStartedIsComparableWithErrorsIs(t *testing.T) {
	err := ErrNotStarted
	if !errors.Is(err, ErrNotStarted) {
		t.Errorf("expected ErrNotStarted to be checkable with errors.Is")
	}
	if !errors.Is(err, ErrQueueNotStarted) {
		t.Errorf("expected ErrNotStarted to be checkable with ErrQueueNotStarted")
	}
}
