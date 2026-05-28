// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"errors"
	"testing"
)

func TestRunJobRecoveringPanicConvertsPanic(t *testing.T) {
	err := runJobRecoveringPanic(func(context.Context) error {
		panic("boom")
	}, context.Background())
	if !errors.Is(err, ErrJobPanicked) {
		t.Fatalf("err = %v, want ErrJobPanicked", err)
	}
}

func TestRunJobRecoveringPanicPassesThroughError(t *testing.T) {
	want := errors.New("fail")
	err := runJobRecoveringPanic(func(context.Context) error {
		return want
	}, context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

func TestJobOutcomeFromErrorPanicked(t *testing.T) {
	if got := jobOutcomeFromError(JobPanicError("x")); got != JobOutcomePanicked {
		t.Fatalf("outcome = %v, want JobOutcomePanicked", got)
	}
}
