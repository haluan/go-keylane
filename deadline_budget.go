// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"time"
)

// DeadlineBudgetTrace records budget snapshots at key scheduler lifecycle points.
type DeadlineBudgetTrace struct {
	AtSubmit       DeadlineBudget
	AtAdmission    DeadlineBudget
	AfterQueueWait DeadlineBudget
	AtHandlerStart DeadlineBudget
	AtCompletion   DeadlineBudget
}

// DeadlineBudget tracks caller deadline and time consumed by queue wait and runtime.
type DeadlineBudget struct {
	StartedAt   time.Time
	Deadline    time.Time
	HasDeadline bool

	QueueWait time.Duration
	Runtime   time.Duration
	Remaining time.Duration
	Exhausted bool
}

// NewDeadlineBudget captures deadline information from ctx at now.
func NewDeadlineBudget(ctx context.Context, now time.Time) DeadlineBudget {
	b := DeadlineBudget{StartedAt: now}
	if ctx == nil {
		return b.refreshAt(now)
	}
	if dl, ok := ctx.Deadline(); ok {
		b.HasDeadline = true
		b.Deadline = dl
	}
	return b.refreshAt(now)
}

func (b DeadlineBudget) refreshAt(now time.Time) DeadlineBudget {
	if !b.HasDeadline {
		b.Remaining = 0
		b.Exhausted = false
		return b
	}
	rem := b.Deadline.Sub(now)
	if rem < 0 {
		rem = 0
	}
	b.Remaining = rem
	b.Exhausted = rem == 0
	return b
}

// WithQueueWait returns a copy with queue wait recorded and remaining recomputed at now.
func (b DeadlineBudget) WithQueueWait(wait time.Duration) DeadlineBudget {
	b.QueueWait = wait
	return b.refreshAt(time.Now())
}

// WithQueueWaitAt is like WithQueueWait but uses an explicit clock time for remaining.
func (b DeadlineBudget) WithQueueWaitAt(wait time.Duration, now time.Time) DeadlineBudget {
	b.QueueWait = wait
	return b.refreshAt(now)
}

// WithRuntime returns a copy with runtime recorded and remaining recomputed at now.
func (b DeadlineBudget) WithRuntime(runtime time.Duration) DeadlineBudget {
	b.Runtime = runtime
	return b.refreshAt(time.Now())
}

// WithRuntimeAt is like WithRuntime but uses an explicit clock time for remaining.
func (b DeadlineBudget) WithRuntimeAt(runtime time.Duration, now time.Time) DeadlineBudget {
	b.Runtime = runtime
	return b.refreshAt(now)
}

// HasRemaining reports whether at least min remains before the deadline at now.
func (b DeadlineBudget) HasRemaining(min time.Duration) bool {
	return b.RemainingAt(time.Now()) >= min
}

// HasRemainingAt reports whether at least min remains at now.
func (b DeadlineBudget) HasRemainingAt(min time.Duration, now time.Time) bool {
	return b.RemainingAt(now) >= min
}

// RemainingAt returns time until deadline at now (zero when no deadline or exhausted).
func (b DeadlineBudget) RemainingAt(now time.Time) time.Duration {
	if !b.HasDeadline {
		return 0
	}
	rem := b.Deadline.Sub(now)
	if rem < 0 {
		return 0
	}
	return rem
}

// IsExhaustedAt reports whether the deadline has passed at now.
func (b DeadlineBudget) IsExhaustedAt(now time.Time) bool {
	if !b.HasDeadline {
		return false
	}
	return !now.Before(b.Deadline)
}

// ClassifyContextError classifies context errors using budget and execution phase.
// beforeHandlerStart should be true when the error is observed before the user handler runs.
func ClassifyContextError(err error, budget DeadlineBudget, beforeHandlerStart bool) Failure {
	if err == nil {
		return NewFailure(FailureNone, nil)
	}
	if errors.Is(err, context.Canceled) {
		return CancelledFailure(err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		if beforeHandlerStart && budget.IsExhaustedAt(time.Now()) {
			return DeadlineExhaustedFailure(err)
		}
		return TimeoutFailure(err)
	}
	return classifyDefault(err)
}

// ClassifyContextErrorAt is ClassifyContextError with an explicit clock for tests.
func ClassifyContextErrorAt(err error, budget DeadlineBudget, beforeHandlerStart bool, now time.Time) Failure {
	if err == nil {
		return NewFailure(FailureNone, nil)
	}
	if errors.Is(err, context.Canceled) {
		return CancelledFailure(err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		if beforeHandlerStart && budget.IsExhaustedAt(now) {
			return DeadlineExhaustedFailure(err)
		}
		return TimeoutFailure(err)
	}
	return classifyDefault(err)
}
