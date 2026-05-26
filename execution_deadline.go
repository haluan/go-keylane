// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"time"
)

// DeadlineBudgetSnapshot is an immutable view of deadline budget state at a pipeline stage boundary.
type DeadlineBudgetSnapshot struct {
	HasDeadline     bool
	Deadline        time.Time
	Remaining       time.Duration
	QueueWait       time.Duration
	Runtime         time.Duration
	BudgetExhausted bool
}

// SnapshotDeadlineBudget copies budget at now into an immutable snapshot for stage code and hooks.
func SnapshotDeadlineBudget(b DeadlineBudget, now time.Time) DeadlineBudgetSnapshot {
	b = b.refreshAt(now)
	return DeadlineBudgetSnapshot{
		HasDeadline:     b.HasDeadline,
		Deadline:        b.Deadline,
		Remaining:       b.RemainingAt(now),
		QueueWait:       b.QueueWait,
		Runtime:         b.Runtime,
		BudgetExhausted: b.IsExhaustedAt(now),
	}
}

// stageDeadlineBudget builds a stage-boundary deadline snapshot including queue wait and elapsed runtime.
func stageDeadlineBudget(reqCtx context.Context, queueWait, runtime time.Duration, at time.Time) DeadlineBudgetSnapshot {
	b := NewDeadlineBudget(reqCtx, at)
	if queueWait > 0 {
		b = b.WithQueueWaitAt(queueWait, at)
	}
	if runtime > 0 {
		b = b.WithRuntimeAt(runtime, at)
	}
	return SnapshotDeadlineBudget(b, at)
}
