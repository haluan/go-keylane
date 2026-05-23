// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"time"
)

// WorkerTiming carries scheduler worker timestamps for a single job execution.
// Injected into context immediately before InternalJob.Run.
type WorkerTiming struct {
	// AcceptedAt is when the job was admitted to the lane queue (zero if not stamped).
	AcceptedAt time.Time
	// StartedAt is when the worker began executing the job (immediately before Run).
	StartedAt time.Time
}

type workerTimingKey struct{}

// ContextWithWorkerTiming returns ctx with worker timing for the current job Run.
func ContextWithWorkerTiming(ctx context.Context, wt WorkerTiming) context.Context {
	return context.WithValue(ctx, workerTimingKey{}, wt)
}

// WorkerTimingFromContext returns timing attached by the scheduler before Run.
func WorkerTimingFromContext(ctx context.Context) (WorkerTiming, bool) {
	wt, ok := ctx.Value(workerTimingKey{}).(WorkerTiming)
	return wt, ok
}

// QueueWaitDuration returns enqueue-to-worker-start wait using AcceptedAt and StartedAt.
func (wt WorkerTiming) QueueWaitDuration() time.Duration {
	if wt.AcceptedAt.IsZero() || wt.StartedAt.IsZero() {
		return 0
	}
	return wt.StartedAt.Sub(wt.AcceptedAt)
}
