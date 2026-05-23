// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"time"

	"github.com/haluan/go-keylane/internal/core"
)

// SubmitRequest submits a typed request into the queue and returns a Future for the output.
func SubmitRequest[I any, O any](
	ctx context.Context,
	q *Queue,
	req Request[I, O],
) (Future[O], error) {
	future := newResultFuture[O]()
	var zero O

	if q == nil {
		future.complete(zero, ErrNilQueue)
		return future, ErrNilQueue
	}

	if err := validateRequest(req); err != nil {
		future.complete(zero, err)
		return future, err
	}

	meta := req.Meta
	shardID := q.ShardIDForKey(meta.Key)

	reject := func(err error) {
		obs := q.newRequestObservation(meta, shardID, 0, 0, err)
		q.emitRequestRejected(obs)
	}

	if err := ctx.Err(); err != nil {
		future.complete(zero, err)
		reject(err)
		return future, err
	}

	if err := CheckAdmission(q, req.Admission, meta); err != nil {
		future.complete(zero, err)
		reject(err)
		return future, err
	}

	reqCtx := ctx
	input := req.Input
	handle := req.Handle

	wrapped := Job{
		Key:             meta.Key,
		Lane:            meta.Lane,
		UseWorkerTiming: q.requestHooksNeedWorkerTiming(),
		Run: func(runCtx context.Context) error {
			wt, ok := core.WorkerTimingFromContext(runCtx)
			queueWait := time.Duration(0)
			var startedAt time.Time
			if ok {
				queueWait = wt.QueueWaitDuration()
				startedAt = wt.StartedAt
			}
			q.emitRequestStarted(q.newRequestObservation(meta, shardID, queueWait, 0, nil))

			if err := reqCtx.Err(); err != nil {
				future.complete(zero, err)
				runDur := time.Duration(0)
				if !startedAt.IsZero() {
					runDur = time.Since(startedAt)
				}
				q.emitRequestCompleted(q.newRequestObservation(meta, shardID, queueWait, runDur, err))
				return err
			}

			out, err := handle(reqCtx, input)
			runDur := time.Duration(0)
			if !startedAt.IsZero() {
				runDur = time.Since(startedAt)
			}
			if err != nil {
				future.complete(zero, err)
			} else {
				future.complete(out, nil)
			}
			q.emitRequestCompleted(q.newRequestObservation(meta, shardID, queueWait, runDur, err))
			return nil
		},
	}

	if err := q.Submit(ctx, wrapped); err != nil {
		future.complete(zero, err)
		reject(err)
		return future, err
	}
	q.emitRequestQueued(meta)

	return future, nil
}
