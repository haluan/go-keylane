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
	policy := FailurePolicy{}
	if q != nil {
		policy = q.failurePolicy
	}
	budget := NewDeadlineBudget(ctx, time.Now())
	future.budgetTrace.AtSubmit = budget

	completeReject := func(err error) {
		future.complete(zero, err, policy, budget, true)
	}

	if q == nil {
		completeReject(ErrNilQueue)
		return future, ErrNilQueue
	}

	if err := validateRequest(req); err != nil {
		completeReject(err)
		return future, err
	}

	meta := req.Meta
	shardID := q.ShardIDForKey(meta.Key)

	reject := func(err error) {
		obs := q.newRequestObservation(meta, shardID, 0, 0, err)
		q.emitRequestRejected(obs)
		q.emitFailureEvent(obs, err)
	}

	if err := ctx.Err(); err != nil {
		completeReject(err)
		reject(err)
		return future, err
	}

	if err := CheckOverload(q, req.Overload, meta); err != nil {
		completeReject(err)
		reject(err)
		return future, err
	}

	if err := CheckAdmission(q, req.Admission, meta); err != nil {
		completeReject(err)
		reject(err)
		return future, err
	}

	perKeyCfg := q.config.PerKeyAdmission
	if req.PerKeyAdmission.Enabled {
		perKeyCfg = req.PerKeyAdmission
	}
	if err := CheckPerKeyAdmission(q, perKeyCfg, meta); err != nil {
		completeReject(err)
		reject(err)
		return future, err
	}

	budget = budget.refreshAt(time.Now())
	future.budgetTrace.AtAdmission = budget

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
			queueNow := time.Now()
			jobBudget := budget.WithQueueWaitAt(queueWait, queueNow)
			future.budgetTrace.AfterQueueWait = jobBudget

			handlerStartNow := time.Now()
			handlerStartBudget := jobBudget.refreshAt(handlerStartNow)
			future.budgetTrace.AtHandlerStart = handlerStartBudget

			q.emitRequestStarted(q.newRequestObservation(meta, shardID, queueWait, 0, nil))

			if err := reqCtx.Err(); err != nil {
				future.complete(zero, err, policy, handlerStartBudget, true)
				runDur := time.Duration(0)
				if !startedAt.IsZero() {
					runDur = time.Since(startedAt)
				}
				obs := q.newRequestObservation(meta, shardID, queueWait, runDur, err)
				q.emitRequestCompleted(obs)
				q.emitFailureEvent(obs, err)
				return err
			}

			out, err := handle(reqCtx, input)
			runDur := time.Duration(0)
			if !startedAt.IsZero() {
				runDur = time.Since(startedAt)
			}
			finalBudget := handlerStartBudget.WithRuntimeAt(runDur, time.Now())
			if err != nil {
				future.complete(zero, err, policy, finalBudget, false)
			} else {
				future.complete(out, nil, policy, finalBudget, false)
			}
			obs := q.newRequestObservation(meta, shardID, queueWait, runDur, err)
			q.emitRequestCompleted(obs)
			if err != nil {
				q.emitFailureEvent(obs, err)
			}
			return nil
		},
	}

	if err := q.Submit(ctx, wrapped); err != nil {
		completeReject(err)
		reject(err)
		return future, err
	}
	q.emitRequestQueued(meta)

	return future, nil
}
