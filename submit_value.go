// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"time"
)

// SubmitValue submits a ValueJob to the queue and returns a Future that will contain the result.
func SubmitValue[T any](
	ctx context.Context,
	q *Queue,
	job ValueJob[T],
) (Future[T], error) {
	future := newResultFuture[T]()
	var zero T
	policy := FailurePolicy{}
	retryPolicy := RetryPolicy{}
	if q != nil {
		policy = q.failurePolicy
		retryPolicy = resolveRetryPolicy(q.retryPolicy, job.Retry)
	}
	budget := NewDeadlineBudget(ctx, time.Now())
	future.budgetTrace.AtSubmit = budget

	completeErr := func(err error) {
		future.complete(zero, err, policy, budget, true)
	}

	if q == nil {
		completeErr(ErrNilQueue)
		return future, ErrNilQueue
	}

	if err := validateValueJob(job); err != nil {
		completeErr(err)
		return future, err
	}

	var retryOpts runWithRetryOpts
	if retryPolicy.Enabled {
		retryOpts = runWithRetryOpts{
			Key: job.Key, Lane: job.Lane,
			Idempotency: job.Idempotency, IdempotencyPolicy: q.idempotencyPolicy,
		}
	}

	wrapped := Job{
		Key:  job.Key,
		Lane: job.Lane,
		Run: func(runCtx context.Context) error {
			handlerStartNow := time.Now()
			handlerStartBudget := budget.refreshAt(handlerStartNow)
			future.budgetTrace.AtHandlerStart = handlerStartBudget

			if err := ctx.Err(); err != nil {
				future.complete(zero, err, policy, handlerStartBudget, true)
				return err
			}

			runStart := handlerStartNow
			var val T
			var err error
			var beforeHandler bool

			if retryPolicy.Enabled {
				res := runValueJobWithRetry(
					ctx, policy, retryPolicy, retryOpts, handlerStartBudget, job.Run,
				)
				future.appendRetryAttempts(res.retryAttempts)
				val, err, beforeHandler = res.val, res.err, res.beforeHandler
			} else {
				val, err = job.Run(ctx)
				beforeHandler = false
			}

			finalBudget := handlerStartBudget.WithRuntimeAt(time.Since(runStart), time.Now())
			if err != nil {
				future.complete(zero, err, policy, finalBudget, beforeHandler)
			} else {
				future.complete(val, nil, policy, finalBudget, beforeHandler)
			}
			return nil
		},
	}

	if err := q.Submit(ctx, wrapped); err != nil {
		completeErr(err)
		return future, err
	}

	return future, nil
}
