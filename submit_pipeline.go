// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"time"

	"github.com/haluan/go-keylane/internal/core"
)

// SubmitPipeline submits a multi-stage pipeline into the queue and returns a Future for the output.
// It reuses SubmitRequest admission, overload, retry, deadline budget, and future completion semantics.
// Stages run sequentially in-worker; non-blocking continuation is not supported in v0.7 KL-1701.
func SubmitPipeline[S any, O any](
	ctx context.Context,
	q *Queue,
	pipeline Pipeline[S, O],
) (Future[O], error) {
	var zero O
	policy := FailurePolicy{}
	if q != nil {
		policy = q.failurePolicy
	}

	if q == nil {
		future := newResultFuture[O]()
		budget := NewDeadlineBudget(ctx, time.Now())
		future.complete(zero, ErrNilQueue, policy, budget, true)
		return future, ErrNilQueue
	}

	if err := validatePipeline(pipeline); err != nil {
		future := newResultFuture[O]()
		budget := NewDeadlineBudget(ctx, time.Now())
		future.complete(zero, err, policy, budget, true)
		return future, err
	}

	return SubmitRequest(ctx, q, Request[S, O]{
		Meta:             pipeline.Meta,
		Admission:        pipeline.Admission,
		Overload:         pipeline.Overload,
		PerKeyAdmission:  pipeline.PerKeyAdmission,
		Retry:            pipeline.Retry,
		Idempotency:      pipeline.Idempotency,
		RetrySuppression: pipeline.RetrySuppression,
		Input:            pipeline.State,
		Handle: func(reqCtx context.Context, state S) (O, error) {
			return runPipelineStages(reqCtx, q, pipeline, state)
		},
	})
}

func runPipelineStages[S any, O any](
	reqCtx context.Context,
	q *Queue,
	pipeline Pipeline[S, O],
	state S,
) (O, error) {
	var zero O
	meta := pipeline.Meta
	shardID := q.ShardIDForKey(meta.Key)

	queueWait := time.Duration(0)
	if wt, ok := core.WorkerTimingFromContext(reqCtx); ok {
		queueWait = wt.QueueWaitDuration()
	}

	for i := range pipeline.Stages {
		if err := reqCtx.Err(); err != nil {
			return zero, err
		}

		stage := pipeline.Stages[i]
		stageStart := time.Now()
		budget := NewDeadlineBudget(reqCtx, stageStart)
		deadlineRemaining := budget.RemainingAt(stageStart)

		q.emitStageStarted(q.newStageObservation(meta, stage.Meta, shardID, queueWait, 0, deadlineRemaining, nil))

		var err error
		state, err = stage.Run(reqCtx, state)
		stageDur := time.Since(stageStart)
		deadlineRemaining = budget.RemainingAt(time.Now())

		if err != nil {
			stageErr := NewStageFailure(stage.Meta, err)
			obs := q.newStageObservation(meta, stage.Meta, shardID, queueWait, stageDur, deadlineRemaining, stageErr)
			q.emitStageFailed(obs)
			return zero, stageErr
		}

		q.emitStageCompleted(q.newStageObservation(meta, stage.Meta, shardID, queueWait, stageDur, deadlineRemaining, nil))
	}

	return pipeline.Complete(reqCtx, state)
}
