// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"time"

	"github.com/haluan/go-keylane/internal/core"
)

// pipelineBudgetExhaustedSentinel is used as the inner error for pre-stage budget exhaustion.
// It satisfies errors.Is(err, context.DeadlineExceeded) so callers can use standard deadline checks,
// while remaining distinct from context.DeadlineExceeded so future.go can preserve FailureDeadlineExhausted
// rather than re-classifying via ClassifyContextErrorAt (which would return FailureTimeout).
type pipelineBudgetExhaustedSentinel struct{}

func (pipelineBudgetExhaustedSentinel) Error() string {
	return "keylane: pipeline budget exhausted before stage"
}

func (pipelineBudgetExhaustedSentinel) Is(target error) bool {
	return target == context.DeadlineExceeded
}

var errPipelineBudgetExhaustedBeforeStage error = pipelineBudgetExhaustedSentinel{}

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

	if hasContinuationStages(pipeline) {
		return submitPipelineWithContinuation(ctx, q, pipeline)
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

func pipelineParentExecution(reqCtx context.Context) (queueWait time.Duration, attempt int, priorRuntime time.Duration) {
	attempt = 1
	if parent, ok := StageExecutionFromContext(reqCtx); ok {
		return parent.QueueWait, parent.Attempt, parent.Deadline.Runtime
	}
	if wt, ok := core.WorkerTimingFromContext(reqCtx); ok {
		return wt.QueueWaitDuration(), attempt, 0
	}
	return 0, attempt, 0
}

func pipelineStageDeadlineExhausted(
	base StageExecutionContext,
	stage StageMeta,
	stageIndex, stageCount int,
	queueWait, priorRuntime time.Duration,
	pipelineStart, at time.Time,
	reqCtx context.Context,
) error {
	runtime := priorRuntime + at.Sub(pipelineStart)
	deadline := stageDeadlineBudget(reqCtx, queueWait, runtime, at)
	if !deadline.BudgetExhausted {
		return nil
	}
	exec := withPipelineStage(base, stage, stageIndex, stageCount, runtime, deadline)
	return NewStageFailureFromExecution(exec, DeadlineExhaustedFailure(errPipelineBudgetExhaustedBeforeStage))
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
	stageCount := len(pipeline.Stages)

	queueWait, attempt, priorRuntime := pipelineParentExecution(reqCtx)
	pipelineStart := time.Now()

	base := baseExecutionContext(
		meta, shardID, queueWait, attempt,
		StageMeta{}, 0, stageCount,
		stageDeadlineBudget(reqCtx, queueWait, priorRuntime, pipelineStart),
	)

	for i := range pipeline.Stages {
		stage := pipeline.Stages[i]
		stageStart := time.Now()
		runtime := priorRuntime + stageStart.Sub(pipelineStart)
		deadline := stageDeadlineBudget(reqCtx, queueWait, runtime, stageStart)

		if err := pipelineStageDeadlineExhausted(
			base, stage.Meta, i, stageCount, queueWait, priorRuntime, pipelineStart, stageStart, reqCtx,
		); err != nil {
			return zero, err
		}

		if err := reqCtx.Err(); err != nil {
			if err == context.DeadlineExceeded {
				exec := withPipelineStage(base, stage.Meta, i, stageCount, runtime, deadline)
				return zero, NewStageFailureFromExecution(exec, DeadlineExhaustedFailure(errPipelineBudgetExhaustedBeforeStage))
			}
			return zero, err
		}

		exec := withPipelineStage(base, stage.Meta, i, stageCount, runtime, deadline)
		stageCtx := ContextWithStageExecution(reqCtx, exec)

		q.emitStageStarted(q.newStageObservationFromExecution(exec, 0, nil))

		var err error
		state, err = stage.Run(stageCtx, state)
		stageDur := time.Since(stageStart)
		endRuntime := priorRuntime + time.Since(pipelineStart)
		endDeadline := stageDeadlineBudget(reqCtx, queueWait, endRuntime, time.Now())
		exec.Deadline = endDeadline

		if err != nil {
			stageErr := NewStageFailureFromExecution(exec, err)
			obs := q.newStageObservationFromExecution(exec, stageDur, stageErr)
			q.emitStageFailed(obs)
			return zero, stageErr
		}

		q.emitStageCompleted(q.newStageObservationFromExecution(exec, stageDur, nil))
	}

	completeStart := time.Now()
	completeRuntime := priorRuntime + completeStart.Sub(pipelineStart)
	completeDeadline := stageDeadlineBudget(reqCtx, queueWait, completeRuntime, completeStart)
	if err := pipelineStageDeadlineExhausted(
		base, StageMeta{Name: StageResponse}, stageCount, stageCount,
		queueWait, priorRuntime, pipelineStart, completeStart, reqCtx,
	); err != nil {
		return zero, err
	}
	completeExec := withPipelineStage(
		base, StageMeta{Name: StageResponse}, stageCount, stageCount,
		completeRuntime, completeDeadline,
	)
	completeCtx := ContextWithStageExecution(reqCtx, completeExec)

	return pipeline.Complete(completeCtx, state)
}
