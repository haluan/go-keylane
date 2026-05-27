// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"sync"
	"time"

	"github.com/haluan/go-keylane/internal/core"
)

// hasContinuationStages returns true when any stage uses RunContinuation.
func hasContinuationStages[S any, O any](pipeline Pipeline[S, O]) bool {
	for i := range pipeline.Stages {
		if pipeline.Stages[i].RunContinuation != nil {
			return true
		}
	}
	return false
}

// enqueueContinuationJob submits a job directly to the scheduler, bypassing overload and admission
// checks. Used for resume jobs that are already part of an admitted pipeline request.
func enqueueContinuationJob(q *Queue, key string, lane Lane, runFn func(context.Context) error) error {
	laneID, ok := q.reg.Lookup(string(lane))
	if !ok {
		return ErrInvalidLane
	}
	keyHash := core.HashKey(key)
	iJob, err := core.NewInternalJob(runFn, keyHash, laneID)
	if err != nil {
		return err
	}
	shardID, becameReady, err := q.sched.Enqueue(iJob)
	if err != nil {
		return err
	}
	if becameReady {
		select {
		case q.sched.ReadyCh <- shardID:
		default:
			// Channel full: shard is already signalled; scheduler will pick it up in the next cycle.
		}
	}
	return nil
}

// submitPipelineWithContinuation handles submission when one or more stages use RunContinuation.
// It replicates SubmitRequest admission semantics and manages future completion directly.
func submitPipelineWithContinuation[S any, O any](
	ctx context.Context,
	q *Queue,
	pipeline Pipeline[S, O],
) (Future[O], error) {
	var zero O
	policy := q.failurePolicy
	future := newResultFuture[O]()
	budget := NewDeadlineBudget(ctx, time.Now())
	future.budgetTrace.AtSubmit = budget

	completeReject := func(err error) {
		future.complete(zero, err, policy, budget, true)
	}

	if q.continuationReg == nil {
		completeReject(ErrContinuationDisabled)
		return future, ErrContinuationDisabled
	}

	meta := pipeline.Meta
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

	if err := CheckOverload(q, pipeline.Overload, meta); err != nil {
		completeReject(err)
		reject(err)
		return future, err
	}

	if err := CheckAdmission(q, pipeline.Admission, meta); err != nil {
		completeReject(err)
		reject(err)
		return future, err
	}

	perKeyCfg := q.config.PerKeyAdmission
	if pipeline.PerKeyAdmission.Enabled {
		perKeyCfg = pipeline.PerKeyAdmission
	}
	if err := CheckPerKeyAdmission(q, perKeyCfg, meta); err != nil {
		completeReject(err)
		reject(err)
		return future, err
	}

	budget = budget.refreshAt(time.Now())
	future.budgetTrace.AtAdmission = budget

	retryPolicy := resolveRetryPolicy(q.retryPolicy, pipeline.Retry)
	retryState := &pipelineRunRetryState{
		opts: buildRunWithRetryOpts(
			q, meta.Key, meta.Lane, shardID, pipeline.Idempotency, pipeline.RetrySuppression,
		),
	}

	// enqueuePipelineRun enqueues one attempt of the pipeline (initial or retry re-enqueue).
	// attempt starts at 1. priorRuntime accumulates total request lifetime before this attempt.
	var enqueuePipelineRun func(attempt int, priorRuntime time.Duration) error
	enqueuePipelineRun = func(attempt int, priorRuntime time.Duration) error {
		return enqueueContinuationJob(q, meta.Key, meta.Lane, func(runCtx context.Context) error {
			wt, ok := core.WorkerTimingFromContext(runCtx)
			queueWait := time.Duration(0)
			if ok {
				queueWait = wt.QueueWaitDuration()
			}
			now := time.Now()
			jobBudget := budget.WithQueueWaitAt(queueWait, now)
			if attempt == 1 {
				future.budgetTrace.AfterQueueWait = jobBudget
			}

			if err := ctx.Err(); err != nil {
				finalBudget := jobBudget.refreshAt(time.Now())
				if attempt == 1 {
					future.budgetTrace.AtHandlerStart = finalBudget
				}
				future.completeWithFailureObs(q, zero, err, policy, finalBudget, true)
				obs := q.newRequestObservation(meta, shardID, queueWait, 0, err)
				q.emitRequestCompleted(obs)
				q.emitFailureEvent(obs, err)
				return err
			}

			if attempt == 1 {
				future.budgetTrace.AtHandlerStart = jobBudget.refreshAt(time.Now())
			}

			runContinuationPipeline(
				ctx, q, pipeline, pipeline.State, 0, future,
				policy, retryPolicy, budget, queueWait, attempt, priorRuntime, shardID,
				enqueuePipelineRun, retryState,
			)
			return nil
		})
	}

	if err := enqueuePipelineRun(1, 0); err != nil {
		completeReject(err)
		reject(err)
		return future, err
	}

	q.emitRequestQueued(meta)
	return future, nil
}

type pipelineRunRetryState struct {
	opts     runWithRetryOpts
	attempts []RetryAttempt
}

// runContinuationPipeline runs pipeline stages from resumeFrom onward, handling both synchronous
// stages (Run) and continuation stages (RunContinuation). On yield the worker is freed immediately;
// on completion or non-retryable failure the future is resolved.
func runContinuationPipeline[S any, O any](
	reqCtx context.Context,
	q *Queue,
	pipeline Pipeline[S, O],
	state S,
	resumeFrom int,
	future *resultFuture[O],
	policy FailurePolicy,
	retryPolicy RetryPolicy,
	submitBudget DeadlineBudget,
	queueWait time.Duration,
	attempt int,
	priorRuntime time.Duration,
	shardID int,
	enqueuePipelineRun func(attempt int, priorRuntime time.Duration) error,
	retryState *pipelineRunRetryState,
) {
	var zero O
	meta := pipeline.Meta
	stageCount := len(pipeline.Stages)
	pipelineStart := time.Now()

	base := baseExecutionContext(
		meta, shardID, queueWait, attempt,
		StageMeta{}, resumeFrom, stageCount,
		stageDeadlineBudget(reqCtx, queueWait, priorRuntime, pipelineStart),
	)

	// completeFailed classifies the error, applies retry if applicable, then resolves the future.
	// Retry restarts the entire pipeline from stage 0.
	completeFailed := func(err error) {
		now := time.Now()
		totalRuntime := priorRuntime + now.Sub(pipelineStart)
		attemptBudget := submitBudget.WithRuntimeAt(totalRuntime, now)

		eval := evaluatePipelineRetry(
			reqCtx, policy, retryPolicy, retryState.opts, attempt, err, attemptBudget,
			&retryState.attempts, nil, defaultRetryClock,
		)
		if eval.ScheduleRetry {
			if sleepErr := defaultRetryClock.Sleep(reqCtx, eval.Delay); sleepErr != nil {
				err = sleepErr
				eval.Failure = classifyFailureWithPolicy(err, policy)
				eval.TerminalFinal = finalStateFromFailure(
					eval.Failure, false, RetryDecisionContextCancelled, "", "",
				)
			} else {
				retryErr := enqueuePipelineRun(attempt+1, totalRuntime)
				if retryErr == nil {
					return
				}
				err = retryErr
				eval.Failure = classifyFailureWithPolicy(err, policy)
				eval.TerminalFinal = finalStateFromFailure(
					eval.Failure, false, RetryDecisionPermanentFailure, "", "",
				)
			}
		}

		finalBudget := submitBudget.WithRuntimeAt(totalRuntime, time.Now())
		future.setRetryOutcome(retryState.attempts, eval.TerminalFinal, len(retryState.attempts) > 0)
		handlerErr := retryHandlerError(err, eval.Failure)
		future.completeWithFailureObs(q, zero, handlerErr, policy, finalBudget, false)
		runDur := time.Since(pipelineStart)
		obs := q.newRequestObservation(meta, shardID, queueWait, runDur, handlerErr)
		q.emitRequestCompleted(obs)
		q.emitFailureEvent(obs, handlerErr)
	}

	for i := resumeFrom; i < stageCount; i++ {
		stage := pipeline.Stages[i]
		stageStart := time.Now()
		runtime := priorRuntime + stageStart.Sub(pipelineStart)
		deadline := stageDeadlineBudget(reqCtx, queueWait, runtime, stageStart)

		if err := pipelineStageDeadlineExhausted(
			base, stage.Meta, i, stageCount, queueWait, priorRuntime, pipelineStart, stageStart, reqCtx,
		); err != nil {
			completeFailed(err)
			return
		}

		if err := reqCtx.Err(); err != nil {
			if err == context.DeadlineExceeded {
				exec := withPipelineStage(base, stage.Meta, i, stageCount, runtime, deadline)
				completeFailed(NewStageFailureFromExecution(exec, DeadlineExhaustedFailure(context.DeadlineExceeded)))
			} else {
				completeFailed(err)
			}
			return
		}

		exec := withPipelineStage(base, stage.Meta, i, stageCount, runtime, deadline)
		stageCtx := ContextWithStageExecution(reqCtx, exec)
		q.emitStageStarted(q.newStageObservationFromExecution(exec, 0, nil))

		if stage.RunContinuation != nil {
			result, err := recoverStageResult(func() (StageResult[S], error) {
				return stage.RunContinuation(stageCtx, state)
			})
			if err != nil {
				stageDur := time.Since(stageStart)
				endRuntime := priorRuntime + time.Since(pipelineStart)
				endDeadline := stageDeadlineBudget(reqCtx, queueWait, endRuntime, time.Now())
				exec.Deadline = endDeadline
				stageErr := NewStageFailureFromExecution(exec, err)
				obs := q.newStageObservationFromExecution(exec, stageDur, stageErr)
				q.emitStageFailed(obs)
				completeFailed(stageErr)
				return
			}

			if result.Continuation != nil {
				cont := result.Continuation
				cont.setRequestContext(reqCtx)
				if err := reqCtx.Err(); err != nil {
					close(cont.done)
					if err == context.DeadlineExceeded {
						completeFailed(NewStageFailureFromExecution(exec, DeadlineExhaustedFailure(context.DeadlineExceeded)))
					} else {
						completeFailed(err)
					}
					return
				}
				cont.exec = exec
				cont.stageIndex = i
				cont.stageCount = stageCount
				cont.yieldedAt = time.Now()
				cont.ID = q.continuationReg.allocID()

				// priorRuntime at yield point includes all time up to yield within this run.
				contPriorRuntime := priorRuntime + cont.yieldedAt.Sub(pipelineStart)

				var doneOnce sync.Once
				closeDone := func() { doneOnce.Do(func() { close(cont.done) }) }

				// Install before register so Complete after cancel cannot run invokeLate with no handler.
				setContinuationLateHandler(cont.boundCompleter, q, exec, nil)

				entry := pendingEntry{
					id:           cont.ID,
					shardID:      shardID,
					registeredAt: cont.yieldedAt,
					closeDone:    closeDone,
				}
				if regErr := q.continuationReg.register(entry); regErr != nil {
					closeDone()
					stageDur := time.Since(stageStart)
					endRuntime := priorRuntime + time.Since(pipelineStart)
					endDeadline := stageDeadlineBudget(reqCtx, queueWait, endRuntime, time.Now())
					exec.Deadline = endDeadline
					stageErr := NewStageFailureFromExecution(exec, regErr)
					obs := q.newStageObservationFromExecution(exec, stageDur, stageErr)
					q.emitStageFailed(obs)
					completeFailed(stageErr)
					return
				}

				yieldedObs := continuationObsFromExec(
					cont.ID, exec, 0, 0, RequestOutcomeCompleted, FailureNone, nil,
				)
				q.emitContinuationYielded(yieldedObs)

				// Resolution goroutine: frees the worker and handles the continuation lifecycle.
				go func(cont *Continuation[S], exec StageExecutionContext, contPriorRuntime time.Duration) {
					recordLateOutcome := func(existing continuationOutcome[S]) {
						cont.recordLateCompletion(q, exec, existing.kind, existing.err)
					}

					var o continuationOutcome[S]
					select {
					case <-reqCtx.Done():
						closeDone()
						o = continuationOutcome[S]{
							kind: continuationOutcomeCancel,
							err:  reqCtx.Err(),
						}
						select {
						case cont.outcome <- o:
						case existing := <-cont.outcome:
							recordLateOutcome(existing)
						default:
						}
						// Completer may publish after the select above (inner default).
						select {
						case existing := <-cont.outcome:
							recordLateOutcome(existing)
						default:
						}
						// Completer may publish after closeDone; outcome buffer holds at most one value.
						select {
						case existing := <-cont.outcome:
							recordLateOutcome(existing)
						default:
						}
					case o = <-cont.outcome:
						if err := reqCtx.Err(); err != nil {
							recordLateOutcome(o)
							o.kind = continuationOutcomeCancel
							o.err = err
						}
					}

					_, found := q.continuationReg.resolve(cont.ID, o.kind)
					yieldedFor := time.Since(cont.yieldedAt)

					obs := continuationObsFromExec(
						cont.ID, exec, yieldedFor, 0,
						classifyOutcomeRequestOutcome(o.kind),
						classifyOutcomeFailureKind(o.kind),
						o.err,
					)

					if !found {
						// Late resolution: another path already resolved this continuation.
						q.emitContinuationLate(obs)
						return
					}

					switch o.kind {
					case continuationOutcomeComplete:
						q.emitContinuationCompleted(obs)
						// Time yielded counts as request runtime for deadline purposes.
						resumePriorRuntime := contPriorRuntime + yieldedFor
						resumeErr := enqueueContinuationJob(q, meta.Key, meta.Lane, func(runCtx context.Context) error {
							wt, ok := core.WorkerTimingFromContext(runCtx)
							resumeQueueWait := time.Duration(0)
							if ok {
								resumeQueueWait = wt.QueueWaitDuration()
							}
							resumeObs := obs
							resumeObs.ResumeQueueWait = resumeQueueWait
							q.emitContinuationResumed(resumeObs)

							runContinuationPipeline(
								reqCtx, q, pipeline, o.state, cont.stageIndex+1, future,
								policy, retryPolicy, submitBudget, queueWait, attempt,
								resumePriorRuntime, shardID, enqueuePipelineRun, retryState,
							)
							return nil
						})
						if resumeErr != nil {
							q.continuationReg.recordResumeRejected()
							now := time.Now()
							finalBudget := submitBudget.WithRuntimeAt(contPriorRuntime+yieldedFor, now)
							stageErr := NewStageFailureFromExecution(exec, ErrContinuationResumeRejected)
							future.completeWithFailureObs(q, zero, stageErr, policy, finalBudget, false)
							failedObs := obs
							failedObs.FailureKind = FailureRejected
							failedObs.Err = ErrContinuationResumeRejected
							q.emitContinuationFailed(failedObs)
						}

					case continuationOutcomeFail:
						q.emitContinuationFailed(obs)
						stageErr := NewStageFailureFromExecution(exec, o.err)
						completeFailed(stageErr)

					case continuationOutcomeCancel:
						q.emitContinuationCancelled(obs)
						completeFailed(o.err)
					}
				}(cont, exec, contPriorRuntime)

				// Worker is free; future completion happens in the resolution goroutine.
				return
			}

			// result.Continuation == nil: stage completed synchronously via RunContinuation.
			stageDur := time.Since(stageStart)
			endRuntime := priorRuntime + time.Since(pipelineStart)
			endDeadline := stageDeadlineBudget(reqCtx, queueWait, endRuntime, time.Now())
			exec.Deadline = endDeadline
			q.emitStageCompleted(q.newStageObservationFromExecution(exec, stageDur, nil))
			state = result.State

		} else {
			// Synchronous stage.
			var err error
			state, err = recoverStageRun(func() (S, error) {
				return stage.Run(stageCtx, state)
			})
			stageDur := time.Since(stageStart)
			endRuntime := priorRuntime + time.Since(pipelineStart)
			endDeadline := stageDeadlineBudget(reqCtx, queueWait, endRuntime, time.Now())
			exec.Deadline = endDeadline

			if err != nil {
				stageErr := NewStageFailureFromExecution(exec, err)
				obs := q.newStageObservationFromExecution(exec, stageDur, stageErr)
				q.emitStageFailed(obs)
				completeFailed(stageErr)
				return
			}

			q.emitStageCompleted(q.newStageObservationFromExecution(exec, stageDur, nil))
		}
	}

	// All stages completed; run the Complete function.
	completeStart := time.Now()
	completeRuntime := priorRuntime + completeStart.Sub(pipelineStart)
	completeDeadline := stageDeadlineBudget(reqCtx, queueWait, completeRuntime, completeStart)
	if err := pipelineStageDeadlineExhausted(
		base, StageMeta{Name: StageResponse}, stageCount, stageCount,
		queueWait, priorRuntime, pipelineStart, completeStart, reqCtx,
	); err != nil {
		completeFailed(err)
		return
	}

	completeExec := withPipelineStage(
		base, StageMeta{Name: StageResponse}, stageCount, stageCount,
		completeRuntime, completeDeadline,
	)
	completeCtx := ContextWithStageExecution(reqCtx, completeExec)

	out, err := pipeline.Complete(completeCtx, state)
	now := time.Now()
	totalRuntime := priorRuntime + now.Sub(pipelineStart)
	finalBudget := submitBudget.WithRuntimeAt(totalRuntime, now)

	if err != nil {
		future.completeWithFailureObs(q, zero, err, policy, finalBudget, false)
	} else {
		if len(retryState.attempts) > 0 {
			future.setRetryOutcome(retryState.attempts, RetryFinalState{Succeeded: true}, true)
		}
		future.complete(out, nil, policy, finalBudget, false)
	}

	runDur := now.Sub(pipelineStart)
	obs := q.newRequestObservation(meta, shardID, queueWait, runDur, err)
	q.emitRequestCompleted(obs)
	if err != nil {
		q.emitFailureEvent(obs, err)
	}
}

// classifyOutcomeRequestOutcome maps a continuation outcome kind to a RequestOutcome.
func classifyOutcomeRequestOutcome(kind ContinuationOutcomeKind) RequestOutcome {
	switch kind {
	case continuationOutcomeComplete:
		return RequestOutcomeCompleted
	case continuationOutcomeCancel:
		return RequestOutcomeCancelled
	default:
		return RequestOutcomeFailed
	}
}

// classifyOutcomeFailureKind maps a continuation outcome kind to a FailureKind for observations.
func classifyOutcomeFailureKind(kind ContinuationOutcomeKind) FailureKind {
	switch kind {
	case continuationOutcomeComplete:
		return FailureNone
	case continuationOutcomeCancel:
		return FailureCancelled
	default:
		return FailureUnknown
	}
}
