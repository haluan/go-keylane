// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "time"

// ContinuationObservation carries lifecycle event data for a pipeline continuation.
type ContinuationObservation struct {
	ContinuationID ContinuationID

	RequestID string
	Key       string
	Lane      Lane
	ShardID   int

	Transport string
	Operation string

	Stage      StageMeta
	StageIndex int
	StageCount int
	Attempt    int

	// YieldedFor is the wall-clock duration the continuation was pending before resolution.
	YieldedFor time.Duration

	// ResumeQueueWait is the duration the resume job waited in the queue before the worker picked it up.
	ResumeQueueWait time.Duration

	Outcome     RequestOutcome
	FailureKind FailureKind
	Err         error
}

// ContinuationHooks contains optional callbacks for continuation lifecycle events.
// All callbacks are invoked through callHook and must not be called while holding locks.
type ContinuationHooks struct {
	// OnContinuationYielded fires when a stage returns a non-nil Continuation.
	OnContinuationYielded func(ContinuationObservation)
	// OnContinuationResumed fires when the pipeline resumes after a continuation completes.
	OnContinuationResumed func(ContinuationObservation)
	// OnContinuationCompleted fires when a continuation completes successfully and the next stage begins.
	OnContinuationCompleted func(ContinuationObservation)
	// OnContinuationFailed fires when a continuation resolves with Fail.
	OnContinuationFailed func(ContinuationObservation)
	// OnContinuationCancelled fires when a continuation resolves with Cancel (including deadline/context).
	OnContinuationCancelled func(ContinuationObservation)
	// OnContinuationLate fires when Complete/Fail/Cancel is called after the continuation was already resolved.
	OnContinuationLate func(ContinuationObservation)
}

func continuationObsFromExec(
	id ContinuationID,
	exec StageExecutionContext,
	yieldedFor, resumeQueueWait time.Duration,
	outcome RequestOutcome,
	failureKind FailureKind,
	err error,
) ContinuationObservation {
	return ContinuationObservation{
		ContinuationID:  id,
		RequestID:       exec.RequestID,
		Key:             exec.Key,
		Lane:            Lane(exec.Lane),
		ShardID:         exec.ShardID,
		Transport:       exec.Transport,
		Operation:       exec.Operation,
		Stage:           exec.Stage,
		StageIndex:      exec.StageIndex,
		StageCount:      exec.StageCount,
		Attempt:         exec.Attempt,
		YieldedFor:      yieldedFor,
		ResumeQueueWait: resumeQueueWait,
		Outcome:         outcome,
		FailureKind:     failureKind,
		Err:             err,
	}
}

func (q *Queue) emitContinuationYielded(obs ContinuationObservation) {
	if !q.requestHooksEnabled() {
		return
	}
	hooks := q.config.Observability.Hooks.Request.Continuation
	if hooks.OnContinuationYielded == nil {
		return
	}
	callHook(func() { hooks.OnContinuationYielded(obs) })
}

func (q *Queue) emitContinuationResumed(obs ContinuationObservation) {
	if !q.requestHooksEnabled() {
		return
	}
	hooks := q.config.Observability.Hooks.Request.Continuation
	if hooks.OnContinuationResumed == nil {
		return
	}
	callHook(func() { hooks.OnContinuationResumed(obs) })
}

func (q *Queue) emitContinuationCompleted(obs ContinuationObservation) {
	if !q.requestHooksEnabled() {
		return
	}
	hooks := q.config.Observability.Hooks.Request.Continuation
	if hooks.OnContinuationCompleted == nil {
		return
	}
	callHook(func() { hooks.OnContinuationCompleted(obs) })
}

func (q *Queue) emitContinuationFailed(obs ContinuationObservation) {
	if !q.requestHooksEnabled() {
		return
	}
	hooks := q.config.Observability.Hooks.Request.Continuation
	if hooks.OnContinuationFailed == nil {
		return
	}
	callHook(func() { hooks.OnContinuationFailed(obs) })
}

func (q *Queue) emitContinuationCancelled(obs ContinuationObservation) {
	if !q.requestHooksEnabled() {
		return
	}
	hooks := q.config.Observability.Hooks.Request.Continuation
	if hooks.OnContinuationCancelled == nil {
		return
	}
	callHook(func() { hooks.OnContinuationCancelled(obs) })
}

func (q *Queue) emitContinuationLate(obs ContinuationObservation) {
	if !q.requestHooksEnabled() {
		return
	}
	hooks := q.config.Observability.Hooks.Request.Continuation
	if hooks.OnContinuationLate == nil {
		return
	}
	callHook(func() { hooks.OnContinuationLate(obs) })
}
