// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "time"

// StageObservation is a snapshot of one pipeline stage execution for observability hooks.
type StageObservation struct {
	Execution StageExecutionContext

	RequestID string
	Key       string
	// KeyHash is always set when Key was non-empty at emission time (even when Key is redacted).
	KeyHash uint64
	Lane    Lane
	ShardID int

	Transport string
	Operation string

	Stage StageName

	Outcome     RequestOutcome
	FailureKind FailureKind

	QueueWaitDuration time.Duration
	StageDuration     time.Duration
	DeadlineRemaining time.Duration
}

func newStageObservationFromExecution(
	exec StageExecutionContext,
	stageDur time.Duration,
	err error,
	policy FailurePolicy,
) StageObservation {
	op := exec.Operation
	if exec.Stage.Operation != "" {
		op = exec.Stage.Operation
	}
	failure := classifyFailureWithPolicy(err, policy)
	return StageObservation{
		Execution:         exec,
		RequestID:         exec.RequestID,
		Key:               exec.Key,
		Lane:              exec.Lane,
		ShardID:           exec.ShardID,
		Transport:         exec.Transport,
		Operation:         op,
		Stage:             exec.Stage.Name,
		Outcome:           classifyRequestOutcome(err),
		FailureKind:       failure.Kind,
		QueueWaitDuration: exec.QueueWait,
		StageDuration:     stageDur,
		DeadlineRemaining: exec.Deadline.Remaining,
	}
}

func (q *Queue) newStageObservationFromExecution(
	exec StageExecutionContext,
	stageDur time.Duration,
	err error,
) StageObservation {
	if q == nil {
		return newStageObservationFromExecution(exec, stageDur, err, FailurePolicy{})
	}
	return newStageObservationFromExecution(exec, stageDur, err, q.failurePolicy)
}
