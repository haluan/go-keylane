// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "time"

// StageObservation is a snapshot of one pipeline stage execution for observability hooks.
type StageObservation struct {
	RequestID string
	Key       string
	Lane      Lane
	ShardID   int

	Transport string
	Operation string

	Stage StageName

	Outcome     RequestOutcome
	FailureKind FailureKind

	QueueWaitDuration time.Duration
	StageDuration     time.Duration
	DeadlineRemaining time.Duration
}

func newStageObservation(
	meta RequestMeta,
	stage StageMeta,
	shardID int,
	queueWait, stageDur, deadlineRemaining time.Duration,
	err error,
	policy FailurePolicy,
) StageObservation {
	op := meta.Operation
	if stage.Operation != "" {
		op = stage.Operation
	}
	failure := classifyFailureWithPolicy(err, policy)
	return StageObservation{
		RequestID:         meta.RequestID,
		Key:               meta.Key,
		Lane:              meta.Lane,
		ShardID:           shardID,
		Transport:         meta.Transport,
		Operation:         op,
		Stage:             stage.Name,
		Outcome:           classifyRequestOutcome(err),
		FailureKind:       failure.Kind,
		QueueWaitDuration: queueWait,
		StageDuration:     stageDur,
		DeadlineRemaining: deadlineRemaining,
	}
}

func (q *Queue) newStageObservation(
	meta RequestMeta,
	stage StageMeta,
	shardID int,
	queueWait, stageDur, deadlineRemaining time.Duration,
	err error,
) StageObservation {
	if q == nil {
		return newStageObservation(meta, stage, shardID, queueWait, stageDur, deadlineRemaining, err, FailurePolicy{})
	}
	return newStageObservation(meta, stage, shardID, queueWait, stageDur, deadlineRemaining, err, q.failurePolicy)
}
