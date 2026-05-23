// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"time"
)

// RequestOutcome classifies how a request finished.
type RequestOutcome string

const (
	RequestOutcomeCompleted         RequestOutcome = "completed"
	RequestOutcomeFailed            RequestOutcome = "failed"
	RequestOutcomeCancelled         RequestOutcome = "cancelled"
	RequestOutcomeTimedOut          RequestOutcome = "timed_out"
	RequestOutcomeRejected          RequestOutcome = "rejected"
	RequestOutcomeAdmissionRejected RequestOutcome = "admission_rejected"
	RequestOutcomeOverloadRejected  RequestOutcome = "overload_rejected"
	RequestOutcomeOverloadShed      RequestOutcome = "overload_shed"
	RequestOutcomeOverloadDegraded  RequestOutcome = "overload_degraded"
)

// RequestObservation is a snapshot of request execution for observability hooks.
type RequestObservation struct {
	RequestID string
	Key       string
	Lane      Lane
	ShardID   int

	Transport string
	Operation string

	QueueWait time.Duration
	Run       time.Duration

	Outcome RequestOutcome
	Err     error
}

// classifyRequestOutcome maps an error to a request outcome.
func classifyRequestOutcome(err error) RequestOutcome {
	if err == nil {
		return RequestOutcomeCompleted
	}
	if errors.Is(err, ErrAdmissionRejected) {
		return RequestOutcomeAdmissionRejected
	}
	if errors.Is(err, ErrOverloadRejected) {
		return RequestOutcomeOverloadRejected
	}
	if errors.Is(err, ErrOverloadShed) {
		return RequestOutcomeOverloadShed
	}
	if errors.Is(err, ErrOverloadDegraded) {
		return RequestOutcomeOverloadDegraded
	}
	if errors.Is(err, ErrQueueFull) || errors.Is(err, ErrStopped) ||
		errors.Is(err, ErrNotStarted) || errors.Is(err, ErrQueueNotStarted) {
		return RequestOutcomeRejected
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return RequestOutcomeTimedOut
	}
	if errors.Is(err, context.Canceled) {
		return RequestOutcomeCancelled
	}
	return RequestOutcomeFailed
}

// ObservationForError builds a request observation when queue wait and run are unknown.
func ObservationForError(q *Queue, meta RequestMeta, err error) RequestObservation {
	shardID := 0
	if q != nil {
		shardID = q.ShardIDForKey(meta.Key)
	}
	return newRequestObservation(meta, shardID, 0, 0, err)
}

func newRequestObservation(
	meta RequestMeta,
	shardID int,
	queueWait, run time.Duration,
	err error,
) RequestObservation {
	return RequestObservation{
		RequestID: meta.RequestID,
		Key:       meta.Key,
		Lane:      meta.Lane,
		ShardID:   shardID,
		Transport: meta.Transport,
		Operation: meta.Operation,
		QueueWait: queueWait,
		Run:       run,
		Outcome:   classifyRequestOutcome(err),
		Err:       err,
	}
}

func (q *Queue) newRequestObservation(
	meta RequestMeta,
	shardID int,
	queueWait, run time.Duration,
	err error,
) RequestObservation {
	return newRequestObservation(meta, shardID, queueWait, run, err)
}
