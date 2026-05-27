// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "time"

// RetryEventKind classifies retry/failure observability events.
type RetryEventKind string

const (
	RetryEventAttemptStarted     RetryEventKind = "attempt_started"
	RetryEventFailureClassified  RetryEventKind = "failure_classified"
	RetryEventScheduled          RetryEventKind = "scheduled"
	RetryEventSuppressed         RetryEventKind = "suppressed"
	RetryEventSafetySuppressed   RetryEventKind = "safety_suppressed"
	RetryEventDeadlineStopped    RetryEventKind = "deadline_stopped"
	RetryEventMaxAttemptsStopped RetryEventKind = "max_attempts_stopped"
	RetryEventExhausted          RetryEventKind = "exhausted"
	RetryEventContextCancelled   RetryEventKind = "context_cancelled"
	RetryEventRetryStopped       RetryEventKind = "retry_stopped"
	RetryEventSucceeded          RetryEventKind = "succeeded"
)

// RetryEvent carries retry/failure observability metadata for hooks.
// Key is omitted by default; use KeyHash for correlation. Do not use raw Key as a metric label.
type RetryEvent struct {
	Kind RetryEventKind

	Key     string
	KeyHash uint64
	Lane    Lane
	ShardID int

	Attempt int
	Delay   time.Duration

	FailureKind FailureKind
	Failure     Failure

	RetryDecisionReason RetryDecisionReason

	SafetyReason      RetrySafetyDecisionReason
	SuppressionReason RetrySuppressionReason

	DeadlineBudget DeadlineBudget
	Pressure       Pressure

	IdempotencyScope     string
	IdempotencyOperation string

	Time time.Time
}

// retryObsRecord is the hot-path observability payload passed from runWithRetry.
type retryObsRecord struct {
	Kind RetryEventKind

	Key     string
	Lane    Lane
	ShardID int

	Attempt int
	Delay   time.Duration

	Failure Failure

	RetryReason       RetryDecisionReason
	SafetyReason      RetrySafetyDecisionReason
	SuppressionReason RetrySuppressionReason

	IdempotencyScope     string
	IdempotencyOperation string

	Budget   DeadlineBudget
	Pressure Pressure
}

// retryObserver records retry observability without coupling runWithRetry to *Queue.
type retryObserver func(retryObsRecord)

func (q *Queue) retryObserver() retryObserver {
	if q == nil {
		return nil
	}
	return q.recordRetryObs
}

func (q *Queue) recordRetryObs(rec retryObsRecord) {
	if q == nil {
		return
	}
	q.retryObs.record(rec)
	if q.hooksEnabled() && q.config.Observability.Hooks.Retry.OnRetryEvent != nil {
		q.emitRetryEvent(retryEventFromRecord(rec))
	}
}

func retryEventFromRecord(rec retryObsRecord) RetryEvent {
	kind := rec.Failure.Kind
	if kind == "" {
		kind = FailureUnknown
	}
	return RetryEvent{
		Kind:                 rec.Kind,
		Key:                  rec.Key,
		Lane:                 rec.Lane,
		ShardID:              rec.ShardID,
		Attempt:              rec.Attempt,
		Delay:                rec.Delay,
		FailureKind:          kind,
		Failure:              rec.Failure,
		RetryDecisionReason:  rec.RetryReason,
		SafetyReason:         rec.SafetyReason,
		SuppressionReason:    rec.SuppressionReason,
		DeadlineBudget:       rec.Budget,
		Pressure:             rec.Pressure,
		IdempotencyScope:     rec.IdempotencyScope,
		IdempotencyOperation: rec.IdempotencyOperation,
		Time:                 time.Now(),
	}
}
