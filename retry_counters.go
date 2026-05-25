// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "sync/atomic"

const (
	failureKindBucketCount            = 10
	retryDecisionReasonBucketCount    = 9
	retrySafetyReasonBucketCount      = 9
	retrySuppressionReasonBucketCount = 14
)

// FailureKindCounter counts failures by FailureKind.
type FailureKindCounter struct {
	Kind  FailureKind
	Count uint64
}

// RetryReasonCounter counts retry decision outcomes by reason.
type RetryReasonCounter struct {
	Reason RetryDecisionReason
	Count  uint64
}

// RetrySafetyReasonCounter counts duplicate-safety outcomes by reason.
type RetrySafetyReasonCounter struct {
	Reason RetrySafetyDecisionReason
	Count  uint64
}

// RetrySuppressionReasonCounter counts suppression outcomes by reason.
type RetrySuppressionReasonCounter struct {
	Reason RetrySuppressionReason
	Count  uint64
}

// RetryFailureSnapshot is a pull-based view of retry/failure counters (may allocate slices).
type RetryFailureSnapshot struct {
	AttemptsTotal                uint64
	RetriesScheduledTotal        uint64
	RetriesSuppressedTotal       uint64
	RetrySafetySuppressedTotal   uint64
	RetryDeadlineStoppedTotal    uint64
	RetryMaxAttemptsStoppedTotal uint64
	RetryExhaustedTotal          uint64

	FailuresTotal      uint64
	TimeoutsTotal      uint64
	CancellationsTotal uint64

	ByFailureKind       []FailureKindCounter
	ByRetryReason       []RetryReasonCounter
	BySuppressionReason []RetrySuppressionReasonCounter
	BySafetyReason      []RetrySafetyReasonCounter
}

type retryCounters struct {
	attemptsTotal                atomic.Uint64
	retriesScheduledTotal        atomic.Uint64
	retriesSuppressedTotal       atomic.Uint64
	retrySafetySuppressedTotal   atomic.Uint64
	retryDeadlineStoppedTotal    atomic.Uint64
	retryMaxAttemptsStoppedTotal atomic.Uint64
	retryExhaustedTotal          atomic.Uint64

	failuresTotal      atomic.Uint64
	timeoutsTotal      atomic.Uint64
	cancellationsTotal atomic.Uint64

	byFailureKind       [failureKindBucketCount]atomic.Uint64
	byRetryReason       [retryDecisionReasonBucketCount]atomic.Uint64
	bySafetyReason      [retrySafetyReasonBucketCount]atomic.Uint64
	bySuppressionReason [retrySuppressionReasonBucketCount]atomic.Uint64
}

func failureKindIndex(k FailureKind) int {
	switch k {
	case FailureNone:
		return 0
	case FailureRetryable:
		return 1
	case FailurePermanent:
		return 2
	case FailureTimeout:
		return 3
	case FailureCancelled:
		return 4
	case FailureOverloaded:
		return 5
	case FailureRejected:
		return 6
	case FailureDeadlineExhausted:
		return 7
	case FailurePanic:
		return 8
	default:
		return 9
	}
}

func retryDecisionReasonIndex(r RetryDecisionReason) int {
	switch r {
	case RetryDecisionNone:
		return 0
	case RetryDecisionDisabled:
		return 1
	case RetryDecisionRetryableFailure:
		return 2
	case RetryDecisionPermanentFailure:
		return 3
	case RetryDecisionMaxAttempts:
		return 4
	case RetryDecisionContextCancelled:
		return 5
	case RetryDecisionDeadlineExhausted:
		return 6
	case RetryDecisionBudgetTooSmall:
		return 7
	default:
		return 8
	}
}

func retrySafetyReasonIndex(r RetrySafetyDecisionReason) int {
	switch r {
	case RetrySafetyDecisionSafe:
		return 0
	case RetrySafetyDecisionUnsafe:
		return 1
	case RetrySafetyDecisionMissingKey:
		return 2
	case RetrySafetyDecisionHookAllowed:
		return 3
	case RetrySafetyDecisionHookRejected:
		return 4
	case RetrySafetyDecisionHookFailed:
		return 5
	case RetrySafetyDecisionNoHook:
		return 6
	case RetrySafetyDecisionExplicitOverride:
		return 7
	default:
		return 8
	}
}

func retrySuppressionReasonIndex(r RetrySuppressionReason) int {
	switch r {
	case RetrySuppressionNone:
		return 0
	case RetrySuppressionDisabled:
		return 1
	case RetrySuppressionGlobalPressure:
		return 2
	case RetrySuppressionGlobalOverload:
		return 3
	case RetrySuppressionLanePressure:
		return 4
	case RetrySuppressionShardPressure:
		return 5
	case RetrySuppressionOverloadFailure:
		return 6
	case RetrySuppressionAdmissionFailure:
		return 7
	case RetrySuppressionPerKeyAdmission:
		return 8
	case RetrySuppressionHotKey:
		return 9
	case RetrySuppressionScaleOutRecommended:
		return 10
	case RetrySuppressionHookRejected:
		return 11
	case RetrySuppressionHookFailed:
		return 12
	default:
		return 13
	}
}

func (c *retryCounters) recordFailureKind(kind FailureKind) {
	if kind == FailureNone {
		return
	}
	c.failuresTotal.Add(1)
	switch kind {
	case FailureTimeout, FailureDeadlineExhausted:
		c.timeoutsTotal.Add(1)
	case FailureCancelled:
		c.cancellationsTotal.Add(1)
	}
	if i := failureKindIndex(kind); i >= 0 && i < failureKindBucketCount {
		c.byFailureKind[i].Add(1)
	}
}

func (c *retryCounters) record(rec retryObsRecord) {
	switch rec.Kind {
	case RetryEventAttemptStarted:
		c.attemptsTotal.Add(1)
	case RetryEventFailureClassified:
		if rec.Failure.Kind != FailureNone {
			c.recordFailureKind(rec.Failure.Kind)
		}
	case RetryEventScheduled:
		c.retriesScheduledTotal.Add(1)
	case RetryEventSuppressed:
		c.retriesSuppressedTotal.Add(1)
	case RetryEventSafetySuppressed:
		c.retrySafetySuppressedTotal.Add(1)
	case RetryEventDeadlineStopped:
		c.retryDeadlineStoppedTotal.Add(1)
	case RetryEventMaxAttemptsStopped:
		c.retryMaxAttemptsStoppedTotal.Add(1)
	case RetryEventExhausted:
		c.retryExhaustedTotal.Add(1)
	case RetryEventSucceeded:
		// no dedicated total; success is visible via Final state on trace
	}
	if rec.RetryReason != "" {
		if i := retryDecisionReasonIndex(rec.RetryReason); i >= 0 {
			c.byRetryReason[i].Add(1)
		}
	}
	if rec.SafetyReason != "" {
		if i := retrySafetyReasonIndex(rec.SafetyReason); i >= 0 {
			c.bySafetyReason[i].Add(1)
		}
	}
	if rec.SuppressionReason != "" && rec.SuppressionReason != RetrySuppressionNone {
		if i := retrySuppressionReasonIndex(rec.SuppressionReason); i >= 0 {
			c.bySuppressionReason[i].Add(1)
		}
	}
}

func (c *retryCounters) snapshot() RetryFailureSnapshot {
	snap := RetryFailureSnapshot{
		AttemptsTotal:                c.attemptsTotal.Load(),
		RetriesScheduledTotal:        c.retriesScheduledTotal.Load(),
		RetriesSuppressedTotal:       c.retriesSuppressedTotal.Load(),
		RetrySafetySuppressedTotal:   c.retrySafetySuppressedTotal.Load(),
		RetryDeadlineStoppedTotal:    c.retryDeadlineStoppedTotal.Load(),
		RetryMaxAttemptsStoppedTotal: c.retryMaxAttemptsStoppedTotal.Load(),
		RetryExhaustedTotal:          c.retryExhaustedTotal.Load(),
		FailuresTotal:                c.failuresTotal.Load(),
		TimeoutsTotal:                c.timeoutsTotal.Load(),
		CancellationsTotal:           c.cancellationsTotal.Load(),
	}
	snap.ByFailureKind = snapshotFailureKinds(c)
	snap.ByRetryReason = snapshotRetryReasons(c)
	snap.BySafetyReason = snapshotSafetyReasons(c)
	snap.BySuppressionReason = snapshotSuppressionReasons(c)
	return snap
}

func snapshotFailureKinds(c *retryCounters) []FailureKindCounter {
	kinds := []FailureKind{
		FailureNone, FailureRetryable, FailurePermanent, FailureTimeout, FailureCancelled,
		FailureOverloaded, FailureRejected, FailureDeadlineExhausted, FailurePanic, FailureUnknown,
	}
	out := make([]FailureKindCounter, 0, failureKindBucketCount)
	for i, k := range kinds {
		n := c.byFailureKind[i].Load()
		if n > 0 {
			out = append(out, FailureKindCounter{Kind: k, Count: n})
		}
	}
	return out
}

func snapshotRetryReasons(c *retryCounters) []RetryReasonCounter {
	reasons := []RetryDecisionReason{
		RetryDecisionNone, RetryDecisionDisabled, RetryDecisionRetryableFailure,
		RetryDecisionPermanentFailure, RetryDecisionMaxAttempts, RetryDecisionContextCancelled,
		RetryDecisionDeadlineExhausted, RetryDecisionBudgetTooSmall, "",
	}
	out := make([]RetryReasonCounter, 0, len(reasons))
	for i, r := range reasons {
		n := c.byRetryReason[i].Load()
		if n > 0 && r != "" {
			out = append(out, RetryReasonCounter{Reason: r, Count: n})
		}
	}
	return out
}

func snapshotSafetyReasons(c *retryCounters) []RetrySafetyReasonCounter {
	reasons := []RetrySafetyDecisionReason{
		RetrySafetyDecisionSafe, RetrySafetyDecisionUnsafe, RetrySafetyDecisionMissingKey,
		RetrySafetyDecisionHookAllowed, RetrySafetyDecisionHookRejected, RetrySafetyDecisionHookFailed,
		RetrySafetyDecisionNoHook, RetrySafetyDecisionExplicitOverride, "",
	}
	out := make([]RetrySafetyReasonCounter, 0, len(reasons))
	for i, r := range reasons {
		n := c.bySafetyReason[i].Load()
		if n > 0 && r != "" {
			out = append(out, RetrySafetyReasonCounter{Reason: r, Count: n})
		}
	}
	return out
}

func snapshotSuppressionReasons(c *retryCounters) []RetrySuppressionReasonCounter {
	reasons := []RetrySuppressionReason{
		RetrySuppressionNone, RetrySuppressionDisabled, RetrySuppressionGlobalPressure,
		RetrySuppressionGlobalOverload, RetrySuppressionLanePressure, RetrySuppressionShardPressure,
		RetrySuppressionOverloadFailure, RetrySuppressionAdmissionFailure, RetrySuppressionPerKeyAdmission,
		RetrySuppressionHotKey, RetrySuppressionScaleOutRecommended, RetrySuppressionHookRejected,
		RetrySuppressionHookFailed, "",
	}
	out := make([]RetrySuppressionReasonCounter, 0, len(reasons))
	for i, r := range reasons {
		n := c.bySuppressionReason[i].Load()
		if n > 0 && r != "" {
			out = append(out, RetrySuppressionReasonCounter{Reason: r, Count: n})
		}
	}
	return out
}

// RetryFailureSnapshot returns cumulative retry/failure counters for the queue.
func (q *Queue) RetryFailureSnapshot() RetryFailureSnapshot {
	if q == nil {
		return RetryFailureSnapshot{}
	}
	return q.retryObs.snapshot()
}
