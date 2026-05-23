// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"errors"
	"sync/atomic"
)

// laneCounters holds atomic metrics counters for a specific lane.
type laneCounters struct {
	// StatsGCPressure cumulative counters.
	submitted                 atomic.Uint64
	accepted                  atomic.Uint64
	rejected                  atomic.Uint64
	pressureAdmissionRejected atomic.Uint64
	overloadReject            atomic.Uint64
	overloadShed              atomic.Uint64
	overloadDegrade           atomic.Uint64
	canceled                  atomic.Uint64
	panicked                  atomic.Uint64

	// StatsGCPressure queue wait (always on).
	gcQueueWaitCount      atomic.Uint64
	gcQueueWaitTotalNanos atomic.Uint64
	gcQueueWaitMaxNanos   atomic.Uint64

	// StatsGCPressure run duration (always on).
	gcRunCount      atomic.Uint64
	gcRunTotalNanos atomic.Uint64
	gcRunMaxNanos   atomic.Uint64

	// Stats() counters (successful enqueue semantics for submittedTotal, non-GC Pressure).
	submittedTotal      atomic.Int64
	completedTotal      atomic.Int64
	failedTotal         atomic.Int64
	queueFullTotal      atomic.Int64
	queueWaitTotalNanos atomic.Int64
	queueWaitCount      atomic.Int64

	quotaChangeTotal      atomic.Uint64
	adaptiveIncreaseTotal atomic.Uint64
	adaptiveDecreaseTotal atomic.Uint64
	adaptiveHoldTotal     atomic.Uint64
	lastQuotaChangeUnix   atomic.Int64
	lastAdaptiveReason    atomic.Uint64
}

type LaneAdaptiveStatsSnapshot struct {
	LaneName string

	KeepTotal      uint64
	RejectTotal    uint64
	ShedTotal      uint64
	DegradeTotal   uint64
	QueueFullTotal uint64

	QuotaChangeTotal      uint64
	AdaptiveIncreaseTotal uint64
	AdaptiveDecreaseTotal uint64
	AdaptiveHoldTotal     uint64

	LastQuotaChangeUnix int64
	LastAdaptiveReason  QuotaAdjustmentReason
}

func (c *laneCounters) snapshotAdaptiveStats(laneName string) LaneAdaptiveStatsSnapshot {
	counters := c.snapshotGCPressure()
	reason := QuotaAdjustmentReason("")
	if idx := c.lastAdaptiveReason.Load(); idx != 0 {
		reason = reasonFromIndex(idx)
	}
	return LaneAdaptiveStatsSnapshot{
		LaneName:              laneName,
		KeepTotal:             counters.Accepted,
		RejectTotal:           counters.AdmissionRejected + counters.OverloadRejected,
		ShedTotal:             counters.OverloadShed,
		DegradeTotal:          counters.OverloadDegrade,
		QueueFullTotal:        counters.QueueFull,
		QuotaChangeTotal:      c.quotaChangeTotal.Load(),
		AdaptiveIncreaseTotal: c.adaptiveIncreaseTotal.Load(),
		AdaptiveDecreaseTotal: c.adaptiveDecreaseTotal.Load(),
		AdaptiveHoldTotal:     c.adaptiveHoldTotal.Load(),
		LastQuotaChangeUnix:   c.lastQuotaChangeUnix.Load(),
		LastAdaptiveReason:    reason,
	}
}

func (c *laneCounters) recordLastAdaptiveReason(reason QuotaAdjustmentReason) {
	c.lastAdaptiveReason.Store(reasonIndex(reason))
}

func (c *laneCounters) recordManualQuotaChange(nowUnix int64) {
	c.quotaChangeTotal.Add(1)
	c.lastQuotaChangeUnix.Store(nowUnix)
}

func (c *laneCounters) recordAdaptiveDecision(action QuotaAdjustmentAction, reason QuotaAdjustmentReason, nowUnix int64, countHold bool) {
	c.recordLastAdaptiveReason(reason)
	switch action {
	case QuotaAdjustmentIncrease:
		c.adaptiveIncreaseTotal.Add(1)
	case QuotaAdjustmentDecrease:
		c.adaptiveDecreaseTotal.Add(1)
	case QuotaAdjustmentHold:
		if countHold {
			c.adaptiveHoldTotal.Add(1)
		}
	}
	if action == QuotaAdjustmentIncrease || action == QuotaAdjustmentDecrease {
		c.quotaChangeTotal.Add(1)
		c.lastQuotaChangeUnix.Store(nowUnix)
	}
}

func reasonIndex(r QuotaAdjustmentReason) uint64 {
	for i, reason := range allQuotaAdjustmentReasons {
		if reason == r {
			return uint64(i + 1)
		}
	}
	return 0
}

func reasonFromIndex(idx uint64) QuotaAdjustmentReason {
	if idx == 0 || idx > uint64(len(allQuotaAdjustmentReasons)) {
		return ""
	}
	return allQuotaAdjustmentReasons[idx-1]
}

var allQuotaAdjustmentReasons = []QuotaAdjustmentReason{
	QuotaReasonNone,
	QuotaReasonCriticalQueueWaitHigh,
	QuotaReasonNormalQueueWaitHigh,
	QuotaReasonGlobalPressureHigh,
	QuotaReasonBackgroundPressureHigh,
	QuotaReasonBestEffortPressureHigh,
	QuotaReasonRunTimeTooHigh,
	QuotaReasonCooldownActive,
	QuotaReasonAtMinBound,
	QuotaReasonAtMaxBound,
	QuotaReasonInsufficientSamples,
	QuotaReasonIncreaseDisabled,
	QuotaReasonDecreaseDisabled,
	QuotaReasonWarmupActive,
	QuotaReasonNeutralPressure,
	QuotaReasonBackgroundQueueWaitHigh,
	QuotaReasonQueueFull,
	QuotaReasonUpdateFailed,
}

func (c *laneCounters) snapshotGCPressureQueueWait() QueueWaitStatsGCPressure {
	return QueueWaitStatsGCPressure{
		Count:      c.gcQueueWaitCount.Load(),
		TotalNanos: c.gcQueueWaitTotalNanos.Load(),
		MaxNanos:   c.gcQueueWaitMaxNanos.Load(),
	}
}

func (c *laneCounters) snapshotGCPressureRun() RunStatsGCPressure {
	return RunStatsGCPressure{
		Count:      c.gcRunCount.Load(),
		TotalNanos: c.gcRunTotalNanos.Load(),
		MaxNanos:   c.gcRunMaxNanos.Load(),
	}
}

// snapshotGCPressure returns a read-only copy of cumulative lane counters for StatsGCPressure.
// Admission fields are loaded with Submitted last so a concurrent enqueue that increments
// Submitted before Accepted/Rejected does not produce Submitted < Accepted+Rejected in
// the snapshot (best-effort; not a global point-in-time atomic snapshot).
func (c *laneCounters) snapshotGCPressure() LaneCountersGCPressure {
	completed := uint64(c.completedTotal.Load())
	failed := uint64(c.failedTotal.Load())
	canceled := c.canceled.Load()
	panicked := c.panicked.Load()
	queueFull := uint64(c.queueFullTotal.Load())
	admissionRejected := c.pressureAdmissionRejected.Load()
	overloadRejected := c.overloadReject.Load()
	overloadShed := c.overloadShed.Load()
	overloadDegrade := c.overloadDegrade.Load()
	accepted := c.accepted.Load()
	rejected := c.rejected.Load()
	submitted := c.submitted.Load()
	return LaneCountersGCPressure{
		Submitted:         submitted,
		Accepted:          accepted,
		Rejected:          rejected,
		AdmissionRejected: admissionRejected,
		OverloadRejected:  overloadRejected,
		OverloadShed:      overloadShed,
		OverloadDegrade:   overloadDegrade,
		Completed:         completed,
		Failed:            failed,
		QueueFull:         queueFull,
		Canceled:          canceled,
		Panicked:          panicked,
	}
}

// recordLaneAdmissionAttempt increments Submitted for every enqueue attempt.
func (c *laneCounters) recordLaneAdmissionAttempt() {
	c.submitted.Add(1)
}

// recordLaneAdmissionResult updates Accepted/Rejected and v1 counters after enqueueIntoShard.
func (c *laneCounters) recordLaneAdmissionResult(err error) {
	if err == nil {
		c.accepted.Add(1)
		c.submittedTotal.Add(1)
		return
	}
	c.rejected.Add(1)
	if errors.Is(err, ErrQueueFull) {
		c.queueFullTotal.Add(1)
	}
}

// recordLaneAdmissionRejected increments Rejected without a shard enqueue attempt result.
func (c *laneCounters) recordLaneAdmissionRejected() {
	c.rejected.Add(1)
}

// recordPressureAdmissionRejected increments Rejected and AdmissionRejected for
// pressure-based admission control rejections before enqueue.
func (c *laneCounters) recordPressureAdmissionRejected() {
	c.rejected.Add(1)
	c.pressureAdmissionRejected.Add(1)
}
