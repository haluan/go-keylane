// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"time"

	"github.com/haluan/go-keylane/internal/core"
)

func (q *Queue) hooksEnabled() bool {
	return q.config.Observability.EnableHooks
}

func (q *Queue) emitQuotaChange(e QuotaChangeEvent) {
	if !q.hooksEnabled() {
		return
	}
	if h := q.config.Observability.Hooks.OnQuotaChange; h != nil {
		callHook(func() { h(e) })
	}
}

func (q *Queue) emitOverloadPolicy(e OverloadPolicyEvent) {
	if !q.hooksEnabled() {
		return
	}
	if h := q.config.Observability.Hooks.OnOverloadPolicyDecision; h != nil {
		callHook(func() { h(e) })
	}
}

func (q *Queue) recordManualQuotaChange(lane Lane) {
	if id, ok := q.reg.Lookup(string(lane)); ok {
		q.sched.RecordManualQuotaChange(id, time.Now().UnixNano())
	}
}

func (q *Queue) emitQuotaChangesFromPolicyDiff(before, after QuotaPolicySnapshot, source QuotaChangeSource, reason string, policyVer uint64) {
	for lane, newQ := range after.LaneQuotas {
		oldQ, ok := before.LaneQuotas[lane]
		if !ok {
			oldQ = before.DefaultQuota
		}
		if oldQ == newQ {
			continue
		}
		q.emitQuotaChange(QuotaChangeEvent{
			Time:          time.Now(),
			Lane:          lane,
			OldQuota:      int(oldQ),
			NewQuota:      int(newQ),
			Source:        source,
			Reason:        reason,
			PolicyVersion: policyVer,
			QuotaVersion:  after.Version,
		})
		if source == QuotaChangeManual {
			q.recordManualQuotaChange(lane)
		}
	}
}

func overloadPolicyEventFromDecision(d OverloadDecision, globalPressure float64) OverloadPolicyEvent {
	return OverloadPolicyEvent{
		Time:           time.Now(),
		Lane:           d.Lane,
		Class:          d.Class,
		Action:         d.Action,
		Reason:         d.Reason,
		RetryAfter:     d.RetryAfter,
		BackoffHint:    d.Backoff,
		GlobalPressure: globalPressure,
		LanePressure:   d.Pressure,
		QueueDepth:     d.LaneDepth,
		MaxQueueDepth:  d.MaxDepth,
		PolicyVersion:  d.PolicyVersion,
	}
}

func overloadPolicyEventFromCore(lane Lane, r core.OverloadEvalResult, globalPressure float64) OverloadPolicyEvent {
	return OverloadPolicyEvent{
		Time:           time.Now(),
		Lane:           lane,
		Class:          LaneClass(r.Class),
		Action:         OverloadAction(r.Action),
		Reason:         OverloadReason(r.Reason),
		RetryAfter:     r.RetryAfter,
		BackoffHint:    backoffHintFromCore(r.RetryAfter, r.MinBackoff, r.MaxBackoff, r.Jitter),
		GlobalPressure: globalPressure,
		LanePressure:   r.Pressure,
		QueueDepth:     r.LaneDepth,
		MaxQueueDepth:  r.MaxDepth,
		PolicyVersion:  r.PolicyVersion,
	}
}

func (q *Queue) emitHotKeyCandidate(c HotKeyCandidate) {
	if !q.hooksEnabled() {
		return
	}
	if h := q.config.Observability.Hooks.OnHotKeyCandidate; h != nil {
		callHook(func() { h(HotKeyCandidateEvent{Time: time.Now(), Candidate: c}) })
	}
}

func (q *Queue) emitShardPressureSummary(summary PressureSummarySnapshot) {
	if !q.hooksEnabled() {
		return
	}
	if h := q.config.Observability.Hooks.OnShardPressureSummary; h != nil {
		callHook(func() { h(ShardPressureSummaryEvent{Time: time.Now(), Summary: summary}) })
	}
}

func (q *Queue) emitScaleSignal(sig ScaleSignal) {
	if !q.hooksEnabled() || !sig.DiagnosticsEnabled {
		return
	}
	if h := q.config.Observability.Hooks.OnScaleSignal; h != nil {
		callHook(func() { h(ScaleSignalEvent{Time: time.Now(), Signal: sig}) })
	}
}

func (q *Queue) emitHotKeyCandidatesFromSnapshot(snap DebugSnapshot) {
	if !q.hooksEnabled() || q.config.Observability.Hooks.OnHotKeyCandidate == nil {
		return
	}
	for _, sh := range snap.Shards {
		if sh.HotKeyCandidate != nil {
			q.emitHotKeyCandidate(*sh.HotKeyCandidate)
		}
		for _, c := range sh.HotKeyCandidates {
			q.emitHotKeyCandidate(c)
		}
	}
}

func laneAdaptiveStatsFromCore(s core.LaneAdaptiveStatsSnapshot) LaneAdaptiveStats {
	var lastChange time.Time
	if s.LastQuotaChangeUnix > 0 {
		lastChange = time.Unix(0, s.LastQuotaChangeUnix)
	}
	return LaneAdaptiveStats{
		Lane:                  Lane(s.LaneName),
		KeepTotal:             s.KeepTotal,
		RejectTotal:           s.RejectTotal,
		ShedTotal:             s.ShedTotal,
		DegradeTotal:          s.DegradeTotal,
		QueueFullTotal:        s.QueueFullTotal,
		QuotaChangeTotal:      s.QuotaChangeTotal,
		AdaptiveIncreaseTotal: s.AdaptiveIncreaseTotal,
		AdaptiveDecreaseTotal: s.AdaptiveDecreaseTotal,
		AdaptiveHoldTotal:     s.AdaptiveHoldTotal,
		LastQuotaChange:       lastChange,
		LastDecision:          s.LastAdaptiveReason,
	}
}
