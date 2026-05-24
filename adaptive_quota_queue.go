// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"time"

	"github.com/haluan/go-keylane/internal/core"
)

func (q *Queue) initAdaptiveController() {
	policy := q.config.AdaptiveQuota
	NormalizeAdaptiveQuotaConfig(&policy.Config)
	cfg := adaptiveQuotaConfigToCore(policy.Config, q.config.Observability.EnableAdaptiveDecisionTracing)
	if !cfg.Enabled {
		return
	}
	q.config.AdaptiveQuota.Config = policy.Config

	initial := make(map[string]int, len(q.config.LaneQuotas))
	for lane, quota := range q.config.LaneQuotas {
		initial[string(lane)] = quota
	}
	lanes := make([]core.LaneAdaptivePolicy, len(policy.Lanes))
	for i, lp := range policy.Lanes {
		lanes[i] = laneAdaptivePolicyToCore(lp)
	}

	var hook core.AdaptiveQuotaDecisionHook
	if q.config.Observability.EnableHooks {
		hAdaptive := q.config.Observability.Hooks.OnAdaptiveQuotaDecision
		if hAdaptive != nil || q.config.Observability.Hooks.OnQuotaChange != nil {
			hook = func(d core.QuotaAdjustmentDecision, t time.Time) {
				if hAdaptive != nil {
					callHook(func() { hAdaptive(adaptiveQuotaEventFromCore(d, t)) })
				}
				if d.Reason != core.QuotaReasonUpdateFailed && d.NewQuota != d.OldQuota {
					q.emitQuotaChange(QuotaChangeEvent{
						Time:          t,
						Lane:          Lane(d.Lane),
						OldQuota:      d.OldQuota,
						NewQuota:      d.NewQuota,
						Source:        QuotaChangeAdaptive,
						Reason:        string(d.Reason),
						PolicyVersion: d.PolicyVersion,
						QuotaVersion:  d.QuotaVersion,
					})
				}
			}
		}
	}

	q.adaptive = core.NewAdaptiveQuotaController(
		q.sched,
		q.reg,
		cfg,
		lanes,
		initial,
		func(ctx context.Context, lane string, quota uint32, expectedVer uint64) (uint64, error) {
			_ = ctx
			return q.updateLaneQuotaIfVersionInternal(Lane(lane), quota, expectedVer, false)
		},
		hook,
	)
}

func adaptiveQuotaEventFromCore(d core.QuotaAdjustmentDecision, t time.Time) AdaptiveQuotaEvent {
	return AdaptiveQuotaEvent{
		Time:           t,
		Lane:           Lane(d.Lane),
		Class:          LaneClass(d.Class),
		Action:         QuotaAdjustmentAction(d.Action),
		Reason:         QuotaAdjustmentReason(d.Reason),
		OldQuota:       d.OldQuota,
		NewQuota:       d.NewQuota,
		GlobalPressure: d.GlobalPressure,
		QueueDepth:     d.QueueDepth,
		InFlight:       d.InFlight,
		QueueWaitP50:   d.QueueWaitP50,
		QueueWaitP95:   d.QueueWaitP95,
		QueueWaitP99:   d.QueueWaitP99,
		RunP50:         d.RunP50,
		RunP95:         d.RunP95,
		RunP99:         d.RunP99,
		PolicyVersion:  d.PolicyVersion,
		QuotaVersion:   d.QuotaVersion,
	}
}

// AdaptiveDebugSnapshot returns adaptive controller state and per-lane stats (copy-out).
func (q *Queue) AdaptiveDebugSnapshot() AdaptiveDebugSnapshot {
	out := AdaptiveDebugSnapshot{Enabled: false}
	if q.adaptive != nil {
		enabled, running, lastEval, tickCount, decisions, policyVer, quotaVer := q.adaptive.Snapshot()
		out.Enabled = enabled
		out.Running = running
		out.LastEvaluation = lastEval
		out.TickCount = tickCount
		out.PolicyVersion = policyVer
		out.QuotaVersion = quotaVer
		if len(decisions) > 0 {
			out.LastDecisions = make([]QuotaAdjustmentDecision, len(decisions))
			for i, d := range decisions {
				out.LastDecisions[i] = quotaAdjustmentDecisionFromCore(d)
			}
		}
	} else if ver, _, _ := q.sched.CurrentQuotaPolicyView(); ver > 0 {
		out.QuotaVersion = ver
	}
	coreLanes := q.sched.LaneAdaptiveStatsSnapshots(q.reg)
	out.Lanes = make([]LaneAdaptiveStats, len(coreLanes))
	for i, s := range coreLanes {
		out.Lanes[i] = laneAdaptiveStatsFromCore(s)
	}
	return out
}

// AdaptiveQuotaSnapshot returns a read-only view of adaptive controller state.
//
// Deprecated: prefer AdaptiveDebugSnapshot, which includes per-lane stats.
func (q *Queue) AdaptiveQuotaSnapshot() AdaptiveControllerSnapshot {
	dbg := q.AdaptiveDebugSnapshot()
	return AdaptiveControllerSnapshot{
		Enabled:        dbg.Enabled,
		Running:        dbg.Running,
		LastEvaluation: dbg.LastEvaluation,
		TickCount:      dbg.TickCount,
		LastDecisions:  dbg.LastDecisions,
		PolicyVersion:  dbg.PolicyVersion,
		QuotaVersion:   dbg.QuotaVersion,
	}
}
