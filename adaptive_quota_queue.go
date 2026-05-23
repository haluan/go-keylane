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
	cfg := adaptiveQuotaConfigToCore(policy.Config)
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
	if q.config.Observability.EnableHooks && q.config.Observability.Hooks.OnAdaptiveQuotaDecision != nil {
		h := q.config.Observability.Hooks.OnAdaptiveQuotaDecision
		hook = func(d core.QuotaAdjustmentDecision, t time.Time) {
			h(adaptiveQuotaEventFromCore(d, t))
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
			return q.UpdateLaneQuotaIfVersion(Lane(lane), quota, expectedVer)
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
		QueueWaitP95:   d.QueueWaitP95,
		RunP95:         d.RunP95,
		PolicyVersion:  d.PolicyVersion,
		QuotaVersion:   d.QuotaVersion,
	}
}

// AdaptiveQuotaSnapshot returns a read-only view of adaptive controller state.
func (q *Queue) AdaptiveQuotaSnapshot() AdaptiveControllerSnapshot {
	if q.adaptive == nil {
		return AdaptiveControllerSnapshot{Enabled: false}
	}
	enabled, running, lastEval, tickCount, decisions, policyVer, quotaVer := q.adaptive.Snapshot()
	out := AdaptiveControllerSnapshot{
		Enabled:        enabled,
		Running:        running,
		LastEvaluation: lastEval,
		TickCount:      tickCount,
		PolicyVersion:  policyVer,
		QuotaVersion:   quotaVer,
	}
	if len(decisions) > 0 {
		out.LastDecisions = make([]QuotaAdjustmentDecision, len(decisions))
		for i, d := range decisions {
			out.LastDecisions[i] = quotaAdjustmentDecisionFromCore(d)
		}
	}
	return out
}
