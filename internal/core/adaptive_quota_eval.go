// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"sort"
	"time"
)

const minQueueWaitSamples = 1

type laneCandidate struct {
	laneIdx  int
	action   QuotaAdjustmentAction
	reason   QuotaAdjustmentReason
	priority int
}

// EvaluateAdaptiveQuota returns quota adjustment decisions for one evaluation tick.
func EvaluateAdaptiveQuota(
	cfg AdaptiveQuotaConfig,
	policies []resolvedLaneAdaptivePolicy,
	snap AdaptiveSignalSnapshot,
	state *AdaptiveControllerState,
	now time.Time,
) []QuotaAdjustmentDecision {
	n := len(snap.Lanes)
	if !cfg.Enabled || state == nil || n == 0 {
		return nil
	}

	decisions := make([]QuotaAdjustmentDecision, n)
	warmupActive := cfg.WarmupDuration > 0 && now.Sub(state.StartedAt) < cfg.WarmupDuration
	pressure := snap.GlobalPressure

	var increaseCandidates, decreaseCandidates []laneCandidate

	for i, sig := range snap.Lanes {
		pol := policies[i]
		oldQ := sig.CurrentQuota
		decisions[i] = QuotaAdjustmentDecision{
			Lane:           pol.Lane,
			Class:          pol.Class,
			Action:         QuotaAdjustmentHold,
			Reason:         QuotaReasonNone,
			OldQuota:       oldQ,
			NewQuota:       oldQ,
			GlobalPressure: pressure,
			QueueDepth:     sig.QueueDepth,
			QueueWaitP95:   sig.QueueWaitMax,
			RunP95:         sig.RunMax,
			PolicyVersion:  snap.PolicyVersion,
			QuotaVersion:   snap.QuotaVersion,
		}

		if !pol.Enabled {
			decisions[i].Reason = QuotaReasonIncreaseDisabled
			continue
		}
		if warmupActive {
			decisions[i].Reason = QuotaReasonWarmupActive
			continue
		}
		if last, ok := state.LastAdjusted[pol.LaneID]; ok && now.Sub(last) < cfg.CooldownDuration {
			decisions[i].Reason = QuotaReasonCooldownActive
			continue
		}
		localizedOverload := localizedOverloadDecreaseEligible(pol.Class, sig)
		if sig.QueueWaitSamples < minQueueWaitSamples && pressure < cfg.PressureHigh && !localizedOverload {
			decisions[i].Reason = QuotaReasonInsufficientSamples
			continue
		}

		shouldDecrease := pressure >= cfg.PressureHigh || localizedOverload
		if cfg.EnableDecrease && pol.AllowDecrease && shouldDecrease {
			if oldQ <= pol.MinQuota {
				decisions[i].Reason = QuotaReasonAtMinBound
				continue
			}
			if reason := decreaseReasonForClass(pol.Class, sig, pressure, cfg); reason != "" {
				decreaseCandidates = append(decreaseCandidates, laneCandidate{i, QuotaAdjustmentDecrease, reason, decreasePriority(pol.Class)})
				continue
			}
		}

		if cfg.EnableIncrease && pol.AllowIncrease && pressure <= cfg.PressureLow {
			if sig.QueueFullCount > 0 {
				decisions[i].Reason = QuotaReasonQueueFull
				continue
			}
			if oldQ >= pol.MaxQuota {
				decisions[i].Reason = QuotaReasonAtMaxBound
				continue
			}
			if sig.RunMax > cfg.RunTimeHigh && cfg.RunTimeHigh > 0 {
				decisions[i].Reason = QuotaReasonRunTimeTooHigh
				continue
			}
			if !queueWaitHigh(sig, pol, cfg) {
				decisions[i].Reason = QuotaReasonNeutralPressure
				continue
			}
			if reason := increaseReasonForClass(pol.Class); reason != "" {
				increaseCandidates = append(increaseCandidates, laneCandidate{i, QuotaAdjustmentIncrease, reason, increasePriority(pol.Class)})
				continue
			}
		}

		if pressure > cfg.PressureLow && pressure < cfg.PressureHigh {
			decisions[i].Reason = QuotaReasonNeutralPressure
		} else if pressure >= cfg.PressureHigh {
			if !pol.AllowDecrease {
				decisions[i].Reason = QuotaReasonDecreaseDisabled
			} else {
				decisions[i].Reason = QuotaReasonGlobalPressureHigh
			}
		} else if pressure <= cfg.PressureLow && !pol.AllowIncrease {
			decisions[i].Reason = QuotaReasonIncreaseDisabled
		}
	}

	applied := 0
	maxAdj := cfg.MaxAdjustmentsPerTick
	if maxAdj < 1 {
		maxAdj = 1
	}

	sort.Slice(decreaseCandidates, func(i, j int) bool {
		return decreaseCandidates[i].priority < decreaseCandidates[j].priority
	})
	for _, c := range decreaseCandidates {
		if applied >= maxAdj {
			break
		}
		decisions[c.laneIdx] = applyCandidate(cfg, policies, snap, c, pressure)
		applied++
	}

	if applied < maxAdj {
		sort.Slice(increaseCandidates, func(i, j int) bool {
			return increaseCandidates[i].priority < increaseCandidates[j].priority
		})
		for _, c := range increaseCandidates {
			if applied >= maxAdj {
				break
			}
			decisions[c.laneIdx] = applyCandidate(cfg, policies, snap, c, pressure)
			applied++
		}
	}

	return decisions
}

func applyCandidate(cfg AdaptiveQuotaConfig, policies []resolvedLaneAdaptivePolicy, snap AdaptiveSignalSnapshot, c laneCandidate, pressure float64) QuotaAdjustmentDecision {
	pol := policies[c.laneIdx]
	sig := snap.Lanes[c.laneIdx]
	oldQ := sig.CurrentQuota
	newQ := oldQ
	switch c.action {
	case QuotaAdjustmentIncrease:
		newQ = oldQ + cfg.IncreaseStep
		if newQ > pol.MaxQuota {
			newQ = pol.MaxQuota
		}
	case QuotaAdjustmentDecrease:
		newQ = oldQ - cfg.DecreaseStep
		if newQ < pol.MinQuota {
			newQ = pol.MinQuota
		}
	}
	if newQ < 1 {
		newQ = 1
	}
	if newQ > int(MaxLaneQuota) {
		newQ = int(MaxLaneQuota)
	}
	return QuotaAdjustmentDecision{
		Lane:           pol.Lane,
		Class:          pol.Class,
		Action:         c.action,
		Reason:         c.reason,
		OldQuota:       oldQ,
		NewQuota:       newQ,
		GlobalPressure: pressure,
		QueueDepth:     sig.QueueDepth,
		QueueWaitP95:   sig.QueueWaitMax,
		RunP95:         sig.RunMax,
		PolicyVersion:  snap.PolicyVersion,
		QuotaVersion:   snap.QuotaVersion,
	}
}

func queueWaitHigh(sig LaneAdaptiveSignal, pol resolvedLaneAdaptivePolicy, cfg AdaptiveQuotaConfig) bool {
	wait := sig.QueueWaitMax
	if wait == 0 {
		wait = sig.QueueWaitAvg
	}
	if pol.TargetQueueWait > 0 && wait >= pol.TargetQueueWait {
		return true
	}
	return cfg.QueueWaitHigh > 0 && wait >= cfg.QueueWaitHigh
}

func increaseReasonForClass(class string) QuotaAdjustmentReason {
	switch class {
	case LaneClassCritical:
		return QuotaReasonCriticalQueueWaitHigh
	case LaneClassNormal:
		return QuotaReasonNormalQueueWaitHigh
	case LaneClassBackground:
		return QuotaReasonBackgroundQueueWaitHigh
	default:
		return ""
	}
}

func laneOverloadElevated(sig LaneAdaptiveSignal) bool {
	return sig.OverloadShedCount > 0 ||
		sig.OverloadRejectCount > 0 ||
		sig.OverloadDegradeCount > 0
}

func localizedOverloadDecreaseEligible(class string, sig LaneAdaptiveSignal) bool {
	if !laneOverloadElevated(sig) {
		return false
	}
	return class == LaneClassBackground || class == LaneClassBestEffort
}

func decreaseReasonForClass(class string, sig LaneAdaptiveSignal, pressure float64, cfg AdaptiveQuotaConfig) QuotaAdjustmentReason {
	switch class {
	case LaneClassBestEffort:
		if pressure >= cfg.PressureHigh || laneOverloadElevated(sig) {
			return QuotaReasonBestEffortPressureHigh
		}
		return ""
	case LaneClassBackground:
		if pressure >= cfg.PressureHigh || laneOverloadElevated(sig) {
			return QuotaReasonBackgroundPressureHigh
		}
		return ""
	case LaneClassNormal:
		if pressure >= cfg.PressureHigh {
			return QuotaReasonGlobalPressureHigh
		}
		if sig.OverloadShedCount > 0 || sig.OverloadRejectCount > 0 {
			return QuotaReasonGlobalPressureHigh
		}
		return ""
	default:
		return ""
	}
}

func increasePriority(class string) int {
	switch class {
	case LaneClassCritical:
		return 0
	case LaneClassNormal:
		return 1
	case LaneClassBackground:
		return 2
	default:
		return 99
	}
}

func decreasePriority(class string) int {
	switch class {
	case LaneClassBestEffort:
		return 0
	case LaneClassBackground:
		return 1
	case LaneClassNormal:
		return 2
	default:
		return 99
	}
}
