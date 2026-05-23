// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "time"

// resolveAdaptiveLanePolicies merges explicit lane policies with class-based defaults.
// Unlisted lanes or lanes with empty Class use the admission policy lane class.
func resolveAdaptiveLanePolicies(reg *LaneRegistry, sched *Scheduler, explicit []LaneAdaptivePolicy, initialQuotas map[string]int) []resolvedLaneAdaptivePolicy {
	byName := make(map[string]LaneAdaptivePolicy, len(explicit))
	for _, lp := range explicit {
		byName[lp.Lane] = lp
	}
	adm := sched.loadAdmissionPolicy()

	out := make([]resolvedLaneAdaptivePolicy, reg.Len())
	for i := 0; i < reg.Len(); i++ {
		id := LaneID(i)
		name := reg.Name(id)
		initial := initialQuotas[name]
		if initial < 1 {
			initial = reg.Quota(id)
		}

		lp, ok := byName[name]
		class := adm.defaultClass
		if i < len(adm.lanes) && adm.lanes[i].class != "" {
			class = adm.lanes[i].class
		}
		if ok && lp.Class != "" {
			class = lp.Class
		}

		r := defaultResolvedPolicy(name, class, initial)
		if ok {
			if lp.Class != "" {
				r.Class = lp.Class
				r = defaultResolvedPolicy(name, r.Class, initial)
			}
			r.Enabled = lp.Enabled
			if !r.Enabled {
				r.AllowIncrease = false
				r.AllowDecrease = false
			}
			if lp.MinQuota >= 1 {
				r.MinQuota = lp.MinQuota
			}
			if lp.MaxQuota >= r.MinQuota {
				r.MaxQuota = lp.MaxQuota
			}
			if lp.TargetQueueWait > 0 {
				r.TargetQueueWait = lp.TargetQueueWait
			}
			if lp.TargetRunTime > 0 {
				r.TargetRunTime = lp.TargetRunTime
			}
			if !lp.AllowIncrease && !lp.AllowDecrease {
				r.AllowIncrease = false
				r.AllowDecrease = false
			} else {
				r.AllowIncrease = lp.AllowIncrease
				r.AllowDecrease = lp.AllowDecrease
			}
		}
		if r.MaxQuota < r.MinQuota {
			r.MaxQuota = r.MinQuota
		}
		if r.MaxQuota > int(MaxLaneQuota) {
			r.MaxQuota = int(MaxLaneQuota)
		}
		r.LaneID = id
		out[i] = r
	}
	return out
}

func defaultResolvedPolicy(lane, class string, initialQuota int) resolvedLaneAdaptivePolicy {
	r := resolvedLaneAdaptivePolicy{
		Lane:     lane,
		Class:    class,
		Enabled:  true,
		MinQuota: 1,
		MaxQuota: initialQuota,
	}
	if r.MaxQuota < 1 {
		r.MaxQuota = 1
	}
	if r.MaxQuota > int(MaxLaneQuota) {
		r.MaxQuota = int(MaxLaneQuota)
	}

	switch class {
	case LaneClassCritical:
		r.AllowIncrease = true
		r.AllowDecrease = false
	case LaneClassNormal:
		r.AllowIncrease = true
		r.AllowDecrease = true
	case LaneClassBackground, LaneClassBestEffort:
		r.AllowIncrease = false
		r.AllowDecrease = true
	default:
		r.AllowIncrease = true
		r.AllowDecrease = true
	}

	switch class {
	case LaneClassCritical:
		r.TargetQueueWait = 20 * time.Millisecond
	case LaneClassBackground:
		r.TargetQueueWait = 100 * time.Millisecond
	default:
		r.TargetQueueWait = 50 * time.Millisecond
	}
	return r
}
