// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "sync/atomic"

type perKeyDecisionCounterGrid struct {
	counts [perKeyDecisionCounterSlots]atomic.Uint64
}

const perKeyDecisionCounterSlots = 24

func perKeyDecisionCounterIndex(action PerKeyMitigationAction, reason PerKeyAdmissionReason) int {
	switch action {
	case PerKeyMitigationThrottle:
		switch reason {
		case PerKeyAdmissionReasonHotKeyCandidate, PerKeyAdmissionReasonDominantHotKey:
			return 0
		case PerKeyAdmissionReasonMaxQueuedPerKey:
			return 1
		case PerKeyAdmissionReasonMaxInflightPerKey:
			return 2
		case PerKeyAdmissionReasonCooldownActive:
			return 3
		case PerKeyAdmissionReasonShardOverloaded:
			return 4
		default:
			return 5
		}
	case PerKeyMitigationReject:
		switch reason {
		case PerKeyAdmissionReasonHotKeyCandidate, PerKeyAdmissionReasonDominantHotKey:
			return 6
		case PerKeyAdmissionReasonMaxQueuedPerKey:
			return 7
		case PerKeyAdmissionReasonMaxInflightPerKey:
			return 8
		case PerKeyAdmissionReasonCooldownActive:
			return 9
		case PerKeyAdmissionReasonShardOverloaded:
			return 10
		default:
			return 11
		}
	case PerKeyMitigationShed:
		switch reason {
		case PerKeyAdmissionReasonHotKeyCandidate, PerKeyAdmissionReasonDominantHotKey:
			return 12
		case PerKeyAdmissionReasonMaxQueuedPerKey:
			return 13
		case PerKeyAdmissionReasonMaxInflightPerKey:
			return 14
		case PerKeyAdmissionReasonCooldownActive:
			return 15
		case PerKeyAdmissionReasonShardOverloaded:
			return 16
		default:
			return 17
		}
	default:
		return -1
	}
}

var perKeyDecisionCounterLabels = [perKeyDecisionCounterSlots]struct {
	action string
	reason string
}{
	0:  {"throttle", "hot_key_candidate"},
	1:  {"throttle", "max_queued_per_key"},
	2:  {"throttle", "max_inflight_per_key"},
	3:  {"throttle", "cooldown_active"},
	4:  {"throttle", "shard_overloaded"},
	5:  {"throttle", "other"},
	6:  {"reject", "hot_key_candidate"},
	7:  {"reject", "max_queued_per_key"},
	8:  {"reject", "max_inflight_per_key"},
	9:  {"reject", "cooldown_active"},
	10: {"reject", "shard_overloaded"},
	11: {"reject", "other"},
	12: {"shed", "hot_key_candidate"},
	13: {"shed", "max_queued_per_key"},
	14: {"shed", "max_inflight_per_key"},
	15: {"shed", "cooldown_active"},
	16: {"shed", "shard_overloaded"},
	17: {"shed", "other"},
}

func (s *Scheduler) recordPerKeyAdmissionDecision(action PerKeyMitigationAction, reason PerKeyAdmissionReason) {
	if action == PerKeyMitigationAllow || action == "" {
		return
	}
	idx := perKeyDecisionCounterIndex(action, reason)
	if idx < 0 || idx >= len(s.perKeyDecisionCounts.counts) {
		return
	}
	s.perKeyDecisionCounts.counts[idx].Add(1)
}

func (s *Scheduler) PerKeyAdmissionDecisionTotals() []PerKeyAdmissionDecisionTotal {
	var out []PerKeyAdmissionDecisionTotal
	for i := range s.perKeyDecisionCounts.counts {
		n := s.perKeyDecisionCounts.counts[i].Load()
		if n == 0 {
			continue
		}
		labels := perKeyDecisionCounterLabels[i]
		out = append(out, PerKeyAdmissionDecisionTotal{
			Action: labels.action,
			Reason: labels.reason,
			Count:  n,
		})
	}
	return out
}

// PerKeyAdmissionDecisionTotal is a cumulative per-key decision counter bucket.
type PerKeyAdmissionDecisionTotal struct {
	Action string
	Reason string
	Count  uint64
}

func (s *Scheduler) HotKeyRejectedTotal() uint64 {
	return s.hotKeyRejectedTotal.Load()
}
