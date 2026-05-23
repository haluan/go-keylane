// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

// RecordAdaptiveQuotaDecision updates per-lane adaptive observability counters.
func (s *Scheduler) RecordAdaptiveQuotaDecision(laneID LaneID, action QuotaAdjustmentAction, reason QuotaAdjustmentReason, nowUnix int64) {
	if int(laneID) < 0 || int(laneID) >= len(s.laneCounters) {
		return
	}
	countHold := action == QuotaAdjustmentHold
	s.laneCounters[laneID].recordAdaptiveDecision(action, reason, nowUnix, countHold)
}

// RecordAdaptiveQuotaLastReason records the latest adaptive evaluation reason without counter bumps.
func (s *Scheduler) RecordAdaptiveQuotaLastReason(laneID LaneID, reason QuotaAdjustmentReason) {
	if int(laneID) < 0 || int(laneID) >= len(s.laneCounters) {
		return
	}
	s.laneCounters[laneID].recordLastAdaptiveReason(reason)
}

// RecordManualQuotaChange increments quota-change counters for operator-driven updates.
func (s *Scheduler) RecordManualQuotaChange(laneID LaneID, nowUnix int64) {
	if int(laneID) < 0 || int(laneID) >= len(s.laneCounters) {
		return
	}
	s.laneCounters[laneID].recordManualQuotaChange(nowUnix)
}

// LaneAdaptiveStatsSnapshots returns copy-out per-lane adaptive stats.
func (s *Scheduler) LaneAdaptiveStatsSnapshots(reg *LaneRegistry) []LaneAdaptiveStatsSnapshot {
	out := make([]LaneAdaptiveStatsSnapshot, reg.Len())
	for i := 0; i < reg.Len(); i++ {
		out[i] = s.laneCounters[i].snapshotAdaptiveStats(reg.Name(LaneID(i)))
	}
	return out
}
