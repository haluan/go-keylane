// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "time"

func buildAdaptiveSignalSnapshot(s *Scheduler, reg *LaneRegistry, policies []resolvedLaneAdaptivePolicy, policyVersion uint64) AdaptiveSignalSnapshot {
	pressure := s.Pressure()
	gc := s.StatsGCPressure()
	quotaVer, _, _ := s.CurrentQuotaPolicyView()

	lanes := make([]LaneAdaptiveSignal, reg.Len())
	for i := 0; i < reg.Len(); i++ {
		pol := policies[i]
		var ls LaneStatsGCPressure
		if i < len(gc.Lanes) {
			ls = gc.Lanes[i]
		}
		qw := ls.QueueWait
		run := ls.Run
		lanes[i] = LaneAdaptiveSignal{
			LaneID:               LaneID(i),
			Lane:                 pol.Lane,
			Class:                pol.Class,
			CurrentQuota:         int(ls.Counters.Submitted), // fallback; overwritten below
			MinQuota:             pol.MinQuota,
			MaxQuota:             pol.MaxQuota,
			QueueDepth:           uint32(ls.Queued),
			InFlight:             uint32(ls.InFlight),
			QueueWaitAvg:         time.Duration(qw.AverageNanos()),
			QueueWaitMax:         time.Duration(qw.MaxNanos),
			QueueWaitSamples:     qw.Count,
			RunAvg:               time.Duration(run.AverageNanos()),
			RunMax:               time.Duration(run.MaxNanos),
			RunSamples:           run.Count,
			OverloadRejectCount:  ls.Counters.OverloadRejected,
			OverloadShedCount:    ls.Counters.OverloadShed,
			OverloadDegradeCount: ls.Counters.OverloadDegrade,
			QueueFullCount:       ls.Counters.QueueFull,
		}
	}

	// Current quota from quota policy snapshot.
	qsnap := s.loadQuotaPolicy()
	for i := 0; i < reg.Len() && i < len(lanes); i++ {
		if i < len(qsnap.laneQuotas) {
			lanes[i].CurrentQuota = qsnap.laneQuotas[i]
		}
	}

	return AdaptiveSignalSnapshot{
		Time:           time.Now(),
		GlobalPressure: pressure.TotalDepthRatio,
		Lanes:          lanes,
		PolicyVersion:  policyVersion,
		QuotaVersion:   quotaVer,
	}
}
