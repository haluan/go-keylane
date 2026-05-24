// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"sort"
)

func enrichV05DebugSnapshot(snap *DebugSnapshot) {
	snap.HotKeys = flattenHotKeyCandidates(snap.Shards)
	snap.ShardPressure = flattenShardPressure(snap.Shards)
	snap.Mitigations = flattenMitigations(snap.PerKeyAdmissionSnapshots)
}

func flattenHotKeyCandidates(shards []ShardSnapshot) []HotKeyCandidateSnapshot {
	var out []HotKeyCandidateSnapshot
	for _, sh := range shards {
		cands := sh.HotKeyCandidates
		if sh.HotKeyCandidate != nil {
			cands = append([]HotKeyCandidate{*sh.HotKeyCandidate}, cands...)
		}
		for _, c := range cands {
			out = append(out, hotKeyCandidateToSnapshot(c))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ShardID != out[j].ShardID {
			return out[i].ShardID < out[j].ShardID
		}
		return out[i].KeyHash < out[j].KeyHash
	})
	return out
}

func hotKeyCandidateToSnapshot(c HotKeyCandidate) HotKeyCandidateSnapshot {
	var lastNano int64
	if !c.LastSeen.IsZero() {
		lastNano = c.LastSeen.UnixNano()
	}
	return HotKeyCandidateSnapshot{
		ShardID:          c.ShardID,
		LaneID:           c.LaneID,
		KeyHash:          c.KeyHash,
		Key:              c.Key,
		SubmittedApprox:  c.SubmittedApprox,
		QueuedApprox:     c.QueuedApprox,
		RejectedApprox:   c.RejectedApprox,
		DepthRatio:       c.DepthRatio,
		WaitRatio:        c.WaitRatio,
		Status:           c.Status,
		Reason:           c.Reason,
		LastSeenUnixNano: lastNano,
	}
}

func flattenShardPressure(shards []ShardSnapshot) []ShardPressureSnapshot {
	out := make([]ShardPressureSnapshot, 0, len(shards))
	for _, sh := range shards {
		if sh.ShardPressure.DiagnosticsEnabled {
			out = append(out, sh.ShardPressure)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ShardID < out[j].ShardID
	})
	return out
}

func flattenMitigations(perKey []PerKeyAdmissionSnapshot) []PerKeyMitigationSnapshot {
	out := make([]PerKeyMitigationSnapshot, len(perKey))
	for i, ps := range perKey {
		out[i] = perKeyAdmissionToMitigation(ps)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ShardID != out[j].ShardID {
			return out[i].ShardID < out[j].ShardID
		}
		if out[i].LaneID != out[j].LaneID {
			return out[i].LaneID < out[j].LaneID
		}
		return out[i].KeyHash < out[j].KeyHash
	})
	return out
}

func perKeyAdmissionToMitigation(ps PerKeyAdmissionSnapshot) PerKeyMitigationSnapshot {
	action := string(ps.Action)
	reason := string(ps.Reason)
	allowed := uint64(0)
	if ps.QueuedApprox > 0 || ps.InflightApprox > 0 {
		allowed = 1
	}
	throttled := uint64(0)
	if ps.Action == PerKeyMitigationThrottle {
		throttled = 1
	}
	return PerKeyMitigationSnapshot{
		ShardID:         ps.ShardID,
		LaneID:          ps.LaneID,
		KeyHash:         ps.KeyHash,
		Action:          action,
		Reason:          reason,
		AllowedApprox:   allowed,
		DelayedApprox:   0,
		RejectedApprox:  ps.RejectedApprox,
		ShedApprox:      0,
		ThrottledApprox: throttled,
		QueuedApprox:    ps.QueuedApprox,
		InflightApprox:  ps.InflightApprox,
		PressureRatio:   ps.PressureRatio,
	}
}
