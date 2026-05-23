// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "time"

// HotKeyStatus classifies detection strength.
type HotKeyStatus string

const (
	HotKeyStatusNone      HotKeyStatus = "none"
	HotKeyStatusCandidate HotKeyStatus = "candidate"
	HotKeyStatusDominant  HotKeyStatus = "dominant"
)

// HotKeyCandidate is a copy-out hot key view for snapshots.
type HotKeyCandidate struct {
	ShardID int
	LaneID  uint16

	KeyHash uint64
	Key     string

	SubmittedApprox uint64
	QueuedApprox    int64
	RejectedApprox  uint64

	DepthRatio float64
	WaitRatio  float64

	Status HotKeyStatus
	Reason string

	LastSeen time.Time
}

type hotKeyScored struct {
	candidate HotKeyCandidate
	score     float64
	status    HotKeyStatus
	reason    string
}

func hotKeyCandidateLimit(cfg HotKeyConfig) int {
	limit := cfg.MaxCandidatesPerSnapshot
	if limit <= 0 {
		limit = 5
	}
	if cfg.MaxTrackedKeysPerShard > 0 && limit > cfg.MaxTrackedKeysPerShard {
		limit = cfg.MaxTrackedKeysPerShard
	}
	return limit
}

func detectionReason(depthHit, submitHit, waitHit bool) string {
	switch {
	case waitHit:
		return "wait_ratio"
	case depthHit && submitHit:
		return "depth_and_submit_ratio"
	case depthHit:
		return "depth_ratio"
	case submitHit:
		return "submit_ratio"
	default:
		return "unknown"
	}
}

// detectHotKeyCandidates evaluates bounded tracker entries against shard totals.
func (t *hotKeyTracker) detectHotKeyCandidates(shardID int, shardDepth uint64, shardWaitNanos uint64) (top *HotKeyCandidate, all []HotKeyCandidate) {
	if !t.enabled() {
		return nil, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	t.expireStaleEntriesLocked(now)

	var totalSubmitted uint64
	var totalQueued int64

	for i := range t.entries {
		e := &t.entries[i]
		if e.keyHash == 0 {
			continue
		}
		totalSubmitted += e.submittedApprox
		if e.queuedApprox > 0 {
			totalQueued += e.queuedApprox
		}
	}

	if totalSubmitted == 0 && totalQueued == 0 && shardDepth == 0 {
		return nil, nil
	}

	scored := make([]hotKeyScored, 0, len(t.index))
	depthDenom := float64(shardDepth)
	if depthDenom <= 0 && totalQueued > 0 {
		depthDenom = float64(totalQueued)
	}
	waitDenom := float64(shardWaitNanos)
	submitDenom := float64(totalSubmitted)
	if submitDenom <= 0 {
		submitDenom = 1
	}

	for i := range t.entries {
		e := &t.entries[i]
		if e.keyHash == 0 {
			continue
		}
		var depthRatio, waitRatio, submitRatio float64
		if depthDenom > 0 && e.queuedApprox > 0 {
			depthRatio = float64(e.queuedApprox) / depthDenom
		}
		if waitDenom > 0 && e.queueWaitApproxNanos > 0 {
			waitRatio = float64(e.queueWaitApproxNanos) / waitDenom
		}
		if e.submittedApprox > 0 {
			submitRatio = float64(e.submittedApprox) / submitDenom
		}
		depthHit := depthRatio >= t.cfg.HotKeyDepthRatio
		submitHit := submitRatio >= t.cfg.HotKeyDepthRatio
		waitHit := waitRatio >= t.cfg.HotKeyWaitRatio
		if !depthHit && !submitHit && !waitHit {
			continue
		}
		useRatio := depthRatio
		if submitRatio > useRatio {
			useRatio = submitRatio
		}
		if waitRatio > useRatio {
			useRatio = waitRatio
		}
		c := HotKeyCandidate{
			ShardID:         shardID,
			LaneID:          uint16(e.laneID),
			KeyHash:         e.keyHash,
			SubmittedApprox: e.submittedApprox,
			QueuedApprox:    e.queuedApprox,
			RejectedApprox:  e.rejectedApprox,
			DepthRatio:      depthRatio,
			WaitRatio:       waitRatio,
			LastSeen:        time.Unix(0, e.lastSeenUnixNano),
		}
		if t.rawKeys != nil && t.cfg.ExposeRawKey {
			c.Key = t.rawKeys[i]
		}
		reason := detectionReason(depthHit, submitHit, waitHit)
		scored = append(scored, hotKeyScored{candidate: c, score: useRatio, status: HotKeyStatusCandidate, reason: reason})
	}

	if len(scored) == 0 {
		return nil, nil
	}

	for i := 0; i < len(scored); i++ {
		best := i
		for j := i + 1; j < len(scored); j++ {
			if scored[j].score > scored[best].score {
				best = j
			}
		}
		scored[i], scored[best] = scored[best], scored[i]
	}

	limit := hotKeyCandidateLimit(t.cfg)
	if len(scored) < limit {
		limit = len(scored)
	}
	all = make([]HotKeyCandidate, limit)
	for i := 0; i < limit; i++ {
		all[i] = scored[i].candidate
		all[i].Status = HotKeyStatusCandidate
		all[i].Reason = scored[i].reason
	}

	topCopy := all[0]
	if scored[0].score >= 0.7 || (len(scored) > 1 && scored[0].score >= 2*scored[1].score) {
		topCopy.Status = HotKeyStatusDominant
		topCopy.Reason = "dominant_key_concentration"
	} else {
		topCopy.Status = HotKeyStatusCandidate
	}
	all[0] = topCopy
	top = &topCopy
	return top, all
}
