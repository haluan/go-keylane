// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "time"

// PerKeyMitigationAction is the mitigation outcome for a single key hash.
type PerKeyMitigationAction string

const (
	PerKeyMitigationAllow    PerKeyMitigationAction = "allow"
	PerKeyMitigationThrottle PerKeyMitigationAction = "throttle"
	PerKeyMitigationReject   PerKeyMitigationAction = "reject"
	PerKeyMitigationShed     PerKeyMitigationAction = "shed"
)

// PerKeyAdmissionReason explains why a per-key decision was made.
type PerKeyAdmissionReason string

const (
	PerKeyAdmissionReasonNone              PerKeyAdmissionReason = "none"
	PerKeyAdmissionReasonHotKeyCandidate   PerKeyAdmissionReason = "hot_key_candidate"
	PerKeyAdmissionReasonDominantHotKey    PerKeyAdmissionReason = "dominant_hot_key"
	PerKeyAdmissionReasonMaxQueuedPerKey   PerKeyAdmissionReason = "max_queued_per_key"
	PerKeyAdmissionReasonMaxInflightPerKey PerKeyAdmissionReason = "max_inflight_per_key"
	PerKeyAdmissionReasonCooldownActive    PerKeyAdmissionReason = "cooldown_active"
	PerKeyAdmissionReasonShardOverloaded   PerKeyAdmissionReason = "shard_overloaded"
)

// PerKeyAdmissionConfig controls targeted hot key mitigation (KL-1502).
type PerKeyAdmissionConfig struct {
	Enabled bool

	MinStatus HotKeyStatus

	DefaultAction PerKeyMitigationAction

	MaxQueuedPerKey   int
	MaxInflightPerKey int

	PressureRatioThreshold float64
	RejectRatioThreshold   float64

	Cooldown       time.Duration
	RecoveryWindow time.Duration

	// MaxSnapshotsPerShard caps per-key mitigation entries per shard in DebugSnapshot (default 5).
	MaxSnapshotsPerShard int

	// MaxSnapshotsTotal caps total per-key mitigation entries across all shards.
	MaxSnapshotsTotal int
}

// PerKeyAdmissionDecision is the outcome of per-key policy evaluation.
type PerKeyAdmissionDecision struct {
	Action PerKeyMitigationAction
	Reason PerKeyAdmissionReason

	ShardID int
	LaneID  uint16
	KeyHash uint64

	HotKeyStatus  HotKeyStatus
	PressureRatio float64

	RetryAfter        time.Duration
	CooldownRemaining time.Duration
}

// PerKeyAdmissionSnapshot is a copy-out view of active per-key mitigation state.
type PerKeyAdmissionSnapshot struct {
	ShardID int
	LaneID  uint16
	KeyHash uint64

	Action PerKeyMitigationAction
	Reason PerKeyAdmissionReason

	QueuedApprox   int64
	InflightApprox int64
	PressureRatio  float64

	CooldownRemaining time.Duration
	LastDecisionAt    time.Time

	RejectedApprox uint64
}

func perKeyAdmissionEnabled(cfg PerKeyAdmissionConfig) bool {
	return cfg.Enabled
}

func statusMeetsMin(status, min HotKeyStatus) bool {
	return hotKeyStatusRank(status) >= hotKeyStatusRank(min)
}

func hotKeyStatusRank(s HotKeyStatus) int {
	switch s {
	case HotKeyStatusDominant:
		return 2
	case HotKeyStatusCandidate:
		return 1
	default:
		return 0
	}
}

// entryHotKeyStatus computes hot key status and pressure ratio for one tracker entry.
func entryHotKeyStatus(e *hotKeyEntry, hkCfg HotKeyConfig, shardDepth, shardWaitNanos uint64, totalSubmitted uint64, totalQueued int64) (HotKeyStatus, float64) {
	if e == nil || e.keyHash == 0 {
		return HotKeyStatusNone, 0
	}
	depthDenom := float64(shardDepth)
	if depthDenom <= 0 && totalQueued > 0 {
		depthDenom = float64(totalQueued)
	}
	waitDenom := float64(shardWaitNanos)
	submitDenom := float64(totalSubmitted)
	if submitDenom <= 0 {
		submitDenom = 1
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
	depthHit := depthRatio >= hkCfg.HotKeyDepthRatio
	submitHit := submitRatio >= hkCfg.HotKeyDepthRatio
	waitHit := waitRatio >= hkCfg.HotKeyWaitRatio
	if !depthHit && !submitHit && !waitHit {
		return HotKeyStatusNone, 0
	}
	useRatio := depthRatio
	if submitRatio > useRatio {
		useRatio = submitRatio
	}
	if waitRatio > useRatio {
		useRatio = waitRatio
	}
	if useRatio >= 0.7 {
		return HotKeyStatusDominant, useRatio
	}
	return HotKeyStatusCandidate, useRatio
}

func (t *hotKeyTracker) evaluatePerKeyAdmission(
	shardID int,
	keyHash uint64,
	laneID LaneID,
	shardDepth uint64,
	shardWaitNanos uint64,
	shardPressure float64,
	cfg PerKeyAdmissionConfig,
	now time.Time,
) PerKeyAdmissionDecision {
	allow := PerKeyAdmissionDecision{
		Action:       PerKeyMitigationAllow,
		Reason:       PerKeyAdmissionReasonNone,
		ShardID:      shardID,
		LaneID:       uint16(laneID),
		KeyHash:      keyHash,
		HotKeyStatus: HotKeyStatusNone,
	}
	if t == nil || !t.enabled() || !perKeyAdmissionEnabled(cfg) {
		return allow
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.expireStaleEntriesLocked(now)

	idx, ok := t.index[keyHash]
	if !ok {
		return allow
	}
	e := &t.entries[idx]
	if e.keyHash != keyHash {
		return allow
	}

	var totalSubmitted uint64
	var totalQueued int64
	for i := range t.entries {
		en := &t.entries[i]
		if en.keyHash == 0 {
			continue
		}
		totalSubmitted += en.submittedApprox
		if en.queuedApprox > 0 {
			totalQueued += en.queuedApprox
		}
	}

	status, pressureRatio := entryHotKeyStatus(e, t.cfg, shardDepth, shardWaitNanos, totalSubmitted, totalQueued)
	dec := PerKeyAdmissionDecision{
		ShardID:       shardID,
		LaneID:        uint16(laneID),
		KeyHash:       keyHash,
		HotKeyStatus:  status,
		PressureRatio: pressureRatio,
	}

	nowNano := now.UnixNano()
	if e.cooldownUntilUnixNano > nowNano {
		dec.CooldownRemaining = time.Duration(e.cooldownUntilUnixNano - nowNano)
	}

	// Recovery: allow when window elapsed and key no longer hot.
	if e.recoveryUntilUnixNano > 0 && nowNano >= e.recoveryUntilUnixNano && status == HotKeyStatusNone {
		e.lastAction = PerKeyMitigationAllow
		e.lastReason = PerKeyAdmissionReasonNone
		e.cooldownUntilUnixNano = 0
		e.recoveryUntilUnixNano = 0
		return allow
	}

	if cfg.MaxQueuedPerKey > 0 && int(e.queuedApprox) >= cfg.MaxQueuedPerKey {
		return t.finishPerKeyDecision(e, dec, PerKeyMitigationThrottle, PerKeyAdmissionReasonMaxQueuedPerKey, cfg, now)
	}
	if cfg.MaxInflightPerKey > 0 && int(e.inflightApprox) >= cfg.MaxInflightPerKey {
		return t.finishPerKeyDecision(e, dec, PerKeyMitigationThrottle, PerKeyAdmissionReasonMaxInflightPerKey, cfg, now)
	}

	if e.submittedApprox > 0 && cfg.RejectRatioThreshold > 0 {
		rejectRatio := float64(e.rejectedApprox) / float64(e.submittedApprox)
		if rejectRatio >= cfg.RejectRatioThreshold {
			action := cfg.DefaultAction
			if action == PerKeyMitigationAllow || action == PerKeyMitigationThrottle {
				action = PerKeyMitigationReject
			}
			reason := PerKeyAdmissionReasonHotKeyCandidate
			if status == HotKeyStatusDominant {
				reason = PerKeyAdmissionReasonDominantHotKey
			}
			return t.finishPerKeyDecision(e, dec, action, reason, cfg, now)
		}
	}

	if e.cooldownUntilUnixNano > nowNano && e.lastAction != PerKeyMitigationAllow && e.lastAction != "" {
		return t.finishPerKeyDecision(e, dec, e.lastAction, PerKeyAdmissionReasonCooldownActive, cfg, now)
	}

	if shardPressure >= 0.95 {
		if statusMeetsMin(status, cfg.MinStatus) {
			return t.finishPerKeyDecision(e, dec, cfg.DefaultAction, PerKeyAdmissionReasonShardOverloaded, cfg, now)
		}
	}

	if !statusMeetsMin(status, cfg.MinStatus) {
		if cfg.PressureRatioThreshold > 0 && pressureRatio >= cfg.PressureRatioThreshold {
			// below MinStatus but high pressure ratio alone does not trigger
		}
		return allow
	}

	if cfg.PressureRatioThreshold > 0 && pressureRatio < cfg.PressureRatioThreshold {
		return allow
	}

	reason := PerKeyAdmissionReasonHotKeyCandidate
	if status == HotKeyStatusDominant {
		reason = PerKeyAdmissionReasonDominantHotKey
	}
	action := cfg.DefaultAction
	if action == PerKeyMitigationAllow {
		action = PerKeyMitigationThrottle
	}
	return t.finishPerKeyDecision(e, dec, action, reason, cfg, now)
}

func (t *hotKeyTracker) finishPerKeyDecision(
	e *hotKeyEntry,
	dec PerKeyAdmissionDecision,
	action PerKeyMitigationAction,
	reason PerKeyAdmissionReason,
	cfg PerKeyAdmissionConfig,
	now time.Time,
) PerKeyAdmissionDecision {
	dec.Action = action
	dec.Reason = reason
	nowNano := now.UnixNano()
	if action != PerKeyMitigationAllow {
		if cfg.Cooldown > 0 {
			e.cooldownUntilUnixNano = nowNano + cfg.Cooldown.Nanoseconds()
			dec.CooldownRemaining = cfg.Cooldown
		}
		if cfg.RecoveryWindow > 0 {
			e.recoveryUntilUnixNano = nowNano + cfg.RecoveryWindow.Nanoseconds()
		}
		if action == PerKeyMitigationThrottle && cfg.Cooldown > 0 {
			dec.RetryAfter = cfg.Cooldown
		}
	}
	e.lastAction = action
	e.lastReason = reason
	e.lastDecisionUnixNano = nowNano
	e.lastSeenUnixNano = nowNano
	return dec
}

func (t *hotKeyTracker) observeInflightStart(keyHash uint64, now time.Time) {
	if t == nil || !t.enabled() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.entryIfFresh(keyHash, now)
	if e == nil {
		return
	}
	e.inflightApprox++
	e.lastSeenUnixNano = now.UnixNano()
}

func (t *hotKeyTracker) observeInflightEnd(keyHash uint64, now time.Time) {
	if t == nil || !t.enabled() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.entryIfFresh(keyHash, now)
	if e == nil {
		return
	}
	if e.inflightApprox > 0 {
		e.inflightApprox--
	}
	e.lastSeenUnixNano = now.UnixNano()
}

func (t *hotKeyTracker) perKeyAdmissionSnapshots(shardID int, limit int) []PerKeyAdmissionSnapshot {
	if t == nil || !t.enabled() || limit <= 0 {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	out := make([]PerKeyAdmissionSnapshot, 0, limit)
	for i := range t.entries {
		if len(out) >= limit {
			break
		}
		e := &t.entries[i]
		if e.keyHash == 0 || e.lastAction == PerKeyMitigationAllow || e.lastAction == "" {
			continue
		}
		var cooldownRem time.Duration
		if e.cooldownUntilUnixNano > now.UnixNano() {
			cooldownRem = time.Duration(e.cooldownUntilUnixNano - now.UnixNano())
		}
		var lastAt time.Time
		if e.lastDecisionUnixNano > 0 {
			lastAt = time.Unix(0, e.lastDecisionUnixNano)
		}
		out = append(out, PerKeyAdmissionSnapshot{
			ShardID:           shardID,
			LaneID:            uint16(e.laneID),
			KeyHash:           e.keyHash,
			Action:            e.lastAction,
			Reason:            e.lastReason,
			QueuedApprox:      e.queuedApprox,
			InflightApprox:    e.inflightApprox,
			RejectedApprox:    e.rejectedApprox,
			CooldownRemaining: cooldownRem,
			LastDecisionAt:    lastAt,
		})
	}
	return out
}
