// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "github.com/haluan/go-keylane/internal/core"

func toCorePerKeyAdmissionConfig(cfg PerKeyAdmissionConfig) core.PerKeyAdmissionConfig {
	return core.PerKeyAdmissionConfig{
		Enabled:                cfg.Enabled,
		MinStatus:              core.HotKeyStatus(cfg.MinStatus),
		DefaultAction:          core.PerKeyMitigationAction(cfg.DefaultAction),
		MaxQueuedPerKey:        cfg.MaxQueuedPerKey,
		MaxInflightPerKey:      cfg.MaxInflightPerKey,
		PressureRatioThreshold: cfg.PressureRatioThreshold,
		RejectRatioThreshold:   cfg.RejectRatioThreshold,
		Cooldown:               cfg.Cooldown,
		RecoveryWindow:         cfg.RecoveryWindow,
		MaxSnapshotsPerShard:   cfg.MaxSnapshotsPerShard,
		MaxSnapshotsTotal:      cfg.MaxSnapshotsTotal,
	}
}

func copyPerKeyAdmissionDecision(d core.PerKeyAdmissionDecision) PerKeyAdmissionDecision {
	return PerKeyAdmissionDecision{
		Action:            PerKeyMitigationAction(d.Action),
		Reason:            PerKeyAdmissionReason(d.Reason),
		ShardID:           d.ShardID,
		LaneID:            d.LaneID,
		KeyHash:           d.KeyHash,
		HotKeyStatus:      HotKeyStatus(d.HotKeyStatus),
		PressureRatio:     d.PressureRatio,
		RetryAfter:        d.RetryAfter,
		CooldownRemaining: d.CooldownRemaining,
	}
}

func copyPerKeyAdmissionSnapshot(s core.PerKeyAdmissionSnapshot) PerKeyAdmissionSnapshot {
	return PerKeyAdmissionSnapshot{
		ShardID:           s.ShardID,
		LaneID:            s.LaneID,
		KeyHash:           s.KeyHash,
		Action:            PerKeyMitigationAction(s.Action),
		Reason:            PerKeyAdmissionReason(s.Reason),
		QueuedApprox:      s.QueuedApprox,
		InflightApprox:    s.InflightApprox,
		PressureRatio:     s.PressureRatio,
		CooldownRemaining: s.CooldownRemaining,
		LastDecisionAt:    s.LastDecisionAt,
		RejectedApprox:    s.RejectedApprox,
	}
}
