// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "github.com/haluan/go-keylane/internal/core"

func toCoreHotKeyConfig(cfg HotKeyConfig) core.HotKeyConfig {
	return core.HotKeyConfig{
		Enabled:                  cfg.Enabled,
		MaxTrackedKeysPerShard:   cfg.MaxTrackedKeysPerShard,
		DetectionWindow:          cfg.DetectionWindow,
		HotKeyDepthRatio:         cfg.HotKeyDepthRatio,
		HotKeyWaitRatio:          cfg.HotKeyWaitRatio,
		MaxCandidatesPerSnapshot: cfg.MaxCandidatesPerSnapshot,
		ExposeRawKey:             cfg.ExposeRawKey,
	}
}

func copyHotKeyCandidate(c core.HotKeyCandidate) HotKeyCandidate {
	return HotKeyCandidate{
		ShardID:         c.ShardID,
		LaneID:          c.LaneID,
		KeyHash:         c.KeyHash,
		Key:             c.Key,
		SubmittedApprox: c.SubmittedApprox,
		QueuedApprox:    c.QueuedApprox,
		RejectedApprox:  c.RejectedApprox,
		DepthRatio:      c.DepthRatio,
		WaitRatio:       c.WaitRatio,
		Status:          HotKeyStatus(c.Status),
		Reason:          c.Reason,
		LastSeen:        c.LastSeen,
	}
}
