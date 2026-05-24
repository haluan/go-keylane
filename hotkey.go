// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"fmt"
	"time"

	"github.com/haluan/go-keylane/internal/core"
)

// HotKeyConfig controls bounded per-shard hot key accounting and detection.
type HotKeyConfig struct {
	Enabled bool

	// MaxTrackedKeysPerShard caps tracked key candidates per shard (must not grow with total unique keys).
	// Zero with Enabled means tracking is on but no slots are allocated (no-op).
	MaxTrackedKeysPerShard int

	// DetectionWindow is the decay window for approximate accounting data.
	DetectionWindow time.Duration

	// HotKeyDepthRatio: key queued/submitted contribution vs shard totals to flag a candidate.
	HotKeyDepthRatio float64

	// HotKeyWaitRatio: key queue-wait contribution vs shard wait window to flag a candidate.
	HotKeyWaitRatio float64

	// MaxCandidatesPerSnapshot limits ranked candidates in DebugSnapshot per shard (default 5).
	MaxCandidatesPerSnapshot int

	// ExposeRawKey includes raw key strings in snapshots when stored for tracked entries (sensitive).
	ExposeRawKey bool
}

// DefaultHotKeyConfig returns spec-recommended defaults (tracking enabled).
// Use HotKeyConfig{} (zero value) or HotKeyConfig{Enabled: false} to disable explicitly.
func DefaultHotKeyConfig() HotKeyConfig {
	return HotKeyConfig{
		Enabled:                  true,
		MaxTrackedKeysPerShard:   64,
		DetectionWindow:          30 * time.Second,
		HotKeyDepthRatio:         0.40,
		HotKeyWaitRatio:          0.40,
		MaxCandidatesPerSnapshot: 5,
		ExposeRawKey:             false,
	}
}

// ValidateHotKeyConfig validates hot key settings after optional normalization.
func ValidateHotKeyConfig(cfg HotKeyConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.MaxTrackedKeysPerShard < 0 {
		return fmt.Errorf("%w: MaxTrackedKeysPerShard must be non-negative", ErrInvalidHotKeyConfig)
	}
	if cfg.MaxTrackedKeysPerShard > 0 {
		if cfg.DetectionWindow <= 0 {
			return fmt.Errorf("%w: DetectionWindow must be positive when tracking is active", ErrInvalidHotKeyConfig)
		}
		if cfg.HotKeyDepthRatio < 0 || cfg.HotKeyDepthRatio > 1 {
			return fmt.Errorf("%w: HotKeyDepthRatio must be between 0 and 1", ErrInvalidHotKeyConfig)
		}
		if cfg.HotKeyWaitRatio < 0 || cfg.HotKeyWaitRatio > 1 {
			return fmt.Errorf("%w: HotKeyWaitRatio must be between 0 and 1", ErrInvalidHotKeyConfig)
		}
	}
	if cfg.MaxCandidatesPerSnapshot < 0 {
		return fmt.Errorf("%w: MaxCandidatesPerSnapshot must be non-negative", ErrInvalidHotKeyConfig)
	}
	return nil
}

// NormalizeHotKeyConfig applies defaults for unset zero-valued fields when enabled.
func NormalizeHotKeyConfig(cfg *HotKeyConfig) {
	if cfg == nil || !cfg.Enabled {
		return
	}
	def := DefaultHotKeyConfig()
	// MaxTrackedKeysPerShard == 0 is intentional no-op; do not bump to def.MaxTrackedKeysPerShard.
	if cfg.DetectionWindow <= 0 {
		cfg.DetectionWindow = def.DetectionWindow
	}
	if cfg.HotKeyDepthRatio <= 0 {
		cfg.HotKeyDepthRatio = def.HotKeyDepthRatio
	}
	if cfg.HotKeyWaitRatio <= 0 {
		cfg.HotKeyWaitRatio = def.HotKeyWaitRatio
	}
	if cfg.MaxCandidatesPerSnapshot <= 0 {
		cfg.MaxCandidatesPerSnapshot = def.MaxCandidatesPerSnapshot
	}
}

// HotKeyStatus classifies hot key detection strength.
type HotKeyStatus = core.HotKeyStatus

const (
	HotKeyStatusNone      = core.HotKeyStatusNone
	HotKeyStatusCandidate = core.HotKeyStatusCandidate
	HotKeyStatusDominant  = core.HotKeyStatusDominant
)

// HotKeyCandidate is a bounded, approximate view of key pressure on a shard.
type HotKeyCandidate struct {
	ShardID int
	LaneID  uint16

	KeyHash uint64
	// Key is set only when HotKeyConfig.ExposeRawKey is true and the key was stored for this slot.
	Key string

	SubmittedApprox uint64
	QueuedApprox    int64
	RejectedApprox  uint64

	DepthRatio float64
	WaitRatio  float64

	Status HotKeyStatus
	Reason string

	LastSeen time.Time
}

// HotKeyDetection is the result of hot key candidate evaluation for one shard.
type HotKeyDetection struct {
	Status    HotKeyStatus
	Candidate HotKeyCandidate
	Reason    string
}
