// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"errors"
	"fmt"
	"time"

	"github.com/haluan/go-keylane/internal/core"
)

// ShardPressureClass classifies shard or global pressure patterns (KL-1503).
type ShardPressureClass = core.ShardPressureClass

const (
	ShardPressureHealthy      = core.ShardPressureHealthy
	ShardPressureLocalizedKey = core.ShardPressureLocalizedKey
	ShardPressureLaneDominant = core.ShardPressureLaneDominant
	ShardPressureShardHot     = core.ShardPressureShardHot
	ShardPressureDistributed  = core.ShardPressureDistributed
	ShardPressureWorkerBound  = core.ShardPressureWorkerBound
	ShardPressureUnknown      = core.ShardPressureUnknown
)

// ShardPressureConfig controls shard pressure diagnostics (KL-1503).
// Zero value disables rich diagnostics; coarse Queue.Pressure() remains available.
type ShardPressureConfig = core.ShardPressureConfig

// DefaultShardPressureConfig returns recommended defaults.
func DefaultShardPressureConfig() ShardPressureConfig {
	return ShardPressureConfig{
		Enabled:                     true,
		Window:                      30 * time.Second,
		HotShardPressureRatio:       0.70,
		DominantLaneRatio:           0.60,
		LocalizedHotKeyRatio:        0.40,
		DistributedShardRatio:       0.50,
		WorkerBusyRatio:             0.80,
		MaxHotShards:                16,
		MaxLaneBreakdownPerShard:    8,
		MaxHotKeyCandidatesPerShard: 4,
	}
}

var ErrInvalidShardPressureConfig = errors.New("keylane: invalid shard pressure config")

// NormalizeShardPressureConfig fills zero fields with defaults when enabled.
func NormalizeShardPressureConfig(cfg *ShardPressureConfig) {
	if cfg == nil || !cfg.Enabled {
		return
	}
	def := DefaultShardPressureConfig()
	if cfg.Window <= 0 {
		cfg.Window = def.Window
	}
	if cfg.HotShardPressureRatio <= 0 {
		cfg.HotShardPressureRatio = def.HotShardPressureRatio
	}
	if cfg.DominantLaneRatio <= 0 {
		cfg.DominantLaneRatio = def.DominantLaneRatio
	}
	if cfg.LocalizedHotKeyRatio <= 0 {
		cfg.LocalizedHotKeyRatio = def.LocalizedHotKeyRatio
	}
	if cfg.DistributedShardRatio <= 0 {
		cfg.DistributedShardRatio = def.DistributedShardRatio
	}
	if cfg.WorkerBusyRatio <= 0 {
		cfg.WorkerBusyRatio = def.WorkerBusyRatio
	}
	if cfg.MaxHotShards <= 0 {
		cfg.MaxHotShards = def.MaxHotShards
	}
	if cfg.MaxLaneBreakdownPerShard <= 0 {
		cfg.MaxLaneBreakdownPerShard = def.MaxLaneBreakdownPerShard
	}
	if cfg.MaxHotKeyCandidatesPerShard <= 0 {
		cfg.MaxHotKeyCandidatesPerShard = def.MaxHotKeyCandidatesPerShard
	}
}

// ValidateShardPressureConfig checks normalized config.
func ValidateShardPressureConfig(cfg ShardPressureConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.Window <= 0 {
		return fmt.Errorf("%w: Window must be positive when enabled", ErrInvalidShardPressureConfig)
	}
	if cfg.HotShardPressureRatio <= 0 || cfg.HotShardPressureRatio > 1 {
		return fmt.Errorf("%w: HotShardPressureRatio must be in (0,1]", ErrInvalidShardPressureConfig)
	}
	if cfg.DominantLaneRatio <= 0 || cfg.DominantLaneRatio > 1 {
		return fmt.Errorf("%w: DominantLaneRatio must be in (0,1]", ErrInvalidShardPressureConfig)
	}
	if cfg.LocalizedHotKeyRatio <= 0 || cfg.LocalizedHotKeyRatio > 1 {
		return fmt.Errorf("%w: LocalizedHotKeyRatio must be in (0,1]", ErrInvalidShardPressureConfig)
	}
	if cfg.MaxHotShards < 0 || cfg.MaxLaneBreakdownPerShard < 0 || cfg.MaxHotKeyCandidatesPerShard < 0 {
		return fmt.Errorf("%w: snapshot limits must be non-negative", ErrInvalidShardPressureConfig)
	}
	return nil
}

// LanePressureSnapshot is lane-level pressure within one shard.
type LanePressureSnapshot = core.LanePressureSnapshot

// HotKeyPressureSnapshot summarizes hot key contribution within a shard.
type HotKeyPressureSnapshot = core.HotKeyPressureSnapshot

// ShardPressureSnapshot is diagnostic output for one shard.
type ShardPressureSnapshot = core.ShardPressureSnapshot

// PressureSummarySnapshot summarizes pressure across all shards.
type PressureSummarySnapshot = core.PressureSummarySnapshot
