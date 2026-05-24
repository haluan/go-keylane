// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "time"

// ShardPressureClass classifies shard or global pressure patterns (KL-1503).
type ShardPressureClass string

const (
	ShardPressureHealthy      ShardPressureClass = "healthy"
	ShardPressureLocalizedKey ShardPressureClass = "localized_key"
	ShardPressureLaneDominant ShardPressureClass = "lane_dominant"
	ShardPressureShardHot     ShardPressureClass = "shard_hot"
	ShardPressureDistributed  ShardPressureClass = "distributed"
	ShardPressureWorkerBound  ShardPressureClass = "worker_bound"
	ShardPressureUnknown      ShardPressureClass = "unknown"
)

// ShardPressureConfig controls shard pressure diagnostics (KL-1503).
type ShardPressureConfig struct {
	Enabled bool

	Window time.Duration

	HotShardPressureRatio float64
	DominantLaneRatio     float64
	LocalizedHotKeyRatio  float64
	DistributedShardRatio float64
	WorkerBusyRatio       float64

	MaxHotShards                int
	MaxLaneBreakdownPerShard    int
	MaxHotKeyCandidatesPerShard int
}

func normalizeShardPressureConfig(cfg *ShardPressureConfig) {
	if cfg == nil || !cfg.Enabled {
		return
	}
	if cfg.HotShardPressureRatio <= 0 {
		cfg.HotShardPressureRatio = 0.70
	}
	if cfg.DominantLaneRatio <= 0 {
		cfg.DominantLaneRatio = 0.60
	}
	if cfg.LocalizedHotKeyRatio <= 0 {
		cfg.LocalizedHotKeyRatio = 0.40
	}
	if cfg.DistributedShardRatio <= 0 {
		cfg.DistributedShardRatio = 0.50
	}
	if cfg.WorkerBusyRatio <= 0 {
		cfg.WorkerBusyRatio = 0.80
	}
	if cfg.MaxHotShards <= 0 {
		cfg.MaxHotShards = 16
	}
	if cfg.MaxLaneBreakdownPerShard <= 0 {
		cfg.MaxLaneBreakdownPerShard = 8
	}
	if cfg.MaxHotKeyCandidatesPerShard <= 0 {
		cfg.MaxHotKeyCandidatesPerShard = 4
	}
}

func shardPressureEnabled(cfg ShardPressureConfig) bool {
	return cfg.Enabled
}

// LanePressureSnapshot is lane-level pressure within one shard.
type LanePressureSnapshot struct {
	LaneID uint16
	Name   string

	QueueDepth      int64
	QueueDepthRatio float64

	QueueWaitApproxNanos uint64
	QueueWaitRatio       float64

	InflightJobs int64

	CompletedApprox uint64
	RejectedApprox  uint64
	ThrottledApprox uint64
	ShedApprox      uint64

	PressureRatio     float64
	ContributionRatio float64
}

// HotKeyPressureSnapshot summarizes hot key contribution within a shard.
type HotKeyPressureSnapshot struct {
	KeyHash uint64
	LaneID  uint16

	Status HotKeyStatus

	QueuedApprox    int64
	SubmittedApprox uint64
	RejectedApprox  uint64
	ThrottledApprox uint64
	ShedApprox      uint64

	DepthContributionRatio     float64
	WaitContributionRatio      float64
	AdmissionContributionRatio float64

	ActiveMitigation PerKeyMitigationAction
	MitigationReason PerKeyAdmissionReason

	LastSeen time.Time
}

// ShardPressureSnapshot is diagnostic output for one shard.
type ShardPressureSnapshot struct {
	ShardID int

	DiagnosticsEnabled bool
	Class              ShardPressureClass

	QueueDepth      int64
	QueueCapacity   int64
	QueueDepthRatio float64

	QueueWaitApproxNanos uint64
	QueueWaitRatio       float64

	InflightJobs    int64
	CompletedApprox uint64
	RejectedApprox  uint64
	ThrottledApprox uint64
	ShedApprox      uint64

	PressureRatio     float64
	PeerPressureRatio float64
	SkewRatio         float64

	DominantLane     *LanePressureSnapshot
	LaneBreakdown    []LanePressureSnapshot
	HotKeyCandidates []HotKeyPressureSnapshot

	UpdatedAt time.Time
}

// PressureSummarySnapshot summarizes pressure across all shards.
type PressureSummarySnapshot struct {
	DiagnosticsEnabled bool
	Class              ShardPressureClass

	TotalQueueDepth     int64
	TotalQueueCapacity  int64
	QueueDepthRatio     float64
	TotalQueueWaitNanos uint64

	HotShardCount int
	HotShardRatio float64

	WorkerBusyRatio float64
	InflightJobs    int64

	DistributedPressureRatio float64
	LocalizedPressureRatio   float64
	LaneDominanceRatio       float64

	ScaleRelevant      bool
	MitigationRelevant bool

	HotShards []ShardPressureSnapshot

	UpdatedAt time.Time
}

type shardAdmissionTotals struct {
	submitted uint64
	rejected  uint64
	throttled uint64
	shed      uint64
	completed uint64
}

func laneDepthShare(shardLaneDepth, laneGlobalDepth uint64) float64 {
	if laneGlobalDepth == 0 || shardLaneDepth == 0 {
		return 0
	}
	return float64(shardLaneDepth) / float64(laneGlobalDepth)
}

func shardLaneAdmissionTotals(sh shardDebugView, lanes []laneDebugView) shardAdmissionTotals {
	var out shardAdmissionTotals
	for _, ld := range sh.laneDeps {
		if ld.depth == 0 {
			continue
		}
		laneIdx := int(ld.laneID)
		if laneIdx < 0 || laneIdx >= len(lanes) {
			continue
		}
		ln := lanes[laneIdx]
		share := laneDepthShare(ld.depth, ln.depth)
		if share <= 0 {
			continue
		}
		out.submitted += uint64(float64(ln.submitted) * share)
		reject := ln.admissionRejected + ln.overloadRejected
		out.rejected += uint64(float64(reject) * share)
		out.shed += uint64(float64(ln.overloadShed) * share)
		out.completed += uint64(float64(ln.completed) * share)
	}
	return out
}
