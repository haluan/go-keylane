// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "math"

type shardPressureInput struct {
	ShardID int

	QueueDepth      int64
	QueueCapacity   int64
	QueueDepthRatio float64

	QueueWaitApproxNanos uint64
	QueueWaitRatio       float64

	InflightJobs int64

	AdmissionPressureRatio  float64
	WorkerContributionRatio float64

	PressureRatio     float64
	PeerPressureRatio float64
	SkewRatio         float64

	TopHotKeyContribution float64
	TopLaneContribution   float64
	HasHotKeyCandidate    bool

	Cfg ShardPressureConfig
}

type globalPressureInput struct {
	ShardCount    int
	HotShardCount int
	HotShardRatio float64

	QueueDepthRatio     float64
	QueueWaitRatio      float64
	WorkerBusyRatio     float64
	MaxSkewRatio        float64
	LocalizedShardCount int

	Cfg ShardPressureConfig
}

func maxFloat64(vals ...float64) float64 {
	var m float64
	for i, v := range vals {
		if i == 0 || v > m {
			m = v
		}
	}
	return m
}

// computeShardPressureRatio derives a conservative normalized pressure signal.
func computeShardPressureRatio(depthRatio, waitRatio, admissionRatio, workerRatio float64) float64 {
	return maxFloat64(depthRatio, waitRatio, admissionRatio, workerRatio)
}

func computeQueueWaitRatio(waitNanos uint64, windowNanos uint64, workerCount int) float64 {
	if windowNanos <= 0 || workerCount <= 0 {
		return 0
	}
	// Normalize cumulative wait against window * workers as a soft threshold.
	denom := float64(windowNanos) * float64(workerCount)
	if denom <= 0 {
		return 0
	}
	r := float64(waitNanos) / denom
	if r > 1 {
		return 1
	}
	return r
}

func computeWorkerContributionRatio(inflight int64, workerCount int) float64 {
	if workerCount <= 0 {
		return 0
	}
	r := float64(inflight) / float64(workerCount)
	if r > 1 {
		return 1
	}
	return r
}

func computeAdmissionPressureRatio(rejected, throttled, shed, submitted uint64) float64 {
	if submitted == 0 {
		return 0
	}
	total := rejected + throttled + shed
	r := float64(total) / float64(submitted)
	if r > 1 {
		return 1
	}
	return r
}

func computePeerPressureRatio(shardPressure float64, peerSum float64, peerCount int) (peerAvg, skew float64) {
	if peerCount <= 0 {
		return 0, 0
	}
	peerAvg = peerSum / float64(peerCount)
	if peerAvg <= 0 {
		if shardPressure > 0 {
			return 0, math.MaxFloat64
		}
		return 0, 0
	}
	return peerAvg, shardPressure / peerAvg
}

func isLocalizedHotKeyPressure(in shardPressureInput) bool {
	if !in.Cfg.Enabled {
		return false
	}
	if in.PressureRatio < in.Cfg.HotShardPressureRatio {
		return false
	}
	if !in.HasHotKeyCandidate {
		return false
	}
	return in.TopHotKeyContribution >= in.Cfg.LocalizedHotKeyRatio
}

func isLaneDominantPressure(in shardPressureInput) bool {
	if !in.Cfg.Enabled {
		return false
	}
	if in.PressureRatio < in.Cfg.HotShardPressureRatio {
		return false
	}
	if isLocalizedHotKeyPressure(in) {
		return false
	}
	return in.TopLaneContribution >= in.Cfg.DominantLaneRatio
}

func classifyShardPressure(in shardPressureInput) ShardPressureClass {
	if !in.Cfg.Enabled {
		return ShardPressureUnknown
	}
	if in.PressureRatio < in.Cfg.HotShardPressureRatio {
		return ShardPressureHealthy
	}
	if isLocalizedHotKeyPressure(in) {
		return ShardPressureLocalizedKey
	}
	if isLaneDominantPressure(in) {
		return ShardPressureLaneDominant
	}
	return ShardPressureShardHot
}

func isDistributedPressure(in globalPressureInput) bool {
	if !in.Cfg.Enabled {
		return false
	}
	return in.HotShardRatio >= in.Cfg.DistributedShardRatio
}

func classifyGlobalPressure(in globalPressureInput) ShardPressureClass {
	if !in.Cfg.Enabled {
		return ShardPressureUnknown
	}
	if in.ShardCount == 0 {
		return ShardPressureUnknown
	}
	if isDistributedPressure(in) {
		return ShardPressureDistributed
	}
	if in.WorkerBusyRatio >= in.Cfg.WorkerBusyRatio &&
		in.QueueWaitRatio >= in.Cfg.HotShardPressureRatio*0.5 &&
		in.MaxSkewRatio < 2.0 {
		return ShardPressureWorkerBound
	}
	if in.HotShardCount == 0 && in.QueueDepthRatio < in.Cfg.HotShardPressureRatio {
		return ShardPressureHealthy
	}
	if in.LocalizedShardCount > 0 {
		return ShardPressureLocalizedKey
	}
	if in.HotShardCount == 1 {
		return ShardPressureShardHot
	}
	if in.HotShardCount > 0 {
		return ShardPressureDistributed
	}
	return ShardPressureHealthy
}

func computeScaleMitigationFlags(class ShardPressureClass, in globalPressureInput) (scale, mitigation bool) {
	switch class {
	case ShardPressureDistributed, ShardPressureWorkerBound:
		scale = true
	case ShardPressureLocalizedKey:
		mitigation = true
	case ShardPressureShardHot:
		if in.LocalizedShardCount > 0 {
			mitigation = true
		} else {
			scale = in.HotShardRatio >= in.Cfg.DistributedShardRatio*0.5
		}
	case ShardPressureLaneDominant:
		scale = false
		mitigation = false
	case ShardPressureHealthy:
		scale = false
		mitigation = false
	default:
		scale = in.QueueDepthRatio >= in.Cfg.HotShardPressureRatio
		mitigation = in.LocalizedShardCount > 0
	}
	return scale, mitigation
}
