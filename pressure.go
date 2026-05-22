// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

// PressuredDepthRatio is the queue depth ratio at which the scheduler is considered pressured.
const PressuredDepthRatio = 0.70

// OverloadedDepthRatio is the queue depth ratio at which the scheduler is considered overloaded.
const OverloadedDepthRatio = 0.90

// Pressure is a coarse queue-depth signal for admission control and degradation decisions.
// It reflects queued depth relative to capacity, not CPU, GC, or latency SLOs.
type Pressure struct {
	// TotalDepth is the sum of queued jobs across all shards and lanes.
	TotalDepth uint64
	// TotalCapacity is the sum of lane queue capacities across all shards.
	TotalCapacity uint64
	// TotalInFlight is the number of jobs currently executing.
	TotalInFlight uint64
	// TotalDepthRatio is TotalDepth / TotalCapacity, or zero when TotalCapacity is zero.
	TotalDepthRatio float64

	// IsHealthy is true when queue depth is comfortably below capacity.
	IsHealthy bool
	// IsPressured is true when depth ratio is at or above PressuredDepthRatio but below OverloadedDepthRatio.
	IsPressured bool
	// IsOverloaded is true when depth ratio is at or above OverloadedDepthRatio.
	IsOverloaded bool
}
