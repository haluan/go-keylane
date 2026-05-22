// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

// PressuredDepthRatio is the queue depth ratio at which the scheduler is considered pressured.
const PressuredDepthRatio = 0.70

// OverloadedDepthRatio is the queue depth ratio at which the scheduler is considered overloaded.
const OverloadedDepthRatio = 0.90

// Pressure is a coarse queue-depth signal for admission control and degradation decisions.
type Pressure struct {
	TotalDepth      uint64
	TotalCapacity   uint64
	TotalInFlight   uint64
	TotalDepthRatio float64
	IsHealthy       bool
	IsPressured     bool
	IsOverloaded    bool
}

func safeRatio(n, d uint64) float64 {
	if d == 0 {
		return 0
	}
	return float64(n) / float64(d)
}

func classifyPressure(depth, capacity, inFlight uint64) Pressure {
	ratio := safeRatio(depth, capacity)
	p := Pressure{
		TotalDepth:      depth,
		TotalCapacity:   capacity,
		TotalInFlight:   inFlight,
		TotalDepthRatio: ratio,
	}
	if capacity == 0 {
		return p
	}
	p.IsHealthy = ratio < PressuredDepthRatio
	p.IsPressured = ratio >= PressuredDepthRatio && ratio < OverloadedDepthRatio
	p.IsOverloaded = ratio >= OverloadedDepthRatio
	return p
}

// collectPressureTotals scans shard queues under brief locks and returns aggregate depth and capacity.
func (s *Scheduler) collectPressureTotals() (depth, capacity, inFlight uint64) {
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.Lock()
		var shardDepth, shardCapacity uint64
		for j := range shard.Lanes {
			shardDepth += uint64(shard.Lanes[j].depth())
			shardCapacity += uint64(shard.Lanes[j].capacity())
		}
		shard.mu.Unlock()
		depth += shardDepth
		capacity += shardCapacity
		inFlight += uint64(s.shardInflight[i].Load())
	}
	return depth, capacity, inFlight
}

// Pressure returns a cheap queue-depth pressure signal without allocating a full debug snapshot.
func (s *Scheduler) Pressure() Pressure {
	depth, capacity, inFlight := s.collectPressureTotals()
	return classifyPressure(depth, capacity, inFlight)
}
