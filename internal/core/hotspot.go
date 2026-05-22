// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "sort"

const topHotShards = 5
const topHotLanes = 5

func rankHotShards(shards []shardDebugView, limit int) []HotShard {
	if limit <= 0 || len(shards) == 0 {
		return nil
	}
	var candidates []HotShard
	for _, sh := range shards {
		if sh.depth == 0 && sh.inFlight == 0 {
			continue
		}
		candidates = append(candidates, HotShard{
			ShardID:    sh.shardID,
			Depth:      sh.depth,
			Capacity:   sh.capacity,
			InFlight:   sh.inFlight,
			DepthRatio: safeRatio(sh.depth, sh.capacity),
		})
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a.Depth != b.Depth {
			return a.Depth > b.Depth
		}
		if a.InFlight != b.InFlight {
			return a.InFlight > b.InFlight
		}
		return a.ShardID < b.ShardID
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	return candidates
}

func rankHotLanes(lanes []laneDebugView, limit int) []HotLane {
	if limit <= 0 || len(lanes) == 0 {
		return nil
	}
	var candidates []HotLane
	for _, ln := range lanes {
		if ln.depth == 0 && ln.inFlight == 0 {
			continue
		}
		candidates = append(candidates, HotLane{
			LaneID:     uint16(ln.laneID),
			Name:       ln.name,
			Depth:      ln.depth,
			Capacity:   ln.capacity,
			InFlight:   ln.inFlight,
			DepthRatio: safeRatio(ln.depth, ln.capacity),
		})
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a.Depth != b.Depth {
			return a.Depth > b.Depth
		}
		if a.InFlight != b.InFlight {
			return a.InFlight > b.InFlight
		}
		return a.LaneID < b.LaneID
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	return candidates
}
