// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "time"

// testShardPressureConfig returns enabled config with explicit defaults for tests.
func testShardPressureConfig() ShardPressureConfig {
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
