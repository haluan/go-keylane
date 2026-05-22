// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "time"

// ObservabilityConfig holds internal configuration for scheduler metrics and hooks.
type ObservabilityConfig struct {
	TrackQueueWait   bool
	SlowJobThreshold time.Duration
	OnSlowJob        func(lane string, shardID int, duration time.Duration)

	// Used only for benchmark testing to compare with and without sync.Pool
	DisablePooling bool
}
