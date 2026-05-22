// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "time"

// QueueWaitStatsGCPressure holds cumulative queue-wait timing for accepted jobs from
// admission until job execution starts (excludes user Run duration). Values are
// best-effort under concurrent updates and intended for diagnostics, not strict accounting.
type QueueWaitStatsGCPressure struct {
	// Count is the number of accepted jobs that started execution and contributed a wait sample.
	Count uint64
	// TotalNanos is the sum of queue-wait durations in nanoseconds.
	TotalNanos uint64
	// MaxNanos is the maximum observed queue-wait duration in nanoseconds.
	MaxNanos uint64
}

// AverageNanos returns the average queue-wait duration in nanoseconds, or zero if Count is zero.
func (s QueueWaitStatsGCPressure) AverageNanos() uint64 {
	if s.Count == 0 {
		return 0
	}
	return s.TotalNanos / s.Count
}

// AverageDuration returns the average queue-wait duration.
func (s QueueWaitStatsGCPressure) AverageDuration() time.Duration {
	return time.Duration(s.AverageNanos())
}

// MaxDuration returns the maximum observed queue-wait duration.
func (s QueueWaitStatsGCPressure) MaxDuration() time.Duration {
	return time.Duration(s.MaxNanos)
}
