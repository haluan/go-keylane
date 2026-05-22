// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "time"

// RunStatsGCPressure holds cumulative run-duration timing for accepted jobs from
// job start until Run returns (excludes queue wait). Values are best-effort under
// concurrent updates and intended for diagnostics, not strict accounting.
type RunStatsGCPressure struct {
	// Count is the number of accepted jobs that finished Run and contributed a run sample.
	Count uint64
	// TotalNanos is the sum of run durations in nanoseconds.
	TotalNanos uint64
	// MaxNanos is the maximum observed run duration in nanoseconds.
	MaxNanos uint64
}

// AverageNanos returns the average run duration in nanoseconds, or zero if Count is zero.
func (s RunStatsGCPressure) AverageNanos() uint64 {
	if s.Count == 0 {
		return 0
	}
	return s.TotalNanos / s.Count
}

// AverageDuration returns the average run duration.
func (s RunStatsGCPressure) AverageDuration() time.Duration {
	return time.Duration(s.AverageNanos())
}

// MaxDuration returns the maximum observed run duration.
func (s RunStatsGCPressure) MaxDuration() time.Duration {
	return time.Duration(s.MaxNanos)
}
