// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"fmt"
	"time"
)

type Config struct {
	ShardCount       int
	WorkerCount      int
	QueueSizePerLane int
	LaneQuotas       map[Lane]int

	Observability ObservabilityConfig

	// OverloadEnabled applies overload policy evaluation on Job.Submit before enqueue.
	OverloadEnabled bool
}

type ObservabilityConfig struct {
	// EnableStats controls StatsGCPressure snapshot assembly (pull API; may allocate).
	EnableStats bool
	// EnableCounters controls cumulative admission and terminal counters on the hot path.
	EnableCounters bool
	// EnableQueueWaitTiming controls AcceptedAt stamping and StatsGCPressure queue-wait samples.
	EnableQueueWaitTiming bool
	// EnableRunTiming controls StatsGCPressure run-duration samples on the worker path.
	EnableRunTiming bool
	// EnableHooks controls OnJobTiming and OnSlowJob dispatch (Hooks are ignored when false).
	EnableHooks bool
	// EnableDebugSnapshot controls DebugSnapshot (Pressure remains available).
	EnableDebugSnapshot bool
	// LowAllocationMode applies LowAllocationObservabilityConfig at queue construction.
	LowAllocationMode bool

	// TrackQueueWait enables v1 Stats() queue-wait counters (EnqueuedAt); independent of EnableQueueWaitTiming.
	TrackQueueWait   bool
	SlowJobThreshold time.Duration
	Hooks            Hooks
}

// Validate ensures the configuration is valid.
func (c Config) Validate() error {
	if c.ShardCount < 1 {
		return fmt.Errorf("%w: ShardCount must be at least 1", ErrInvalidShardCount)
	}
	if c.WorkerCount < 1 {
		return fmt.Errorf("%w: WorkerCount must be at least 1", ErrInvalidWorkerCount)
	}
	if c.QueueSizePerLane < 1 {
		return fmt.Errorf("%w: QueueSizePerLane must be at least 1", ErrInvalidQueueSize)
	}
	if len(c.LaneQuotas) == 0 {
		return ErrMissingLaneQuotas
	}
	for lane, quota := range c.LaneQuotas {
		if lane == "" {
			return fmt.Errorf("%w: lane name cannot be empty", ErrInvalidLane)
		}
		if quota < 1 {
			return fmt.Errorf("%w: quota for lane %q must be at least 1", ErrInvalidLaneQuota, lane)
		}
	}
	return nil
}
