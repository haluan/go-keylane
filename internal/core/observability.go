package core

import (
	"sync/atomic"
	"time"
)

// ObservabilityConfig holds internal configuration for scheduler metrics and hooks.
type ObservabilityConfig struct {
	TrackQueueWait   bool
	SlowJobThreshold time.Duration
	OnSlowJob        func(lane string, shardID int, duration time.Duration)

	// Used only for benchmark testing to compare with and without sync.Pool
	DisablePooling bool
}

// laneCounters holds atomic metrics counters for a specific lane.
type laneCounters struct {
	submittedTotal      atomic.Int64
	completedTotal      atomic.Int64
	failedTotal         atomic.Int64
	queueFullTotal      atomic.Int64
	queueWaitTotalNanos atomic.Int64
	queueWaitCount      atomic.Int64
}
