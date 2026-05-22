// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"github.com/haluan/go-keylane/internal/core"
	"sync"
	"time"
)

// Queue is the main entry point for the keylane library.
// It manages job routing, queueing, and execution.
type Queue struct {
	config  Config
	sched   *core.Scheduler
	reg     *core.LaneRegistry
	start   sync.Once
	started bool
}

// New creates a new Queue instance with the specified configuration.
func New(config Config) (*Queue, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// Convert map[Lane]int to map[string]int for LaneRegistry
	quotas := make(map[string]int, len(config.LaneQuotas))
	for lane, quota := range config.LaneQuotas {
		quotas[string(lane)] = quota
	}

	reg, err := core.NewLaneRegistry(quotas)
	if err != nil {
		return nil, err
	}

	sched, err := core.NewScheduler(config.ShardCount, config.WorkerCount, config.QueueSizePerLane, reg)
	if err != nil {
		return nil, err
	}

	sched.Obs = core.ObservabilityConfig{
		TrackQueueWait:   config.Observability.TrackQueueWait,
		SlowJobThreshold: config.Observability.SlowJobThreshold,
	}
	if config.Observability.Hooks.OnSlowJob != nil {
		sched.Obs.OnSlowJob = func(lane string, shardID int, duration time.Duration) {
			config.Observability.Hooks.OnSlowJob(SlowJobEvent{
				Lane:     Lane(lane),
				ShardID:  shardID,
				Duration: duration,
			})
		}
	}

	return &Queue{
		config: config,
		sched:  sched,
		reg:    reg,
	}, nil
}

// Start launches the worker goroutines.
// It returns ErrQueueAlreadyStarted if called more than once.
func (q *Queue) Start(ctx context.Context) error {
	if err := q.sched.Start(ctx); err != nil {
		if errors.Is(err, core.ErrQueueAlreadyStarted) {
			return ErrQueueAlreadyStarted
		}
		return err
	}
	return nil
}

// Stats returns a snapshot of the queue's internal metrics.
func (q *Queue) Stats() Stats {
	coreShards, totalDepth := q.sched.Stats()

	shards := make([]ShardStats, len(coreShards))
	for i, cs := range coreShards {
		lanes := make([]LaneStats, len(cs.Lanes))
		for j, cl := range cs.Lanes {
			lanes[j] = LaneStats{
				Lane:                Lane(cl.LaneName),
				Depth:               cl.Depth,
				Capacity:            cl.Capacity,
				Quota:               cl.Quota,
				SubmittedTotal:      cl.SubmittedTotal,
				CompletedTotal:      cl.CompletedTotal,
				FailedTotal:         cl.FailedTotal,
				QueueFullTotal:      cl.QueueFullTotal,
				QueueWaitTotalNanos: cl.QueueWaitTotalNanos,
				QueueWaitCount:      cl.QueueWaitCount,
			}
		}
		shards[i] = ShardStats{
			ShardID:    cs.ShardID,
			Ready:      cs.Ready,
			TotalDepth: cs.TotalDepth,
			Lanes:      lanes,
		}
	}

	return Stats{
		ShardCount:  q.config.ShardCount,
		WorkerCount: q.config.WorkerCount,
		TotalDepth:  totalDepth,
		Shards:      shards,
	}
}

// StatsGCPressure returns a read-only best-effort snapshot of scheduler GC pressure
// state: queue depths, in-flight jobs, worker/shard/lane configuration, capacity, and
// cumulative per-lane counters in Lanes[].Counters. The snapshot is safe to read
// concurrently with submit and worker activity. Counter fields are cumulative since
// queue start and intended for diagnostics and lightweight observability, not strict
// accounting. See LaneCountersGCPressure for per-field semantics.
func (q *Queue) StatsGCPressure() StatsGCPressureSnapshot {
	cs := q.sched.StatsGCPressure()

	shards := make([]ShardStatsGCPressure, len(cs.Shards))
	for i, shard := range cs.Shards {
		perLane := make([]LaneDepthGCPressure, len(shard.PerLane))
		for j, pl := range shard.PerLane {
			perLane[j] = LaneDepthGCPressure{
				LaneID: pl.LaneID,
				Queued: pl.Queued,
			}
		}
		shards[i] = ShardStatsGCPressure{
			ShardID:  shard.ShardID,
			Queued:   shard.Queued,
			InFlight: shard.InFlight,
			Capacity: shard.Capacity,
			PerLane:  perLane,
		}
	}

	lanes := make([]LaneStatsGCPressure, len(cs.Lanes))
	for i, lane := range cs.Lanes {
		lanes[i] = LaneStatsGCPressure{
			LaneID:   lane.LaneID,
			Name:     lane.Name,
			Queued:   lane.Queued,
			InFlight: lane.InFlight,
			Capacity: lane.Capacity,
			Counters: LaneCountersGCPressure{
				Submitted: lane.Counters.Submitted,
				Accepted:  lane.Counters.Accepted,
				Rejected:  lane.Counters.Rejected,
				Completed: lane.Counters.Completed,
				Failed:    lane.Counters.Failed,
				QueueFull: lane.Counters.QueueFull,
				Canceled:  lane.Counters.Canceled,
				Panicked:  lane.Counters.Panicked,
			},
		}
	}

	return StatsGCPressureSnapshot{
		Version:       cs.Version,
		ShardCount:    cs.ShardCount,
		LaneCount:     cs.LaneCount,
		WorkerCount:   cs.WorkerCount,
		TotalQueued:   cs.TotalQueued,
		TotalInFlight: cs.TotalInFlight,
		Shards:        shards,
		Lanes:         lanes,
	}
}
