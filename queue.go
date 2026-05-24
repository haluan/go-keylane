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
	config   Config
	sched    *core.Scheduler
	reg      *core.LaneRegistry
	adaptive *core.AdaptiveQuotaController
	start    sync.Once
	started  bool

	hotKeyExposeRawKey     bool
	perKeyAdmissionEnabled bool
	perKeyAdmissionCore    core.PerKeyAdmissionConfig
	failurePolicy          FailurePolicy
	retryPolicy            RetryPolicy
}

// New creates a new Queue instance with the specified configuration.
func New(config Config) (*Queue, error) {
	hotKey := config.HotKey
	NormalizeHotKeyConfig(&hotKey)
	config.HotKey = hotKey
	perKey := config.PerKeyAdmission
	NormalizePerKeyAdmissionConfig(&perKey)
	config.PerKeyAdmission = perKey
	shardPressure := config.ShardPressure
	NormalizeShardPressureConfig(&shardPressure)
	config.ShardPressure = shardPressure
	autoscaling := config.AutoscalingSignal
	NormalizeAutoscalingSignalConfig(&autoscaling)
	config.AutoscalingSignal = autoscaling
	retry := config.Retry
	NormalizeRetryPolicy(&retry)
	config.Retry = retry
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

	obs := ResolveObservabilityConfig(config.Observability)
	config.Observability = obs
	wireSchedulerObservability(sched, obs)

	sched.ConfigureHotKey(toCoreHotKeyConfig(config.HotKey))
	if err := sched.ConfigurePerKeyAdmission(toCorePerKeyAdmissionConfig(config.PerKeyAdmission)); err != nil {
		return nil, err
	}
	sched.ConfigureShardPressure(config.ShardPressure)
	sched.ConfigureAutoscalingSignal(config.AutoscalingSignal)

	q := &Queue{
		config:                 config,
		sched:                  sched,
		reg:                    reg,
		hotKeyExposeRawKey:     config.HotKey.Enabled && config.HotKey.ExposeRawKey,
		perKeyAdmissionEnabled: config.PerKeyAdmission.Enabled,
		perKeyAdmissionCore:    toCorePerKeyAdmissionConfig(config.PerKeyAdmission),
		failurePolicy:          config.FailurePolicy,
		retryPolicy:            config.Retry,
	}
	q.initAdaptiveController()
	return q, nil
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
	if q.adaptive != nil {
		_ = q.adaptive.Start(ctx)
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
// state when EnableStats is true. Queue-wait and run-duration samples require
// EnableQueueWaitTiming and EnableRunTiming respectively. The snapshot is safe to read
// concurrently with submit and worker activity. See LaneCountersGCPressure for semantics.
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
			ShardID:   shard.ShardID,
			Queued:    shard.Queued,
			InFlight:  shard.InFlight,
			Capacity:  shard.Capacity,
			QueueWait: copyQueueWaitStatsGCPressure(shard.QueueWait),
			Run:       copyRunStatsGCPressure(shard.Run),
			PerLane:   perLane,
		}
	}

	lanes := make([]LaneStatsGCPressure, len(cs.Lanes))
	for i, lane := range cs.Lanes {
		lanes[i] = LaneStatsGCPressure{
			LaneID:    lane.LaneID,
			Name:      lane.Name,
			Queued:    lane.Queued,
			InFlight:  lane.InFlight,
			Capacity:  lane.Capacity,
			QueueWait: copyQueueWaitStatsGCPressure(lane.QueueWait),
			Run:       copyRunStatsGCPressure(lane.Run),
			Counters: LaneCountersGCPressure{
				Submitted:         lane.Counters.Submitted,
				Accepted:          lane.Counters.Accepted,
				Rejected:          lane.Counters.Rejected,
				AdmissionRejected: lane.Counters.AdmissionRejected,
				OverloadRejected:  lane.Counters.OverloadRejected,
				OverloadShed:      lane.Counters.OverloadShed,
				OverloadDegrade:   lane.Counters.OverloadDegrade,
				Completed:         lane.Counters.Completed,
				Failed:            lane.Counters.Failed,
				QueueFull:         lane.Counters.QueueFull,
				Canceled:          lane.Counters.Canceled,
				Panicked:          lane.Counters.Panicked,
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
		QueueWait:     copyQueueWaitStatsGCPressure(cs.QueueWait),
		Run:           copyRunStatsGCPressure(cs.Run),
		Shards:        shards,
		Lanes:         lanes,
	}
}

func copyQueueWaitStatsGCPressure(in core.QueueWaitStatsGCPressure) QueueWaitStatsGCPressure {
	return QueueWaitStatsGCPressure{
		Count:      in.Count,
		TotalNanos: in.TotalNanos,
		MaxNanos:   in.MaxNanos,
	}
}

func copyRunStatsGCPressure(in core.RunStatsGCPressure) RunStatsGCPressure {
	return RunStatsGCPressure{
		Count:      in.Count,
		TotalNanos: in.TotalNanos,
		MaxNanos:   in.MaxNanos,
	}
}

func wireSchedulerObservability(sched *core.Scheduler, obs ObservabilityConfig) {
	sched.Obs = core.ObservabilityConfig{
		EnableStats:           obs.EnableStats,
		EnableCounters:        obs.EnableCounters,
		EnableQueueWaitTiming: obs.EnableQueueWaitTiming,
		EnableRunTiming:       obs.EnableRunTiming,
		EnableHooks:           obs.EnableHooks,
		EnableDebugSnapshot:   obs.EnableDebugSnapshot,
		TrackQueueWait:        obs.TrackQueueWait,
		SlowJobThreshold:      obs.SlowJobThreshold,
	}
	if !obs.EnableHooks {
		return
	}
	if obs.Hooks.OnJobTiming != nil {
		h := obs.Hooks.OnJobTiming
		sched.Obs.OnJobTiming = func(shardID int, laneID core.LaneID, laneName string, queueWait, runDuration time.Duration, outcome core.JobOutcome) {
			h(JobTimingEvent{
				ShardID:     shardID,
				LaneID:      uint16(laneID),
				Lane:        Lane(laneName),
				QueueWait:   queueWait,
				RunDuration: runDuration,
				Outcome:     JobOutcome(outcome),
			})
		}
	}
	if obs.Hooks.OnSlowJob != nil {
		h := obs.Hooks.OnSlowJob
		sched.Obs.OnSlowJob = func(shardID int, laneID core.LaneID, laneName string, queueWait, runDuration, threshold time.Duration, outcome core.JobOutcome) {
			h(SlowJobEvent{
				ShardID:     shardID,
				LaneID:      uint16(laneID),
				Lane:        Lane(laneName),
				QueueWait:   queueWait,
				RunDuration: runDuration,
				Threshold:   threshold,
				Outcome:     JobOutcome(outcome),
			})
		}
	}
}

// Pressure returns a cheap queue-depth pressure signal for admission control and
// degradation decisions. It does not allocate a full debug snapshot.
func (q *Queue) Pressure() Pressure {
	return copyPressure(q.sched.Pressure())
}

// DebugSnapshot returns a near-time diagnostic view of queue depth, capacity,
// in-flight jobs, pressure, and hot shard/lane rankings. Safe for concurrent reads
// while workers run; not a globally atomic stop-the-world snapshot.
func (q *Queue) DebugSnapshot() DebugSnapshot {
	snap := copyDebugSnapshot(q.sched.DebugSnapshot())
	q.emitHotKeyCandidatesFromSnapshot(snap)
	return snap
}

// PressureSummary returns global shard pressure diagnostics.
func (q *Queue) PressureSummary() PressureSummarySnapshot {
	summary := q.sched.PressureSummarySnapshot()
	q.emitShardPressureSummary(summary)
	return summary
}

// ScaleSignal returns autoscaling signal diagnostics.
func (q *Queue) ScaleSignal() ScaleSignal {
	sig := q.sched.ScaleSignalSnapshot()
	q.emitScaleSignal(sig)
	return sig
}

// ScaleAdmissionTotals returns cumulative admission counters aggregated across lanes.
func (q *Queue) ScaleAdmissionTotals() ScaleAdmissionTotals {
	rejected, shed, throttled := q.sched.ScaleAdmissionTotals()
	return ScaleAdmissionTotals{
		Rejected:  rejected,
		Shed:      shed,
		Throttled: throttled,
	}
}

// HotKeyRejectedTotal returns cumulative hot-key reject observations since queue start.
func (q *Queue) HotKeyRejectedTotal() uint64 {
	return q.sched.HotKeyRejectedTotal()
}

// PerKeyAdmissionDecisionTotals returns cumulative per-key admission decisions by action and reason.
func (q *Queue) PerKeyAdmissionDecisionTotals() []PerKeyAdmissionDecisionTotal {
	coreTotals := q.sched.PerKeyAdmissionDecisionTotals()
	out := make([]PerKeyAdmissionDecisionTotal, len(coreTotals))
	for i, t := range coreTotals {
		out[i] = PerKeyAdmissionDecisionTotal{Action: t.Action, Reason: t.Reason, Count: t.Count}
	}
	return out
}

// PerKeyAdmissionDecisionTotal is a cumulative per-key decision counter bucket.
type PerKeyAdmissionDecisionTotal = core.PerKeyAdmissionDecisionTotal

// ShardPressure returns diagnostics for one shard.
func (q *Queue) ShardPressure(shardID int) (ShardPressureSnapshot, bool) {
	return q.sched.ShardPressureSnapshot(shardID)
}

// HotShards returns bounded hot shard pressure snapshots.
func (q *Queue) HotShards() []ShardPressureSnapshot {
	return q.sched.HotShardPressureSnapshots()
}

// AppendHotShards appends hot shard pressure snapshots to dst.
func (q *Queue) AppendHotShards(dst []ShardPressureSnapshot) []ShardPressureSnapshot {
	return q.sched.AppendHotShardPressureSnapshots(dst)
}

func copyPressure(in core.Pressure) Pressure {
	return Pressure{
		TotalDepth:      in.TotalDepth,
		TotalCapacity:   in.TotalCapacity,
		TotalInFlight:   in.TotalInFlight,
		TotalDepthRatio: in.TotalDepthRatio,
		IsHealthy:       in.IsHealthy,
		IsPressured:     in.IsPressured,
		IsOverloaded:    in.IsOverloaded,
	}
}

func copyDebugSnapshot(in core.DebugSnapshot) DebugSnapshot {
	hotShards := make([]HotShard, len(in.HotShards))
	for i, hs := range in.HotShards {
		hotShards[i] = HotShard{
			ShardID:    hs.ShardID,
			Depth:      hs.Depth,
			Capacity:   hs.Capacity,
			InFlight:   hs.InFlight,
			DepthRatio: hs.DepthRatio,
		}
	}
	hotLanes := make([]HotLane, len(in.HotLanes))
	for i, hl := range in.HotLanes {
		hotLanes[i] = HotLane{
			LaneID:     hl.LaneID,
			Name:       hl.Name,
			Depth:      hl.Depth,
			Capacity:   hl.Capacity,
			InFlight:   hl.InFlight,
			DepthRatio: hl.DepthRatio,
		}
	}
	shards := make([]ShardSnapshot, len(in.Shards))
	for i, sh := range in.Shards {
		laneDepths := make([]LaneDepthSnapshot, len(sh.LaneDepths))
		for j, ld := range sh.LaneDepths {
			laneDepths[j] = LaneDepthSnapshot{
				LaneID: ld.LaneID,
				Name:   ld.Name,
				Depth:  ld.Depth,
			}
		}
		ss := ShardSnapshot{
			ShardID:    sh.ShardID,
			Depth:      sh.Depth,
			Capacity:   sh.Capacity,
			InFlight:   sh.InFlight,
			DepthRatio: sh.DepthRatio,
			LaneDepths: laneDepths,
		}
		if sh.HotKeyCandidate != nil {
			c := copyHotKeyCandidate(*sh.HotKeyCandidate)
			ss.HotKeyCandidate = &c
		}
		if len(sh.HotKeyCandidates) > 0 {
			ss.HotKeyCandidates = make([]HotKeyCandidate, len(sh.HotKeyCandidates))
			for j, hc := range sh.HotKeyCandidates {
				ss.HotKeyCandidates[j] = copyHotKeyCandidate(hc)
			}
		}
		ss.ShardPressure = sh.ShardPressure
		shards[i] = ss
	}
	lanes := make([]LaneSnapshot, len(in.Lanes))
	for i, ln := range in.Lanes {
		lanes[i] = LaneSnapshot{
			LaneID:              ln.LaneID,
			Name:                ln.Name,
			Depth:               ln.Depth,
			Capacity:            ln.Capacity,
			InFlight:            ln.InFlight,
			DepthRatio:          ln.DepthRatio,
			Submitted:           ln.Submitted,
			Completed:           ln.Completed,
			Failed:              ln.Failed,
			QueueFull:           ln.QueueFull,
			QueueWaitNanosTotal: ln.QueueWaitNanosTotal,
			QueueWaitNanosMax:   ln.QueueWaitNanosMax,
			RunNanosTotal:       ln.RunNanosTotal,
			RunNanosMax:         ln.RunNanosMax,
		}
	}
	perKeySnaps := make([]PerKeyAdmissionSnapshot, len(in.PerKeyAdmissionSnapshots))
	for i, ps := range in.PerKeyAdmissionSnapshots {
		perKeySnaps[i] = copyPerKeyAdmissionSnapshot(ps)
	}
	out := DebugSnapshot{
		Version:                  in.Version,
		GeneratedAt:              in.GeneratedAt,
		AdmissionPolicyVersion:   in.AdmissionPolicyVersion,
		OverloadPolicyVersion:    in.OverloadPolicyVersion,
		ShardCount:               in.ShardCount,
		LaneCount:                in.LaneCount,
		WorkerCount:              in.WorkerCount,
		TotalDepth:               in.TotalDepth,
		TotalCapacity:            in.TotalCapacity,
		TotalInFlight:            in.TotalInFlight,
		Pressure:                 copyPressure(in.Pressure),
		PressureSummary:          in.PressureSummary,
		ScaleSignal:              in.ScaleSignal,
		HotShards:                hotShards,
		HotLanes:                 hotLanes,
		Shards:                   shards,
		Lanes:                    lanes,
		PerKeyAdmissionSnapshots: perKeySnaps,
	}
	enrichV05DebugSnapshot(&out)
	return out
}
