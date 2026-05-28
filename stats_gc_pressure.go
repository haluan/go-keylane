// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

// StatsGCPressureVersion is the schema version of StatsGCPressureSnapshot.
const StatsGCPressureVersion = "5"

// StatsGCPressureSnapshot is a read-only, best-effort snapshot of queue depth,
// in-flight pressure, and cumulative per-lane counters across shards and lanes.
// It is safe to read concurrently with submit and worker activity and intended for
// diagnostics and lightweight observability, not strict accounting.
type StatsGCPressureSnapshot struct {
	Version string

	ShardCount  int
	LaneCount   int
	WorkerCount int

	TotalQueued   uint64
	TotalInFlight uint64

	QueueWait QueueWaitStatsGCPressure
	Run       RunStatsGCPressure

	Shards []ShardStatsGCPressure
	Lanes  []LaneStatsGCPressure
}

// ShardStatsGCPressure reports queued depth, in-flight jobs, capacity, and queue-wait
// timing for one shard.
type ShardStatsGCPressure struct {
	ShardID   uint32
	Queued    uint64
	InFlight  uint64
	Capacity  uint64
	QueueWait QueueWaitStatsGCPressure
	Run       RunStatsGCPressure
	PerLane   []LaneDepthGCPressure
}

// LaneCountersGCPressure holds cumulative per-lane counters since queue start.
// Values are best-effort under concurrent updates and intended for diagnostics, not
// durable or exactly-once accounting.
type LaneCountersGCPressure struct {
	// Submitted counts every enqueue attempt to this lane after the lane ID is known,
	// before admission succeeds or fails. Answers: how much traffic targets this lane?
	Submitted uint64
	// Accepted counts jobs successfully admitted into the lane queue. Answers: how much
	// traffic did the scheduler accept for this lane?
	Accepted uint64
	// Rejected counts enqueue attempts not accepted, including queue-full, scheduler
	// stopped/not-started failures, and pressure admission rejections. Answers: how often
	// does this lane reject work?
	Rejected uint64
	// AdmissionRejected counts rejections by pressure-based admission control before
	// enqueue. Each admission rejection also increments Rejected. Answers: how often is
	// this lane shedding load due to runtime pressure (distinct from QueueFull)?
	AdmissionRejected uint64
	// OverloadRejected counts rejections by overload policy (OverloadActionReject) before
	// enqueue. Each overload rejection also increments Rejected and AdmissionRejected.
	// Answers: how often does overload policy hard-reject this lane?
	OverloadRejected uint64
	// OverloadShed counts intentional pre-enqueue load shedding (OverloadActionShed).
	// Each shed also increments Rejected and AdmissionRejected. Answers: how often is
	// this lane shedding best-effort or background work under pressure?
	OverloadShed uint64
	// OverloadDegrade counts overload degrade decisions (OverloadActionDegrade) before
	// enqueue. Each degrade also increments Rejected and AdmissionRejected. Answers: how
	// often should callers use a cheaper fallback for this lane?
	OverloadDegrade uint64
	// Completed counts accepted jobs that finished with a nil error. Answers: how much
	// work completed normally for this lane?
	Completed uint64
	// Failed counts accepted jobs that returned a non-nil error other than context.Canceled.
	// Answers: are failures concentrated in this lane?
	Failed uint64
	// QueueFull counts rejections because the lane queue reached its bounded capacity.
	// Each queue-full rejection also increments Rejected. Answers: is this lane under
	// capacity pressure?
	QueueFull uint64
	// Canceled counts accepted jobs canceled before or during execution, including jobs
	// that return context.Canceled and jobs skipped when a worker context is canceled.
	// Answers: how often is work for this lane canceled?
	Canceled uint64
	// Panicked counts accepted jobs whose Run function panicked and were recovered by the worker.
	Panicked uint64
}

// LaneStatsGCPressure reports aggregated queued depth, in-flight jobs, capacity, and
// cumulative counters for one lane across all shards.
type LaneStatsGCPressure struct {
	LaneID   uint16
	Name     string
	Queued   uint64
	InFlight uint64
	Capacity uint64
	// Counters holds cumulative per-lane admission and terminal-outcome totals since
	// queue start. See LaneCountersGCPressure for per-field semantics.
	Counters LaneCountersGCPressure
	// QueueWait holds cumulative queue-wait timing for this lane across all shards.
	QueueWait QueueWaitStatsGCPressure
	// Run holds cumulative run-duration timing for this lane across all shards.
	Run RunStatsGCPressure
}

// LaneDepthGCPressure reports queued depth for one lane within a single shard.
type LaneDepthGCPressure struct {
	LaneID uint16
	Queued uint64
}
