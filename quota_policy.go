// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Quota policy controls how many jobs per lane a worker drains from a shard in one
// processShard cycle. Policies are published as immutable snapshots; workers load the
// current snapshot once at the start of each drain cycle. A cycle in progress keeps
// the snapshot it loaded; the next cycle observes the newest published policy.
// Queued jobs are not dropped and running jobs are not interrupted by an update.
package keylane

import "github.com/haluan/go-keylane/internal/core"

// MaxLaneQuota is the upper bound for per-lane drain quotas in a QuotaPolicy.
const MaxLaneQuota = core.MaxLaneQuota

// QuotaPolicy describes lane drain quotas for runtime updates.
// Lanes are fixed at queue construction; only quotas for registered lanes may change.
// Lanes omitted from LaneQuotas use DefaultQuota.
type QuotaPolicy struct {
	DefaultQuota uint32
	LaneQuotas   map[Lane]uint32
}

// QuotaPolicySnapshot is a read-only view of the active quota policy.
// LaneQuotas is a defensive copy; mutating it does not affect the scheduler.
type QuotaPolicySnapshot struct {
	Version      uint64
	DefaultQuota uint32
	LaneQuotas   map[Lane]uint32
}

// UpdateQuotaPolicy validates and publishes a new quota policy snapshot.
// It may be called before Start or while the queue is running.
// It returns ErrStopped if the queue is stopping or stopped.
// The returned version increases monotonically on each successful update.
func (q *Queue) UpdateQuotaPolicy(policy QuotaPolicy) (uint64, error) {
	input := core.QuotaPolicyInput{
		DefaultQuota: policy.DefaultQuota,
		LaneQuotas:   make(map[string]uint32, len(policy.LaneQuotas)),
	}
	for lane, quota := range policy.LaneQuotas {
		if err := lane.Validate(); err != nil {
			return 0, err
		}
		input.LaneQuotas[string(lane)] = quota
	}
	return q.sched.UpdateQuotaPolicy(input)
}

// UpdateLaneQuota updates a single lane quota through the safe quota policy path.
func (q *Queue) UpdateLaneQuota(lane Lane, quota uint32) (uint64, error) {
	current := q.CurrentQuotaPolicy()
	return q.UpdateLaneQuotaIfVersion(lane, quota, current.Version)
}

// UpdateLaneQuotaIfVersion updates one lane quota only when the active policy version matches expectedVersion.
func (q *Queue) UpdateLaneQuotaIfVersion(lane Lane, quota uint32, expectedVersion uint64) (uint64, error) {
	if err := lane.Validate(); err != nil {
		return 0, err
	}
	current := q.CurrentQuotaPolicy()
	laneQuotas := make(map[Lane]uint32, len(current.LaneQuotas)+1)
	for l, qv := range current.LaneQuotas {
		laneQuotas[l] = qv
	}
	laneQuotas[lane] = quota
	input := core.QuotaPolicyInput{
		DefaultQuota: current.DefaultQuota,
		LaneQuotas:   make(map[string]uint32, len(laneQuotas)),
	}
	for l, qv := range laneQuotas {
		input.LaneQuotas[string(l)] = qv
	}
	ver, err := q.sched.UpdateQuotaPolicyIfVersion(input, expectedVersion)
	if err != nil {
		return 0, err
	}
	return ver, nil
}

// CurrentQuotaPolicy returns a copy of the active quota policy snapshot.
func (q *Queue) CurrentQuotaPolicy() QuotaPolicySnapshot {
	version, defaultQuota, laneQuotas := q.sched.CurrentQuotaPolicyView()
	out := make(map[Lane]uint32, len(laneQuotas))
	for lane, quota := range laneQuotas {
		out[Lane(lane)] = quota
	}
	return QuotaPolicySnapshot{
		Version:      version,
		DefaultQuota: defaultQuota,
		LaneQuotas:   out,
	}
}
