// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Admission policy controls per-lane rejection before enqueue based on lane class,
// per-lane queue depth, and global queue pressure. Policies are published as immutable
// snapshots; each admission decision loads the current snapshot once.
//
// LaneClass is an admission priority, not a strict scheduling priority. LaneCritical
// delays rejection but does not mean unlimited. LaneBestEffort is shed earlier under
// pressure but does not mean work never runs. Per-lane MaxQueueDepth protects
// scheduler capacity during overload by capping how many jobs may wait per lane.
//
// Admission policy only affects new submissions when admission is enabled on the
// request path; it does not drop queued work or interrupt running jobs.
package keylane

import "github.com/haluan/go-keylane/internal/core"

// AdmissionDecision is the outcome of an admission check.
type AdmissionDecision string

const (
	AdmissionAdmit  AdmissionDecision = "admit"
	AdmissionReject AdmissionDecision = "reject"
)

// Admission rejection reason constants (also used in AdmissionRejectedError.Reason).
const (
	AdmissionReasonPressureAboveThreshold = core.AdmissionReasonPressureAboveThreshold
	AdmissionReasonLaneQueueDepthExceeded = core.AdmissionReasonLaneQueueDepthExceeded
)

// LanePolicy describes admission behavior for a single lane.
type LanePolicy struct {
	Lane  Lane
	Class LaneClass
	// RejectAboveRatio is the global TotalDepthRatio at or above which new work
	// for this lane may be rejected (after per-lane depth is checked).
	RejectAboveRatio float64
	// MaxQueueDepth is the maximum total queued jobs allowed for this lane across
	// all shards. It protects scheduler capacity during overload even when global
	// pressure is still acceptable.
	MaxQueueDepth uint32
}

// AdmissionPolicy describes runtime admission rules for all registered lanes.
// Lanes omitted from Lanes use the default fields.
type AdmissionPolicy struct {
	DefaultClass            LaneClass
	DefaultRejectAboveRatio float64
	DefaultMaxQueueDepth    uint32
	Lanes                   []LanePolicy
}

// AdmissionPolicySnapshot is a read-only view of the active admission policy.
type AdmissionPolicySnapshot struct {
	Version                 uint64
	DefaultClass            LaneClass
	DefaultRejectAboveRatio float64
	DefaultMaxQueueDepth    uint32
	Lanes                   []LanePolicy
}

// AdmissionResult describes the outcome of an admission evaluation.
type AdmissionResult struct {
	Decision AdmissionDecision
	Lane     Lane
	Class    LaneClass
	Reason   string
}

// UpdateAdmissionPolicy validates and publishes a new admission policy snapshot.
// It may be called before Start or while the queue is running.
// It returns ErrStopped if the queue is stopping or stopped.
func (q *Queue) UpdateAdmissionPolicy(policy AdmissionPolicy) (uint64, error) {
	if err := policy.DefaultClass.Validate(); err != nil {
		return 0, err
	}
	input := core.AdmissionPolicyInput{
		DefaultClass:            string(policy.DefaultClass),
		DefaultRejectAboveRatio: policy.DefaultRejectAboveRatio,
		DefaultMaxQueueDepth:    policy.DefaultMaxQueueDepth,
		Lanes:                   make([]core.LanePolicyInput, 0, len(policy.Lanes)),
	}
	for _, lp := range policy.Lanes {
		if err := lp.Lane.Validate(); err != nil {
			return 0, err
		}
		if err := lp.Class.Validate(); err != nil {
			return 0, err
		}
		input.Lanes = append(input.Lanes, core.LanePolicyInput{
			Lane:             string(lp.Lane),
			Class:            string(lp.Class),
			RejectAboveRatio: lp.RejectAboveRatio,
			MaxQueueDepth:    lp.MaxQueueDepth,
		})
	}
	return q.sched.UpdateAdmissionPolicy(input)
}

// CurrentAdmissionPolicy returns a copy of the active admission policy snapshot.
func (q *Queue) CurrentAdmissionPolicy() AdmissionPolicySnapshot {
	version, policy := q.sched.CurrentAdmissionPolicyView()
	lanes := make([]LanePolicy, len(policy.Lanes))
	for i, lp := range policy.Lanes {
		lanes[i] = LanePolicy{
			Lane:             Lane(lp.Lane),
			Class:            LaneClass(lp.Class),
			RejectAboveRatio: lp.RejectAboveRatio,
			MaxQueueDepth:    lp.MaxQueueDepth,
		}
	}
	return AdmissionPolicySnapshot{
		Version:                 version,
		DefaultClass:            LaneClass(policy.DefaultClass),
		DefaultRejectAboveRatio: policy.DefaultRejectAboveRatio,
		DefaultMaxQueueDepth:    policy.DefaultMaxQueueDepth,
		Lanes:                   lanes,
	}
}
