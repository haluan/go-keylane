// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Overload policy controls pre-enqueue decisions: keep, reject, shed, or degrade.
// Policies are published as immutable snapshots; each decision loads the current
// snapshot once. Overload policy only applies to new submissions; it does not
// drop queued work or cancel running work. Shed means intentional pre-enqueue
// load shedding. Degrade means callers or middleware may use a cheaper fallback;
// core Keylane does not create business fallback responses. RetryAfter and
// BackoffHint are guidance only; Keylane does not sleep or retry internally.
// Critical lanes are protected but not unlimited. Queue closed and shutdown
// override overload policy.
package keylane

import (
	"time"

	"github.com/haluan/go-keylane/internal/core"
)

// OverloadAction is the action to take before enqueue.
type OverloadAction string

const (
	OverloadKeep    OverloadAction = OverloadAction(core.OverloadActionKeep)
	OverloadReject  OverloadAction = OverloadAction(core.OverloadActionReject)
	OverloadShed    OverloadAction = OverloadAction(core.OverloadActionShed)
	OverloadDegrade OverloadAction = OverloadAction(core.OverloadActionDegrade)
)

// OverloadReason is a stable reason code for overload decisions.
type OverloadReason string

const (
	OverloadReasonNone               OverloadReason = OverloadReason(core.OverloadReasonNone)
	OverloadReasonGlobalPressureHigh OverloadReason = OverloadReason(core.OverloadReasonGlobalPressureHigh)
	OverloadReasonLanePressureHigh   OverloadReason = OverloadReason(core.OverloadReasonLanePressureHigh)
	OverloadReasonLaneDepthExceeded  OverloadReason = OverloadReason(core.OverloadReasonLaneDepthExceeded)
	OverloadReasonQueueFull          OverloadReason = OverloadReason(core.OverloadReasonQueueFull)
	OverloadReasonQueueClosed        OverloadReason = OverloadReason(core.OverloadReasonQueueClosed)
	OverloadReasonBestEffortShedding OverloadReason = OverloadReason(core.OverloadReasonBestEffortShedding)
	OverloadReasonBackgroundShedding OverloadReason = OverloadReason(core.OverloadReasonBackgroundShedding)
	OverloadReasonDegradePreferred   OverloadReason = OverloadReason(core.OverloadReasonDegradePreferred)
)

// LaneOverloadPolicy describes overload behavior for a single lane.
type LaneOverloadPolicy struct {
	Lane  Lane
	Class LaneClass

	RejectAboveRatio  float64
	ShedAboveRatio    float64
	DegradeAboveRatio float64
	MaxQueueDepth     uint32

	RetryAfter time.Duration
	MinBackoff time.Duration
	MaxBackoff time.Duration
	Jitter     bool
}

// OverloadPolicy describes runtime overload rules for all registered lanes.
// Lanes omitted from Lanes use Default fields.
type OverloadPolicy struct {
	Default LaneOverloadPolicy
	Lanes   []LaneOverloadPolicy
}

// OverloadPolicySnapshot is a read-only view of the active overload policy.
type OverloadPolicySnapshot struct {
	Version uint64
	Default LaneOverloadPolicy
	Lanes   []LaneOverloadPolicy
}

func laneOverloadPolicyToInput(lp LaneOverloadPolicy) core.LaneOverloadPolicyInput {
	return core.LaneOverloadPolicyInput{
		Lane:              string(lp.Lane),
		Class:             string(lp.Class),
		RejectAboveRatio:  lp.RejectAboveRatio,
		ShedAboveRatio:    lp.ShedAboveRatio,
		DegradeAboveRatio: lp.DegradeAboveRatio,
		MaxQueueDepth:     lp.MaxQueueDepth,
		RetryAfter:        lp.RetryAfter,
		MinBackoff:        lp.MinBackoff,
		MaxBackoff:        lp.MaxBackoff,
		Jitter:            lp.Jitter,
	}
}

func laneOverloadPolicyFromInput(lp core.LaneOverloadPolicyInput) LaneOverloadPolicy {
	return LaneOverloadPolicy{
		Lane:              Lane(lp.Lane),
		Class:             LaneClass(lp.Class),
		RejectAboveRatio:  lp.RejectAboveRatio,
		ShedAboveRatio:    lp.ShedAboveRatio,
		DegradeAboveRatio: lp.DegradeAboveRatio,
		MaxQueueDepth:     lp.MaxQueueDepth,
		RetryAfter:        lp.RetryAfter,
		MinBackoff:        lp.MinBackoff,
		MaxBackoff:        lp.MaxBackoff,
		Jitter:            lp.Jitter,
	}
}

func normalizeLaneOverloadPolicy(lp *LaneOverloadPolicy) {
	if lp.Class == "" {
		lp.Class = LaneNormal
	}
	if lp.RejectAboveRatio == 0 {
		lp.RejectAboveRatio = 0.90
	}
	if lp.ShedAboveRatio == 0 {
		lp.ShedAboveRatio = 1.0
	}
	if lp.DegradeAboveRatio == 0 {
		lp.DegradeAboveRatio = 1.0
	}
	if lp.MaxQueueDepth == 0 {
		lp.MaxQueueDepth = 1024
	}
}

// UpdateOverloadPolicy validates and publishes a new overload policy snapshot.
func (q *Queue) UpdateOverloadPolicy(policy OverloadPolicy) (uint64, error) {
	normalizeLaneOverloadPolicy(&policy.Default)
	if err := policy.Default.Class.Validate(); err != nil {
		return 0, err
	}
	if err := validateLaneOverloadBackoff(policy.Default); err != nil {
		return 0, err
	}
	input := core.OverloadPolicyInput{
		Default: laneOverloadPolicyToInput(policy.Default),
		Lanes:   make([]core.LaneOverloadPolicyInput, 0, len(policy.Lanes)),
	}
	for i := range policy.Lanes {
		lp := &policy.Lanes[i]
		normalizeLaneOverloadPolicy(lp)
		if err := lp.Lane.Validate(); err != nil {
			return 0, err
		}
		if err := lp.Class.Validate(); err != nil {
			return 0, err
		}
		if err := validateLaneOverloadBackoff(*lp); err != nil {
			return 0, err
		}
		input.Lanes = append(input.Lanes, laneOverloadPolicyToInput(*lp))
	}
	return q.sched.UpdateOverloadPolicy(input)
}

// CurrentOverloadPolicy returns a copy of the active overload policy snapshot.
func (q *Queue) CurrentOverloadPolicy() OverloadPolicySnapshot {
	version, policy := q.sched.CurrentOverloadPolicyView()
	lanes := make([]LaneOverloadPolicy, len(policy.Lanes))
	for i, lp := range policy.Lanes {
		lanes[i] = laneOverloadPolicyFromInput(lp)
	}
	return OverloadPolicySnapshot{
		Version: version,
		Default: laneOverloadPolicyFromInput(policy.Default),
		Lanes:   lanes,
	}
}
