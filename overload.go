// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"errors"
	"fmt"
	"time"

	"github.com/haluan/go-keylane/internal/core"
)

// OverloadConfig enables overload policy evaluation before enqueue.
// Zero value disables overload checks. When enabled, overload runs before
// admission control; prefer overload-only for new integrations.
type OverloadConfig struct {
	Enabled bool
}

// OverloadDecision is the result of overload policy evaluation.
type OverloadDecision struct {
	Action        OverloadAction
	Reason        OverloadReason
	Lane          Lane
	Class         LaneClass
	Pressure      float64
	LaneDepth     uint32
	MaxDepth      uint32
	RetryAfter    time.Duration
	Backoff       BackoffHint
	PolicyVersion uint64
}

// ErrOverloadRejected indicates the request was rejected by overload policy before enqueue.
var ErrOverloadRejected = errors.New("keylane: overload rejected")

// ErrOverloadShed indicates the request was shed by overload policy before enqueue.
var ErrOverloadShed = errors.New("keylane: overload shed")

// ErrOverloadDegraded indicates the request should use a degraded path instead of normal enqueue.
var ErrOverloadDegraded = errors.New("keylane: overload degraded")

// OverloadError carries structured overload decision details.
type OverloadError struct {
	Decision OverloadDecision
}

func (e OverloadError) Error() string {
	switch e.Decision.Action {
	case OverloadShed:
		return fmt.Sprintf("keylane: overload shed (lane %s reason %s)", e.Decision.Lane, e.Decision.Reason)
	case OverloadDegrade:
		return fmt.Sprintf("keylane: overload degraded (lane %s reason %s)", e.Decision.Lane, e.Decision.Reason)
	default:
		return fmt.Sprintf("keylane: overload rejected (lane %s reason %s)", e.Decision.Lane, e.Decision.Reason)
	}
}

func (e OverloadError) Unwrap() error {
	switch e.Decision.Action {
	case OverloadShed:
		return ErrOverloadShed
	case OverloadDegrade:
		return ErrOverloadDegraded
	default:
		return ErrOverloadRejected
	}
}

// CheckOverload evaluates overload policy for a request before enqueue.
func CheckOverload(q *Queue, cfg OverloadConfig, meta RequestMeta) error {
	if q == nil {
		return ErrNilQueue
	}
	if !cfg.Enabled {
		return nil
	}
	if err := meta.Lane.Validate(); err != nil {
		return err
	}

	laneID, ok := q.reg.Lookup(string(meta.Lane))
	if !ok {
		return ErrInvalidLane
	}

	keyHash := core.HashKey(meta.Key)
	signals := q.sched.OverloadSignalsForLane(laneID, keyHash)
	result := q.sched.EvaluateOverloadForLane(laneID, signals)

	if result.Action == core.OverloadActionKeep {
		return nil
	}

	q.sched.RecordOverloadDecision(laneID, result.Action)

	decision := overloadDecisionFromResult(meta.Lane, result)
	pressure := q.sched.Pressure().TotalDepthRatio
	if q.hooksEnabled() && q.config.Observability.Hooks.OnOverloadPolicyDecision != nil {
		q.config.Observability.Hooks.OnOverloadPolicyDecision(overloadPolicyEventFromCore(meta.Lane, result, pressure))
	}
	return OverloadError{Decision: decision}
}

func overloadDecisionFromResult(lane Lane, r core.OverloadEvalResult) OverloadDecision {
	return OverloadDecision{
		Action:        OverloadAction(r.Action),
		Reason:        OverloadReason(r.Reason),
		Lane:          lane,
		Class:         LaneClass(r.Class),
		Pressure:      r.Pressure,
		LaneDepth:     r.LaneDepth,
		MaxDepth:      r.MaxDepth,
		RetryAfter:    r.RetryAfter,
		Backoff:       backoffHintFromCore(r.RetryAfter, r.MinBackoff, r.MaxBackoff, r.Jitter),
		PolicyVersion: r.PolicyVersion,
	}
}
