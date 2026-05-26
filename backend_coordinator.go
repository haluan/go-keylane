// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"sync"
	"time"
)

type backendCoordinator struct {
	enabled   bool
	resources map[BackendResourceName]*backendResourceState
}

type backendResourceState struct {
	lanes map[BackendLane]*backendLaneState
}

type backendLaneState struct {
	mu       sync.Mutex
	policy   BackendLanePolicy
	inflight int
	queued   int
}

func newBackendCoordinator(cfg BackendResourceConfig) *backendCoordinator {
	if !cfg.Enabled {
		return &backendCoordinator{enabled: false}
	}
	coord := &backendCoordinator{
		enabled:   true,
		resources: make(map[BackendResourceName]*backendResourceState, len(cfg.Resources)),
	}
	for res, pol := range cfg.Resources {
		rs := &backendResourceState{lanes: make(map[BackendLane]*backendLaneState, len(pol.Lanes))}
		for lane, lp := range pol.Lanes {
			rs.lanes[lane] = &backendLaneState{policy: lp}
		}
		coord.resources[res] = rs
	}
	return coord
}

func (c *backendCoordinator) acquire(
	ctx context.Context,
	q *Queue,
	op BackendOperation,
) (BackendLease, BackendAdmissionDecision, error) {
	if err := ValidateBackendOperation(op); err != nil {
		return nil, BackendAdmissionDecision{}, err
	}
	if err := ctx.Err(); err != nil {
		dec := newBackendAdmissionDecision(ctx, op, backendAdmissionReasonFromContext(err), false, 0, 0, 0)
		q.emitBackendAdmission(dec)
		return nil, dec, backendAdmissionError(dec, err)
	}
	if !c.enabled {
		dec := newBackendAdmissionDecision(ctx, op, BackendAdmissionDisabled, true, 0, 0, 0)
		q.emitBackendAdmission(dec)
		return noopBackendLease{}, dec, nil
	}
	if exec, ok := StageExecutionFromContext(ctx); ok && exec.Deadline.HasDeadline {
		if exec.Deadline.BudgetExhausted || exec.Deadline.Remaining <= 0 {
			cause := DeadlineExhaustedFailure(context.DeadlineExceeded)
			dec := newBackendAdmissionDecision(ctx, op, BackendAdmissionDeadlineExhausted, false, 0, 0, 0)
			q.emitBackendAdmission(dec)
			return nil, dec, backendAdmissionError(dec, cause)
		}
	}
	rs, ok := c.resources[op.Resource]
	if !ok {
		dec := newBackendAdmissionDecision(ctx, op, BackendAdmissionUnknownResource, false, 0, 0, 0)
		q.emitBackendAdmission(dec)
		return nil, dec, backendAdmissionError(dec, nil)
	}
	ls, ok := rs.lanes[op.Lane]
	if !ok {
		dec := newBackendAdmissionDecision(ctx, op, BackendAdmissionUnknownLane, false, 0, 0, 0)
		q.emitBackendAdmission(dec)
		return nil, dec, backendAdmissionError(dec, nil)
	}

	ls.mu.Lock()
	capacity := ls.policy.MaxInFlight
	inflight := ls.inflight
	queued := ls.queued
	if inflight >= capacity {
		ls.mu.Unlock()
		dec := newBackendAdmissionDecision(ctx, op, BackendAdmissionSaturated, false, inflight, capacity, queued)
		q.emitBackendAdmission(dec)
		return nil, dec, backendAdmissionError(dec, nil)
	}
	ls.inflight++
	inflight = ls.inflight
	ls.mu.Unlock()

	dec := newBackendAdmissionDecision(ctx, op, BackendAdmissionAccepted, true, inflight, capacity, queued)
	q.emitBackendAdmission(dec)

	acquiredAt := time.Now()
	lease := &trackedBackendLease{
		release: func() {
			held := time.Since(acquiredAt)
			ls.mu.Lock()
			if ls.inflight > 0 {
				ls.inflight--
			}
			inflightAfter := ls.inflight
			queuedAfter := ls.queued
			ls.mu.Unlock()
			q.emitBackendReleased(newBackendReleaseEvent(dec, held, inflightAfter, capacity, queuedAfter))
		},
	}
	return lease, dec, nil
}
