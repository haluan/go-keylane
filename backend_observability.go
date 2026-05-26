// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "time"

// BackendReleaseEvent reports backend lease release metadata.
type BackendReleaseEvent struct {
	Resource BackendResourceName
	Lane     BackendLane
	Stage    StageName

	RequestID   string
	KeyHash     uint64
	RequestLane Lane
	ShardID     int
	Transport   string
	Operation   string
	StageIndex  int
	StageCount  int
	Attempt     int

	DeadlineRemaining time.Duration
	DeadlineExhausted bool

	HeldFor  time.Duration
	InFlight int
	Capacity int
	Queued   int
}

// BackendResourceHooks contains optional backend coordination callbacks.
type BackendResourceHooks struct {
	OnBackendAdmission func(BackendAdmissionDecision)
	OnBackendReleased  func(BackendReleaseEvent)
}

func newBackendReleaseEvent(admission BackendAdmissionDecision, held time.Duration, inflight, capacity, queued int) BackendReleaseEvent {
	return BackendReleaseEvent{
		Resource:          admission.Resource,
		Lane:              admission.Lane,
		Stage:             admission.Stage,
		RequestID:         admission.RequestID,
		KeyHash:           admission.KeyHash,
		RequestLane:       admission.RequestLane,
		ShardID:           admission.ShardID,
		Transport:         admission.Transport,
		Operation:         admission.Operation,
		StageIndex:        admission.StageIndex,
		StageCount:        admission.StageCount,
		Attempt:           admission.Attempt,
		DeadlineRemaining: admission.DeadlineRemaining,
		DeadlineExhausted: admission.DeadlineExhausted,
		HeldFor:           held,
		InFlight:          inflight,
		Capacity:          capacity,
		Queued:            queued,
	}
}

func (q *Queue) backendHooksEnabled() bool {
	return q.requestHooksEnabled()
}

func (q *Queue) emitBackendAdmission(dec BackendAdmissionDecision) {
	if !q.backendHooksEnabled() {
		return
	}
	fn := q.config.Observability.Hooks.Backend.OnBackendAdmission
	if fn == nil {
		return
	}
	callRequestHook(func() { fn(dec) })
}

func (q *Queue) emitBackendReleased(ev BackendReleaseEvent) {
	if !q.backendHooksEnabled() {
		return
	}
	fn := q.config.Observability.Hooks.Backend.OnBackendReleased
	if fn == nil {
		return
	}
	callRequestHook(func() { fn(ev) })
}
