// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/haluan/go-keylane/internal/core"
)

// BackendAdmissionDecision reports the outcome of a backend acquisition attempt.
type BackendAdmissionDecision struct {
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

	Accepted bool
	Reason   BackendAdmissionReason

	InFlight int
	Capacity int
	Queued   int
}

// BackendAdmissionReason classifies backend admission outcomes.
type BackendAdmissionReason string

const (
	BackendAdmissionAccepted          BackendAdmissionReason = "accepted"
	BackendAdmissionDisabled          BackendAdmissionReason = "disabled"
	BackendAdmissionUnknownResource   BackendAdmissionReason = "unknown_resource"
	BackendAdmissionUnknownLane       BackendAdmissionReason = "unknown_lane"
	BackendAdmissionSaturated         BackendAdmissionReason = "saturated"
	BackendAdmissionQueueFull         BackendAdmissionReason = "queue_full"
	BackendAdmissionCancelled         BackendAdmissionReason = "cancelled"
	BackendAdmissionDeadlineExhausted BackendAdmissionReason = "deadline_exhausted"
)

// ErrBackendAdmission indicates backend admission was rejected.
var ErrBackendAdmission = errors.New("keylane: backend admission rejected")

// BackendAdmissionError carries structured backend admission metadata.
type BackendAdmissionError struct {
	Decision BackendAdmissionDecision
	err      error
}

func (e BackendAdmissionError) Error() string {
	return fmt.Sprintf("keylane: backend admission rejected (%s %s: %s)", e.Decision.Resource, e.Decision.Lane, e.Decision.Reason)
}

func (e BackendAdmissionError) Unwrap() []error {
	if e.err != nil {
		return []error{ErrBackendAdmission, e.err}
	}
	return []error{ErrBackendAdmission}
}

func backendAdmissionError(dec BackendAdmissionDecision, err error) error {
	return BackendAdmissionError{Decision: dec, err: err}
}

func newBackendAdmissionDecision(
	ctx context.Context,
	op BackendOperation,
	reason BackendAdmissionReason,
	accepted bool,
	inflight, capacity, queued int,
) BackendAdmissionDecision {
	dec := BackendAdmissionDecision{
		Resource:  op.Resource,
		Lane:      op.Lane,
		Stage:     op.Stage,
		Operation: op.Operation,
		Accepted:  accepted,
		Reason:    reason,
		InFlight:  inflight,
		Capacity:  capacity,
		Queued:    queued,
	}
	if exec, ok := StageExecutionFromContext(ctx); ok {
		dec.RequestID = exec.RequestID
		if exec.Key != "" {
			dec.KeyHash = core.HashKey(exec.Key)
		}
		dec.RequestLane = exec.Lane
		dec.ShardID = exec.ShardID
		dec.Transport = exec.Transport
		if dec.Operation == "" {
			dec.Operation = exec.Operation
		}
		if dec.Stage == "" {
			dec.Stage = exec.Stage.Name
		}
		dec.StageIndex = exec.StageIndex
		dec.StageCount = exec.StageCount
		dec.Attempt = exec.Attempt
		dec.DeadlineRemaining = exec.Deadline.Remaining
		dec.DeadlineExhausted = exec.Deadline.BudgetExhausted
	}
	return dec
}

func backendAdmissionReasonFromContext(err error) BackendAdmissionReason {
	if errors.Is(err, context.Canceled) {
		return BackendAdmissionCancelled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return BackendAdmissionDeadlineExhausted
	}
	return BackendAdmissionCancelled
}
