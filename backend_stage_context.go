// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "context"

// BackendOperationFromStage builds a BackendOperation using explicit resource/lane and stage metadata from ctx.
func BackendOperationFromStage(ctx context.Context, resource BackendResourceName, lane BackendLane) BackendOperation {
	op := BackendOperation{Resource: resource, Lane: lane}
	if exec, ok := StageExecutionFromContext(ctx); ok {
		op.Stage = exec.Stage.Name
		if op.Operation == "" {
			op.Operation = exec.Operation
		}
	}
	return op
}

// AcquireBackend acquires a lease for the given backend operation on q.
func AcquireBackend(ctx context.Context, q *Queue, op BackendOperation) (BackendLease, error) {
	if q == nil {
		return nil, ErrNilQueue
	}
	if exec, ok := StageExecutionFromContext(ctx); ok {
		if op.Stage == "" {
			op.Stage = exec.Stage.Name
		}
		if op.Operation == "" {
			op.Operation = exec.Operation
		}
	}
	lease, _, err := q.backendCoord.acquire(ctx, q, op)
	return lease, err
}

// WithBackend runs fn while holding a backend lease for op.
func WithBackend[S any](ctx context.Context, q *Queue, op BackendOperation, fn func(context.Context) (S, error)) (S, error) {
	var zero S
	lease, err := AcquireBackend(ctx, q, op)
	if err != nil {
		return zero, err
	}
	defer lease.Release()
	return fn(ctx)
}
