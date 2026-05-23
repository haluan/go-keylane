// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Package keylane request runtime (SubmitRequest) uses Go context cancellation semantics.
//
// Cancellation is cooperative: Keylane does not forcibly terminate handler goroutines.
// Handlers should observe ctx.Done() for long-running or blocking work.
//
// The context passed to SubmitRequest controls request execution. If it is cancelled
// before enqueue, the request is not submitted. If cancelled while queued, the handler
// is skipped when the job is dequeued. If cancelled while running, the same context
// is passed to Handle so the handler can return early.
//
// The context passed to Future.Await controls caller waiting only. An Await timeout or
// cancellation does not cancel the underlying request; the handler may still complete
// and the Future remains safe to await again.
package keylane

import "context"

// RequestMeta holds routing and optional identity metadata for a request.
type RequestMeta struct {
	// RequestID is optional caller-provided identity (not used for routing in v0.3.0).
	RequestID string
	// Key routes the request to a shard (required).
	Key string
	// Lane selects the workload class queue (required).
	Lane Lane
	// Transport is an optional transport name (for example "http" or "worker").
	Transport string
	// Operation is an optional stable operation name for observability (low cardinality).
	Operation string
}

// Request is a typed unit of business work with input and output.
type Request[I any, O any] struct {
	Meta      RequestMeta
	Admission AdmissionConfig
	Overload  OverloadConfig
	Input     I
	Handle    func(context.Context, I) (O, error)
}

func validateRequest[I any, O any](req Request[I, O]) error {
	if req.Meta.Key == "" {
		return ErrInvalidKey
	}
	if err := req.Meta.Lane.Validate(); err != nil {
		return err
	}
	if req.Handle == nil {
		return ErrNilJobRun
	}
	return nil
}
