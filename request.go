// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

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
}

// Request is a typed unit of business work with input and output.
type Request[I any, O any] struct {
	Meta   RequestMeta
	Input  I
	Handle func(context.Context, I) (O, error)
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
