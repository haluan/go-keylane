// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "context"

// SubmitRequest submits a typed request into the queue and returns a Future for the output.
func SubmitRequest[I any, O any](
	ctx context.Context,
	q *Queue,
	req Request[I, O],
) (Future[O], error) {
	future := newResultFuture[O]()
	var zero O

	if q == nil {
		future.complete(zero, ErrNilQueue)
		return future, ErrNilQueue
	}

	if err := validateRequest(req); err != nil {
		future.complete(zero, err)
		return future, err
	}

	input := req.Input
	handle := req.Handle
	wrapped := Job{
		Key:  req.Meta.Key,
		Lane: req.Meta.Lane,
		Run: func(ctx context.Context) error {
			out, err := handle(ctx, input)
			if err != nil {
				future.complete(zero, err)
			} else {
				future.complete(out, nil)
			}
			return nil
		},
	}

	if err := q.Submit(ctx, wrapped); err != nil {
		future.complete(zero, err)
		return future, err
	}

	return future, nil
}
