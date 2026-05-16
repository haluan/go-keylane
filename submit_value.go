package keylane

import (
	"context"
)

// SubmitValue submits a ValueJob to the queue and returns a Future that will contain the result.
func SubmitValue[T any](
	ctx context.Context,
	q *Queue,
	job ValueJob[T],
) (Future[T], error) {
	future := newResultFuture[T]()
	var zero T

	if q == nil {
		future.complete(zero, ErrNilQueue)
		return future, ErrNilQueue
	}

	if err := validateValueJob(job); err != nil {
		future.complete(zero, err)
		return future, err
	}

	wrapped := Job{
		Key:  job.Key,
		Lane: job.Lane,
		Run: func(ctx context.Context) error {
			val, err := job.Run(ctx)
			future.complete(val, err)
			return nil // The scheduler itself only cares about execution flow, not results
		},
	}

	if err := q.Submit(ctx, wrapped); err != nil {
		future.complete(zero, err)
		return future, err
	}

	return future, nil
}
