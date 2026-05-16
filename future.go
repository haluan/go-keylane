package keylane

import (
	"context"
	"sync"
)

// Future represents a value that will be available in the future.
type Future[T any] interface {
	// Await blocks until the result is available or the context is cancelled.
	// It returns the value and any error that occurred during job execution.
	Await(ctx context.Context) (T, error)

	// Done returns a channel that is closed when the result is available.
	Done() <-chan struct{}
}

type resultFuture[T any] struct {
	done chan struct{}
	once sync.Once
	mu   sync.Mutex
	val  T
	err  error
}

func newResultFuture[T any]() *resultFuture[T] {
	return &resultFuture[T]{
		done: make(chan struct{}),
	}
}

func (f *resultFuture[T]) complete(val T, err error) {
	f.once.Do(func() {
		f.mu.Lock()
		f.val = val
		f.err = err
		f.mu.Unlock()
		close(f.done)
	})
}

func (f *resultFuture[T]) Await(ctx context.Context) (T, error) {
	select {
	case <-f.done:
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.val, f.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

func (f *resultFuture[T]) Done() <-chan struct{} {
	return f.done
}
