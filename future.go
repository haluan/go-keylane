// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Future represents a value that will be available in the future.
type Future[T any] interface {
	// Await blocks until the result is available or the context is cancelled.
	// It returns the value and any error that occurred during job execution.
	Await(ctx context.Context) (T, error)

	// Done returns a channel that is closed when the result is available.
	Done() <-chan struct{}
}

type failureMetadataFuture interface {
	failureMetadata() (Failure, DeadlineBudget, DeadlineBudgetTrace, bool)
}

type resultFuture[T any] struct {
	done        chan struct{}
	once        sync.Once
	mu          sync.Mutex
	val         T
	err         error
	failure     Failure
	budget      DeadlineBudget
	budgetTrace DeadlineBudgetTrace
}

func newResultFuture[T any]() *resultFuture[T] {
	return &resultFuture[T]{
		done: make(chan struct{}),
	}
}

func (f *resultFuture[T]) failureMetadata() (Failure, DeadlineBudget, DeadlineBudgetTrace, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.failure, f.budget, f.budgetTrace, true
}

func (f *resultFuture[T]) complete(val T, err error, policy FailurePolicy, budget DeadlineBudget, beforeHandler bool) {
	failure := NewFailure(FailureNone, nil)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			failure = ClassifyContextErrorAt(err, budget, beforeHandler, time.Now())
		} else {
			failure = classifyFailureWithPolicy(err, policy)
		}
	}
	f.once.Do(func() {
		f.mu.Lock()
		f.val = val
		f.budget = budget
		f.budgetTrace.AtCompletion = budget
		f.failure = failure
		if err != nil && failure.Kind != FailureNone {
			f.err = failure
		} else {
			f.err = err
		}
		f.mu.Unlock()
		close(f.done)
	})
}

func (f *resultFuture[T]) completeSimple(val T, err error, policy FailurePolicy) {
	f.complete(val, err, policy, DeadlineBudget{}, false)
}

// completeValue completes the future with default failure classification (tests and internal paths).
func (f *resultFuture[T]) completeValue(val T, err error) {
	f.complete(val, err, FailurePolicy{}, DeadlineBudget{}, false)
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

// FailureFromFuture returns classified failure metadata when f is a keylane result future.
func FailureFromFuture[T any](f Future[T]) (Failure, bool) {
	rf, ok := f.(*resultFuture[T])
	if !ok {
		return Failure{}, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.failure, true
}

// BudgetFromFuture returns the deadline budget snapshot when f is a keylane result future.
func BudgetFromFuture[T any](f Future[T]) (DeadlineBudget, bool) {
	rf, ok := f.(*resultFuture[T])
	if !ok {
		return DeadlineBudget{}, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.budget, true
}

// BudgetTraceFromFuture returns lifecycle budget snapshots when f is a keylane result future.
func BudgetTraceFromFuture[T any](f Future[T]) (DeadlineBudgetTrace, bool) {
	rf, ok := f.(*resultFuture[T])
	if !ok {
		return DeadlineBudgetTrace{}, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.budgetTrace, true
}

// FailureFromFutureAny returns failure metadata from any result future without a typed output.
func FailureFromFutureAny(f any) (Failure, bool) {
	if fm, ok := f.(failureMetadataFuture); ok {
		fail, _, _, ok := fm.failureMetadata()
		return fail, ok
	}
	return Failure{}, false
}

// BudgetFromFutureAny returns the completion budget snapshot from any result future.
func BudgetFromFutureAny(f any) (DeadlineBudget, bool) {
	if fm, ok := f.(failureMetadataFuture); ok {
		_, b, _, ok := fm.failureMetadata()
		return b, ok
	}
	return DeadlineBudget{}, false
}

// BudgetTraceFromFutureAny returns lifecycle budget snapshots from any result future.
func BudgetTraceFromFutureAny(f any) (DeadlineBudgetTrace, bool) {
	if fm, ok := f.(failureMetadataFuture); ok {
		_, _, trace, ok := fm.failureMetadata()
		return trace, ok
	}
	return DeadlineBudgetTrace{}, false
}
