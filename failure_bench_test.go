// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"testing"
	"time"
)

func BenchmarkClassifyFailureNil(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = ClassifyFailure(nil)
	}
}

func BenchmarkClassifyFailureCanceled(b *testing.B) {
	err := context.Canceled
	for i := 0; i < b.N; i++ {
		_ = ClassifyFailure(err)
	}
}

func BenchmarkClassifyFailureDeadlineExceeded(b *testing.B) {
	err := context.DeadlineExceeded
	for i := 0; i < b.N; i++ {
		_ = ClassifyFailure(err)
	}
}

func BenchmarkClassifyFailurePlainError(b *testing.B) {
	err := errors.New("plain")
	for i := 0; i < b.N; i++ {
		_ = ClassifyFailure(err)
	}
}

func BenchmarkNewDeadlineBudget(b *testing.B) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second))
	defer cancel()
	now := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewDeadlineBudget(ctx, now)
	}
}

func BenchmarkDeadlineBudgetHasRemaining(b *testing.B) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second))
	defer cancel()
	budget := NewDeadlineBudget(ctx, time.Now())
	now := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = budget.HasRemainingAt(time.Millisecond, now)
	}
}

func BenchmarkResultFutureComplete(b *testing.B) {
	f := newResultFuture[int]()
	policy := FailurePolicy{}
	err := errors.New("err")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f2 := newResultFuture[int]()
		f2.complete(0, err, policy, DeadlineBudget{}, false)
	}
	_ = f
}
