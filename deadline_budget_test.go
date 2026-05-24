// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"testing"
	"time"
)

func TestNewDeadlineBudgetNoDeadline(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	b := NewDeadlineBudget(context.Background(), now)
	if b.HasDeadline {
		t.Fatal("expected no deadline")
	}
	if b.Exhausted {
		t.Fatal("no deadline should not be exhausted")
	}
}

func TestDeadlineBudgetRemaining(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(100 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	b := NewDeadlineBudget(ctx, now)
	if !b.HasDeadline {
		t.Fatal("expected deadline")
	}
	rem := b.RemainingAt(now.Add(30 * time.Millisecond))
	if rem != 70*time.Millisecond {
		t.Fatalf("remaining = %v, want 70ms", rem)
	}
}

func TestDeadlineBudgetExhausted(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(50 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	b := NewDeadlineBudget(ctx, now)
	if !b.IsExhaustedAt(now.Add(60 * time.Millisecond)) {
		t.Fatal("expected exhausted after deadline")
	}
}

func TestDeadlineBudgetWithQueueWaitAndRuntime(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(200 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	b := NewDeadlineBudget(ctx, now)
	b = b.WithQueueWaitAt(40*time.Millisecond, now.Add(40*time.Millisecond))
	if b.QueueWait != 40*time.Millisecond {
		t.Fatalf("queue wait = %v", b.QueueWait)
	}
	b = b.WithRuntimeAt(30*time.Millisecond, now.Add(70*time.Millisecond))
	if b.Runtime != 30*time.Millisecond {
		t.Fatalf("runtime = %v", b.Runtime)
	}
	rem := b.RemainingAt(now.Add(70 * time.Millisecond))
	if rem != 130*time.Millisecond {
		t.Fatalf("remaining = %v, want 130ms", rem)
	}
}

func TestHasRemaining(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(10 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	b := NewDeadlineBudget(ctx, now)
	if !b.HasRemainingAt(5*time.Millisecond, now.Add(2*time.Millisecond)) {
		t.Fatal("expected remaining >= 5ms")
	}
	if b.HasRemainingAt(20*time.Millisecond, now.Add(5*time.Millisecond)) {
		t.Fatal("expected insufficient remaining")
	}
}

func TestClassifyContextErrorDeadlineExhaustedBeforeHandler(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	deadline := now
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	b := NewDeadlineBudget(ctx, now.Add(-time.Millisecond))
	f := ClassifyContextErrorAt(context.DeadlineExceeded, b, true, now)
	if f.Kind != FailureDeadlineExhausted {
		t.Fatalf("kind = %q, want deadline_exhausted", f.Kind)
	}
}

func TestClassifyContextErrorTimeoutDuringHandler(t *testing.T) {
	now := time.Now()
	b := NewDeadlineBudget(context.Background(), now)
	f := ClassifyContextErrorAt(context.DeadlineExceeded, b, false, now)
	if f.Kind != FailureTimeout {
		t.Fatalf("kind = %q, want timeout", f.Kind)
	}
}

func TestDeadlineBudgetMonotonicSub(t *testing.T) {
	base := time.Now()
	deadline := base.Add(100 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	b := NewDeadlineBudget(ctx, base)
	// Strip monotonic reading so two wall times share the same monotonic clock.
	fixedDeadline := time.Unix(0, deadline.UnixNano())
	b.Deadline = fixedDeadline

	now1 := time.Unix(0, base.Add(10*time.Millisecond).UnixNano())
	now2 := time.Unix(0, base.Add(40*time.Millisecond).UnixNano())

	rem1 := b.RemainingAt(now1)
	rem2 := b.RemainingAt(now2)
	if rem1 != 90*time.Millisecond {
		t.Fatalf("rem1 = %v, want 90ms", rem1)
	}
	if rem2 != 60*time.Millisecond {
		t.Fatalf("rem2 = %v, want 60ms", rem2)
	}
	if rem2 >= rem1 {
		t.Fatalf("remaining should decrease: rem1=%v rem2=%v", rem1, rem2)
	}

	refreshed := b.refreshAt(now2)
	if refreshed.Remaining != rem2 {
		t.Fatalf("refreshAt remaining = %v, want %v", refreshed.Remaining, rem2)
	}
}

func TestDeadlineBudgetTracePhasesDecreaseRemaining(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(100 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	submit := NewDeadlineBudget(ctx, now)
	admit := submit.refreshAt(now.Add(10 * time.Millisecond))
	queued := admit.WithQueueWaitAt(20*time.Millisecond, now.Add(30*time.Millisecond))
	start := queued.refreshAt(now.Add(35 * time.Millisecond))
	done := start.WithRuntimeAt(10*time.Millisecond, now.Add(45*time.Millisecond))

	trace := DeadlineBudgetTrace{
		AtSubmit:       submit,
		AtAdmission:    admit,
		AfterQueueWait: queued,
		AtHandlerStart: start,
		AtCompletion:   done,
	}

	remaining := []time.Duration{
		trace.AtSubmit.RemainingAt(now),
		trace.AtAdmission.RemainingAt(now.Add(10 * time.Millisecond)),
		trace.AfterQueueWait.RemainingAt(now.Add(30 * time.Millisecond)),
		trace.AtHandlerStart.RemainingAt(now.Add(35 * time.Millisecond)),
		trace.AtCompletion.RemainingAt(now.Add(45 * time.Millisecond)),
	}
	for i := 1; i < len(remaining); i++ {
		if remaining[i] > remaining[i-1] {
			t.Fatalf("remaining increased at phase %d: %v -> %v", i, remaining[i-1], remaining[i])
		}
	}
}

func TestClassifyContextErrorCancelled(t *testing.T) {
	f := ClassifyContextError(context.Canceled, DeadlineBudget{}, true)
	if f.Kind != FailureCancelled {
		t.Fatalf("kind = %q", f.Kind)
	}
}
