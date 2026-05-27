// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestOnRetryEventFiresWhenHooksEnabled(t *testing.T) {
	var events atomic.Int32
	cfg := newTestConfig()
	cfg.Retry = RetryPolicy{
		Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond,
		Jitter: false, MinRemainingBudget: 0,
	}
	cfg.Observability.EnableHooks = true
	cfg.Observability.Hooks.Retry.OnRetryEvent = func(RetryEvent) {
		events.Add(1)
	}
	q, ctx := retryTestQueueWithConfig(t, cfg)
	var n atomic.Int32
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default", Idempotency: retrySafeIdempotency(),
		Run: func(context.Context) (int, error) {
			if n.Add(1) < 2 {
				return 0, RetryableFailure(errors.New("transient"))
			}
			return 1, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, awaitErr := future.Await(context.Background()); awaitErr != nil {
		t.Fatal(awaitErr)
	}
	if events.Load() == 0 {
		t.Fatal("expected retry events")
	}
}

func TestOnRetryEventDisabledWhenHooksOff(t *testing.T) {
	var events atomic.Int32
	cfg := newTestConfig()
	cfg.Retry = RetryPolicy{
		Enabled: true, MaxAttempts: 2, InitialBackoff: time.Millisecond,
		Jitter: false, MinRemainingBudget: 0,
	}
	cfg.Observability.EnableHooks = false
	cfg.Observability.Hooks.Retry.OnRetryEvent = func(RetryEvent) {
		events.Add(1)
	}
	q, ctx := retryTestQueueWithConfig(t, cfg)
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default", Idempotency: retrySafeIdempotency(),
		Run: func(context.Context) (int, error) {
			return 0, RetryableFailure(errors.New("x"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = future.Await(context.Background())
	if events.Load() != 0 {
		t.Fatalf("events = %d", events.Load())
	}
}

func TestOnRetryEventPanicRecovered(t *testing.T) {
	before := HookPanicsRecovered()
	cfg := newTestConfig()
	cfg.Retry = RetryPolicy{
		Enabled: true, MaxAttempts: 2, InitialBackoff: time.Millisecond,
		Jitter: false, MinRemainingBudget: 0,
	}
	cfg.Observability.EnableHooks = true
	cfg.Observability.Hooks.Retry.OnRetryEvent = func(RetryEvent) {
		panic("hook panic")
	}
	q, ctx := retryTestQueueWithConfig(t, cfg)
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default", Idempotency: retrySafeIdempotency(),
		Run: func(context.Context) (int, error) {
			return 0, PermanentFailure(errors.New("done"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, awaitErr := future.Await(context.Background()); awaitErr == nil {
		t.Fatal("expected error")
	}
	if HookPanicsRecovered() <= before {
		t.Fatalf("HookPanicsRecovered = %d, want > %d", HookPanicsRecovered(), before)
	}
}

func TestRetryEventUsesLowCardinalityFields(t *testing.T) {
	var got RetryEvent
	cfg := newTestConfig()
	cfg.Retry = RetryPolicy{
		Enabled: true, MaxAttempts: 2, InitialBackoff: time.Millisecond,
		Jitter: false, MinRemainingBudget: 0,
	}
	cfg.Observability.EnableHooks = true
	cfg.Observability.Hooks.Retry.OnRetryEvent = func(e RetryEvent) {
		got = e
	}
	q, ctx := retryTestQueueWithConfig(t, cfg)
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key:  "secret-key",
		Lane: "default",
		Idempotency: Idempotency{
			Safety: RetrySafetySafe, Key: "secret-idem", Scope: "payment", Operation: "charge",
		},
		Run: func(context.Context) (int, error) {
			return 0, PermanentFailure(errors.New("x"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = future.Await(context.Background())
	if got.Key != "" {
		t.Fatalf("Key = %q, want empty (redacted)", got.Key)
	}
	if got.KeyHash != HashKey("secret-key") {
		t.Fatalf("KeyHash = %d, want %d", got.KeyHash, HashKey("secret-key"))
	}
	if got.IdempotencyScope != "payment" || got.IdempotencyOperation != "charge" {
		t.Fatalf("event = %+v", got)
	}
	snap := q.RetryFailureSnapshot()
	for range snap.ByFailureKind {
		// snapshot has no idempotency key fields
	}
}

func TestAttemptsTotalCountsSuccessfulAttempts(t *testing.T) {
	q, err := New(newTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	var n atomic.Int32
	opts := runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetySafe}, Observer: q.retryObserver()}
	_ = runWithRetry(context.Background(), FailurePolicy{}, p, opts, NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
		if n.Add(1) < 2 {
			return 0, RetryableFailure(errors.New("t"))
		}
		return 1, nil
	})
	snap := q.RetryFailureSnapshot()
	if snap.AttemptsTotal != 2 {
		t.Fatalf("AttemptsTotal = %d, want 2", snap.AttemptsTotal)
	}
}

func TestRunWithRetryScheduledEmitsScheduledEvent(t *testing.T) {
	var kinds []RetryEventKind
	cfg := newTestConfig()
	cfg.Observability.EnableHooks = true
	cfg.Observability.Hooks.Retry.OnRetryEvent = func(e RetryEvent) {
		kinds = append(kinds, e.Kind)
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	clock := &testRetryClock{now: now}
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	var n atomic.Int32
	opts := runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetySafe}, Observer: q.retryObserver()}
	_ = runWithRetry(context.Background(), FailurePolicy{}, p, opts, NewDeadlineBudget(context.Background(), now), clock, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
		if n.Add(1) < 2 {
			return 0, RetryableFailure(errors.New("t"))
		}
		return 1, nil
	})
	var hasScheduled bool
	for _, k := range kinds {
		if k == RetryEventScheduled {
			hasScheduled = true
		}
	}
	if !hasScheduled {
		t.Fatalf("events = %v, want scheduled", kinds)
	}
	snap := q.RetryFailureSnapshot()
	if snap.RetriesScheduledTotal != 1 {
		t.Fatalf("RetriesScheduledTotal = %d, want 1", snap.RetriesScheduledTotal)
	}
	if retryReasonCount(snap, RetryDecisionRetryableFailure) < 1 {
		t.Fatalf("ByRetryReason = %+v", snap.ByRetryReason)
	}
}

func TestRunWithRetryDisabledNoScheduledRetry(t *testing.T) {
	var kinds []RetryEventKind
	cfg := newTestConfig()
	cfg.Observability.EnableHooks = true
	cfg.Observability.Hooks.Retry.OnRetryEvent = func(e RetryEvent) {
		kinds = append(kinds, e.Kind)
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	p := RetryPolicy{Enabled: false, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	opts := runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetySafe}, Observer: q.retryObserver()}
	_ = runWithRetry(context.Background(), FailurePolicy{}, p, opts, NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
		return 0, RetryableFailure(errors.New("t"))
	})
	for _, k := range kinds {
		if k == RetryEventScheduled {
			t.Fatalf("unexpected scheduled event when retry disabled: %v", kinds)
		}
	}
	if q.RetryFailureSnapshot().RetriesScheduledTotal != 0 {
		t.Fatal("expected zero scheduled counter when retry disabled")
	}
}

func TestRunWithRetryIncrementsScheduledCounter(t *testing.T) {
	q, err := New(newTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	clock := &testRetryClock{now: now}
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	var n atomic.Int32
	opts := runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetySafe}, Observer: q.retryObserver()}
	_ = runWithRetry(context.Background(), FailurePolicy{}, p, opts, NewDeadlineBudget(context.Background(), now), clock, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
		if n.Add(1) < 2 {
			return 0, RetryableFailure(errors.New("t"))
		}
		return 1, nil
	})
	snap := q.RetryFailureSnapshot()
	if snap.RetriesScheduledTotal != 1 {
		t.Fatalf("scheduled = %d", snap.RetriesScheduledTotal)
	}
}

func safetyReasonCount(snap RetryFailureSnapshot, reason RetrySafetyDecisionReason) uint64 {
	for _, c := range snap.BySafetyReason {
		if c.Reason == reason {
			return c.Count
		}
	}
	return 0
}

func retryReasonCount(snap RetryFailureSnapshot, reason RetryDecisionReason) uint64 {
	for _, c := range snap.ByRetryReason {
		if c.Reason == reason {
			return c.Count
		}
	}
	return 0
}

func suppressionReasonCount(snap RetryFailureSnapshot, reason RetrySuppressionReason) uint64 {
	for _, c := range snap.BySuppressionReason {
		if c.Reason == reason {
			return c.Count
		}
	}
	return 0
}

func runRetryObsSnapshot(
	t *testing.T,
	idempotency Idempotency,
	idempotencyPolicy IdempotencyPolicy,
	run func(int, DeadlineBudget) (int, error),
) RetryFailureSnapshot {
	t.Helper()
	q, err := New(newTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	opts := runWithRetryOpts{
		Idempotency:       idempotency,
		IdempotencyPolicy: idempotencyPolicy,
		Observer:          q.retryObserver(),
	}
	_ = runWithRetry(context.Background(), FailurePolicy{}, p, opts, NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), run)
	return q.RetryFailureSnapshot()
}

func TestRetrySafetyReasonObservability(t *testing.T) {
	t.Run("safe", func(t *testing.T) {
		var n atomic.Int32
		snap := runRetryObsSnapshot(t, Idempotency{Safety: RetrySafetySafe}, IdempotencyPolicy{}, func(int, DeadlineBudget) (int, error) {
			if n.Add(1) < 2 {
				return 0, RetryableFailure(errors.New("t"))
			}
			return 1, nil
		})
		if safetyReasonCount(snap, RetrySafetyDecisionSafe) < 1 {
			t.Fatalf("snap = %+v", snap)
		}
		if snap.RetrySafetySuppressedTotal != 0 {
			t.Fatalf("safety suppressed = %d", snap.RetrySafetySuppressedTotal)
		}
	})

	t.Run("hook_allowed", func(t *testing.T) {
		snap := runRetryObsSnapshot(t,
			Idempotency{Safety: RetrySafetyRequiresCheck, Key: "idem"},
			IdempotencyPolicy{Hook: func(context.Context, RetrySafetyCheck) RetrySafetyDecision {
				return RetrySafetyDecision{Allow: true, Reason: RetrySafetyDecisionHookAllowed}
			}},
			func(int, DeadlineBudget) (int, error) {
				return 0, RetryableFailure(errors.New("t"))
			},
		)
		if safetyReasonCount(snap, RetrySafetyDecisionHookAllowed) < 1 {
			t.Fatalf("snap = %+v", snap)
		}
	})

	t.Run("hook_rejected", func(t *testing.T) {
		snap := runRetryObsSnapshot(t,
			Idempotency{Safety: RetrySafetyRequiresCheck, Key: "idem"},
			IdempotencyPolicy{Hook: func(context.Context, RetrySafetyCheck) RetrySafetyDecision {
				return RetrySafetyDecision{Allow: false, Reason: RetrySafetyDecisionHookRejected}
			}},
			func(int, DeadlineBudget) (int, error) {
				return 0, RetryableFailure(errors.New("t"))
			},
		)
		if safetyReasonCount(snap, RetrySafetyDecisionHookRejected) != 1 {
			t.Fatalf("snap = %+v", snap)
		}
		if snap.RetrySafetySuppressedTotal != 1 {
			t.Fatalf("safety suppressed = %d", snap.RetrySafetySuppressedTotal)
		}
	})

	t.Run("hook_failed", func(t *testing.T) {
		snap := runRetryObsSnapshot(t,
			Idempotency{Safety: RetrySafetyRequiresCheck, Key: "idem"},
			IdempotencyPolicy{Hook: func(context.Context, RetrySafetyCheck) RetrySafetyDecision {
				panic("hook")
			}},
			func(int, DeadlineBudget) (int, error) {
				return 0, RetryableFailure(errors.New("t"))
			},
		)
		if safetyReasonCount(snap, RetrySafetyDecisionHookFailed) != 1 {
			t.Fatalf("snap = %+v", snap)
		}
		if snap.RetrySafetySuppressedTotal != 1 {
			t.Fatalf("safety suppressed = %d", snap.RetrySafetySuppressedTotal)
		}
	})

	t.Run("missing_idempotency_key", func(t *testing.T) {
		snap := runRetryObsSnapshot(t,
			Idempotency{Safety: RetrySafetySafe, Key: ""},
			IdempotencyPolicy{RequireForRetry: true},
			func(int, DeadlineBudget) (int, error) {
				return 0, RetryableFailure(errors.New("t"))
			},
		)
		if safetyReasonCount(snap, RetrySafetyDecisionMissingKey) != 1 {
			t.Fatalf("snap = %+v", snap)
		}
		if snap.RetrySafetySuppressedTotal != 1 {
			t.Fatalf("safety suppressed = %d", snap.RetrySafetySuppressedTotal)
		}
	})

	t.Run("unsafe", func(t *testing.T) {
		snap := runRetryObsSnapshot(t,
			Idempotency{Safety: RetrySafetyUnsafe},
			IdempotencyPolicy{},
			func(int, DeadlineBudget) (int, error) {
				return 0, RetryableFailure(errors.New("t"))
			},
		)
		if safetyReasonCount(snap, RetrySafetyDecisionUnsafe) < 1 {
			t.Fatalf("snap = %+v", snap)
		}
		if snap.RetrySafetySuppressedTotal < 1 {
			t.Fatalf("safety suppressed = %d", snap.RetrySafetySuppressedTotal)
		}
	})
}

func TestRunWithRetryExhaustedObservability(t *testing.T) {
	q, err := New(newTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	p := RetryPolicy{Enabled: true, MaxAttempts: 2, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	opts := runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetySafe}, Observer: q.retryObserver()}
	_ = runWithRetry(context.Background(), FailurePolicy{}, p, opts, NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
		return 0, RetryableFailure(errors.New("transient"))
	})
	snap := q.RetryFailureSnapshot()
	if snap.RetryExhaustedTotal != 1 {
		t.Fatalf("RetryExhaustedTotal = %d, want 1", snap.RetryExhaustedTotal)
	}
	if snap.RetryMaxAttemptsStoppedTotal != 1 {
		t.Fatalf("RetryMaxAttemptsStoppedTotal = %d, want 1", snap.RetryMaxAttemptsStoppedTotal)
	}
	if retryReasonCount(snap, RetryDecisionMaxAttempts) < 1 {
		t.Fatalf("ByRetryReason max_attempts = %+v", snap.ByRetryReason)
	}
}

func TestRunWithRetryPermanentFailureTerminalObservability(t *testing.T) {
	q, err := New(newTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	opts := runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetySafe}, Observer: q.retryObserver()}
	_ = runWithRetry(context.Background(), FailurePolicy{}, p, opts, NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
		return 0, PermanentFailure(errors.New("perm"))
	})
	snap := q.RetryFailureSnapshot()
	if snap.RetryMaxAttemptsStoppedTotal != 0 || snap.RetryExhaustedTotal != 0 || snap.RetryDeadlineStoppedTotal != 0 {
		t.Fatalf("snap = %+v", snap)
	}
	if retryReasonCount(snap, RetryDecisionPermanentFailure) < 1 {
		t.Fatalf("ByRetryReason = %+v", snap.ByRetryReason)
	}
}

func TestRunWithRetryContextCancelledTerminalObservability(t *testing.T) {
	q, err := New(newTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	now := time.Now()
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	opts := runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetySafe}, Observer: q.retryObserver()}
	_ = runWithRetry(ctx, FailurePolicy{}, p, opts, NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
		return 0, RetryableFailure(errors.New("t"))
	})
	snap := q.RetryFailureSnapshot()
	if snap.AttemptsTotal != 0 {
		t.Fatalf("AttemptsTotal = %d, want 0 when cancelled before handler", snap.AttemptsTotal)
	}
	if snap.RetryDeadlineStoppedTotal != 0 {
		t.Fatalf("RetryDeadlineStoppedTotal = %d", snap.RetryDeadlineStoppedTotal)
	}
	if snap.CancellationsTotal < 1 {
		t.Fatalf("CancellationsTotal = %d", snap.CancellationsTotal)
	}
	if retryReasonCount(snap, RetryDecisionContextCancelled) < 1 {
		t.Fatalf("ByRetryReason = %+v", snap.ByRetryReason)
	}
}

func TestRunWithRetryContextCancelledDuringSleepObservability(t *testing.T) {
	q, err := New(newTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := time.Now()
	clock := &cancelOnSleepClock{testRetryClock: testRetryClock{now: now}, cancel: cancel}
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	opts := runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetySafe}, Observer: q.retryObserver()}
	_ = runWithRetry(ctx, FailurePolicy{}, p, opts, NewDeadlineBudget(context.Background(), now), clock, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
		return 0, RetryableFailure(errors.New("t"))
	})
	snap := q.RetryFailureSnapshot()
	if snap.AttemptsTotal < 1 {
		t.Fatalf("AttemptsTotal = %d, want at least 1 before sleep cancel", snap.AttemptsTotal)
	}
	if snap.RetryDeadlineStoppedTotal != 0 {
		t.Fatalf("RetryDeadlineStoppedTotal = %d", snap.RetryDeadlineStoppedTotal)
	}
	if snap.CancellationsTotal < 1 {
		t.Fatalf("CancellationsTotal = %d", snap.CancellationsTotal)
	}
	if retryReasonCount(snap, RetryDecisionContextCancelled) < 1 {
		t.Fatalf("ByRetryReason = %+v", snap.ByRetryReason)
	}
}

func TestRunWithRetryExhaustedEmitsBothTerminalEvents(t *testing.T) {
	var kinds []RetryEventKind
	cfg := newTestConfig()
	cfg.Observability.EnableHooks = true
	cfg.Observability.Hooks.Retry.OnRetryEvent = func(e RetryEvent) {
		kinds = append(kinds, e.Kind)
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	p := RetryPolicy{Enabled: true, MaxAttempts: 2, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	opts := runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetySafe}, Observer: q.retryObserver()}
	_ = runWithRetry(context.Background(), FailurePolicy{}, p, opts, NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
		return 0, RetryableFailure(errors.New("transient"))
	})
	var hasMaxStopped, hasExhausted bool
	for _, k := range kinds {
		if k == RetryEventMaxAttemptsStopped {
			hasMaxStopped = true
		}
		if k == RetryEventExhausted {
			hasExhausted = true
		}
	}
	if !hasMaxStopped || !hasExhausted {
		t.Fatalf("events = %v, want max_attempts_stopped and exhausted", kinds)
	}
}

func TestRunWithRetryDeadlineStoppedObservability(t *testing.T) {
	q, err := New(newTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	budget := DeadlineBudget{HasDeadline: true, Deadline: now.Add(-time.Millisecond), StartedAt: now, Exhausted: true}
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	opts := runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetySafe}, Observer: q.retryObserver()}
	_ = runWithRetry(context.Background(), FailurePolicy{}, p, opts, budget, &testRetryClock{now: now}, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
		return 0, RetryableFailure(errors.New("t"))
	})
	snap := q.RetryFailureSnapshot()
	if snap.RetryDeadlineStoppedTotal < 1 {
		t.Fatalf("RetryDeadlineStoppedTotal = %d", snap.RetryDeadlineStoppedTotal)
	}
	if snap.RetryExhaustedTotal != 0 {
		t.Fatalf("RetryExhaustedTotal = %d", snap.RetryExhaustedTotal)
	}
	if retryReasonCount(snap, RetryDecisionDeadlineExhausted) < 1 {
		t.Fatalf("ByRetryReason = %+v", snap.ByRetryReason)
	}
}

type cancelOnSleepClock struct {
	testRetryClock
	cancel context.CancelFunc
}

func (c *cancelOnSleepClock) Sleep(ctx context.Context, d time.Duration) error {
	c.cancel()
	return ctx.Err()
}

func runRetrySuppressionObs(
	t *testing.T,
	policy RetrySuppressionPolicy,
	snapshot func(string, Lane, int) RetrySuppressionSnapshot,
	run func(int, DeadlineBudget) (int, error),
	retryableKinds ...FailureKind,
) RetryFailureSnapshot {
	t.Helper()
	q, err := New(newTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	if len(retryableKinds) > 0 {
		p.RetryableKinds = retryableKinds
	}
	opts := runWithRetryOpts{
		Idempotency:       Idempotency{Safety: RetrySafetySafe},
		SuppressionPolicy: policy,
		Observer:          q.retryObserver(),
		Snapshot:          snapshot,
	}
	_ = runWithRetry(context.Background(), FailurePolicy{}, p, opts, NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), run)
	return q.RetryFailureSnapshot()
}

func TestRetrySuppressionReasonObservability(t *testing.T) {
	retryable := func(int, DeadlineBudget) (int, error) {
		return 0, RetryableFailure(errors.New("t"))
	}
	healthySnap := func() RetrySuppressionSnapshot {
		return RetrySuppressionSnapshot{Pressure: healthyPressure(), LaneClass: LaneNormal}
	}

	t.Run("global_overload", func(t *testing.T) {
		snap := runRetrySuppressionObs(t, enabledSuppressionPolicy(), func(string, Lane, int) RetrySuppressionSnapshot {
			return RetrySuppressionSnapshot{Pressure: overloadedPressure(), LaneClass: LaneNormal}
		}, retryable)
		if snap.RetriesSuppressedTotal < 1 || suppressionReasonCount(snap, RetrySuppressionGlobalOverload) < 1 {
			t.Fatalf("snap = %+v", snap)
		}
	})

	t.Run("global_pressure", func(t *testing.T) {
		policy := enabledSuppressionPolicy()
		policy.SuppressWhenOverloaded = false
		snap := runRetrySuppressionObs(t, policy, func(string, Lane, int) RetrySuppressionSnapshot {
			return RetrySuppressionSnapshot{Pressure: pressuredPressure(), LaneClass: LaneBestEffort}
		}, retryable)
		if snap.RetriesSuppressedTotal < 1 || suppressionReasonCount(snap, RetrySuppressionGlobalPressure) < 1 {
			t.Fatalf("snap = %+v", snap)
		}
	})

	t.Run("hot_key", func(t *testing.T) {
		policy := enabledSuppressionPolicy()
		policy.SuppressHotKeyRetry = true
		snap := runRetrySuppressionObs(t, policy, func(string, Lane, int) RetrySuppressionSnapshot {
			s := healthySnap()
			s.LaneClass = LaneBackground
			s.HotKeyCandidate = true
			return s
		}, retryable)
		if snap.RetriesSuppressedTotal < 1 || suppressionReasonCount(snap, RetrySuppressionHotKey) < 1 {
			t.Fatalf("snap = %+v", snap)
		}
	})

	t.Run("lane_pressure", func(t *testing.T) {
		policy := enabledSuppressionPolicy()
		policy.SuppressWhenOverloaded = false
		policy.SuppressNonCriticalWhenPressured = false
		policy.SuppressLaneAboveRatio = 0.5
		snap := runRetrySuppressionObs(t, policy, func(string, Lane, int) RetrySuppressionSnapshot {
			s := healthySnap()
			s.LaneDepthRatio = 1.0
			return s
		}, retryable)
		if snap.RetriesSuppressedTotal < 1 || suppressionReasonCount(snap, RetrySuppressionLanePressure) < 1 {
			t.Fatalf("snap = %+v", snap)
		}
	})

	t.Run("shard_pressure", func(t *testing.T) {
		policy := enabledSuppressionPolicy()
		policy.SuppressWhenOverloaded = false
		policy.SuppressNonCriticalWhenPressured = false
		policy.SuppressShardAboveRatio = 0.5
		snap := runRetrySuppressionObs(t, policy, func(string, Lane, int) RetrySuppressionSnapshot {
			s := healthySnap()
			s.ShardDepthRatio = 1.0
			return s
		}, retryable)
		if snap.RetriesSuppressedTotal < 1 || suppressionReasonCount(snap, RetrySuppressionShardPressure) < 1 {
			t.Fatalf("snap = %+v", snap)
		}
	})

	t.Run("overload_failure", func(t *testing.T) {
		snap := runRetrySuppressionObs(t, enabledSuppressionPolicy(), func(string, Lane, int) RetrySuppressionSnapshot {
			return healthySnap()
		}, func(int, DeadlineBudget) (int, error) {
			return 0, OverloadedFailure(errors.New("overload"))
		}, FailureOverloaded)
		if snap.RetriesSuppressedTotal < 1 || suppressionReasonCount(snap, RetrySuppressionOverloadFailure) < 1 {
			t.Fatalf("snap = %+v", snap)
		}
	})

	t.Run("admission_failure", func(t *testing.T) {
		snap := runRetrySuppressionObs(t, enabledSuppressionPolicy(), func(string, Lane, int) RetrySuppressionSnapshot {
			return healthySnap()
		}, func(int, DeadlineBudget) (int, error) {
			return 0, RejectedFailure(ErrAdmissionRejected)
		}, FailureRejected)
		if snap.RetriesSuppressedTotal < 1 || suppressionReasonCount(snap, RetrySuppressionAdmissionFailure) < 1 {
			t.Fatalf("snap = %+v", snap)
		}
	})

	t.Run("per_key_admission", func(t *testing.T) {
		snap := runRetrySuppressionObs(t, enabledSuppressionPolicy(), func(string, Lane, int) RetrySuppressionSnapshot {
			return healthySnap()
		}, func(int, DeadlineBudget) (int, error) {
			return 0, RejectedFailure(ErrPerKeyAdmissionThrottled)
		}, FailureRejected)
		if snap.RetriesSuppressedTotal < 1 || suppressionReasonCount(snap, RetrySuppressionPerKeyAdmission) < 1 {
			t.Fatalf("snap = %+v", snap)
		}
	})

	t.Run("scale_out_recommended", func(t *testing.T) {
		snap := runRetrySuppressionObs(t, enabledSuppressionPolicy(), func(string, Lane, int) RetrySuppressionSnapshot {
			s := healthySnap()
			s.LaneClass = LaneBestEffort
			s.ScaleSignal = ScaleSignal{Recommended: true}
			return s
		}, retryable)
		if snap.RetriesSuppressedTotal < 1 || suppressionReasonCount(snap, RetrySuppressionScaleOutRecommended) < 1 {
			t.Fatalf("snap = %+v", snap)
		}
	})

	t.Run("hook_rejected", func(t *testing.T) {
		policy := enabledSuppressionPolicy()
		policy.Hook = func(context.Context, RetrySuppressionCheck) RetrySuppressionDecision {
			return RetrySuppressionDecision{Suppress: true, Reason: RetrySuppressionHookRejected}
		}
		snap := runRetrySuppressionObs(t, policy, func(string, Lane, int) RetrySuppressionSnapshot {
			return healthySnap()
		}, retryable)
		if snap.RetriesSuppressedTotal < 1 || suppressionReasonCount(snap, RetrySuppressionHookRejected) < 1 {
			t.Fatalf("snap = %+v", snap)
		}
	})

	t.Run("hook_failed", func(t *testing.T) {
		policy := enabledSuppressionPolicy()
		policy.Hook = func(context.Context, RetrySuppressionCheck) RetrySuppressionDecision {
			panic("hook")
		}
		snap := runRetrySuppressionObs(t, policy, func(string, Lane, int) RetrySuppressionSnapshot {
			return healthySnap()
		}, retryable)
		if snap.RetriesSuppressedTotal < 1 || suppressionReasonCount(snap, RetrySuppressionHookFailed) < 1 {
			t.Fatalf("snap = %+v", snap)
		}
	})
}

func TestOnRetryEventDoesNotChangeResult(t *testing.T) {
	now := time.Now()
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	clock := &testRetryClock{now: now}
	budget := NewDeadlineBudget(context.Background(), now)
	makeRun := func() func(int, DeadlineBudget) (int, error) {
		var n atomic.Int32
		return func(int, DeadlineBudget) (int, error) {
			if n.Add(1) < 2 {
				return 0, RetryableFailure(errors.New("t"))
			}
			return 42, nil
		}
	}

	baseline := runWithRetry(context.Background(), FailurePolicy{}, p, runWithRetryOpts{Idempotency: retrySafeIdempotency()}, budget, clock, fixedJitterSource(0.5), makeRun())

	q, err := New(newTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	withObs := runWithRetry(context.Background(), FailurePolicy{}, p, runWithRetryOpts{
		Idempotency: retrySafeIdempotency(), Observer: q.retryObserver(),
	}, budget, clock, fixedJitterSource(0.5), makeRun())

	cfg := newTestConfig()
	cfg.Observability.EnableHooks = true
	cfg.Observability.Hooks.Retry.OnRetryEvent = func(RetryEvent) {}
	q2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	withHook := runWithRetry(context.Background(), FailurePolicy{}, p, runWithRetryOpts{
		Idempotency: retrySafeIdempotency(), Observer: q2.retryObserver(),
	}, budget, clock, fixedJitterSource(0.5), makeRun())

	assertRetryResultsEqual(t, baseline, withObs)
	assertRetryResultsEqual(t, baseline, withHook)
}

func assertRetryResultsEqual(t *testing.T, a, b runWithRetryResult[int]) {
	t.Helper()
	if a.val != b.val {
		t.Fatalf("val %d vs %d", a.val, b.val)
	}
	if !errorsEqual(a.err, b.err) {
		t.Fatalf("err %v vs %v", a.err, b.err)
	}
	if a.beforeHandler != b.beforeHandler {
		t.Fatalf("beforeHandler %v vs %v", a.beforeHandler, b.beforeHandler)
	}
	if len(a.retryAttempts) != len(b.retryAttempts) {
		t.Fatalf("attempts %d vs %d", len(a.retryAttempts), len(b.retryAttempts))
	}
	if a.retryFinal != b.retryFinal {
		t.Fatalf("final %+v vs %+v", a.retryFinal, b.retryFinal)
	}
}

func errorsEqual(a, b error) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return errors.Is(a, b) || a.Error() == b.Error()
}

func retryTestQueueWithConfig(t *testing.T, cfg Config) (*Queue, context.Context) {
	t.Helper()
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return q, ctx
}
