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

func idempotencyPolicyWithTrackingHook(t *testing.T) (*atomic.Int32, IdempotencyPolicy) {
	t.Helper()
	var calls atomic.Int32
	return &calls, IdempotencyPolicy{
		Hook: func(context.Context, RetrySafetyCheck) RetrySafetyDecision {
			calls.Add(1)
			return RetrySafetyDecision{Allow: true, Reason: RetrySafetyDecisionHookAllowed}
		},
	}
}

func runWithRetryRequiresCheckOpts(policy IdempotencyPolicy) runWithRetryOpts {
	return runWithRetryOpts{
		Idempotency:       Idempotency{Safety: RetrySafetyRequiresCheck, Key: "idem"},
		IdempotencyPolicy: policy,
	}
}

func TestDecideRetrySafetyRetryDisabledAllows(t *testing.T) {
	check := RetrySafetyCheck{
		Retry:       RetryPolicy{Enabled: false},
		Idempotency: Idempotency{},
	}
	got := DecideRetrySafety(context.Background(), check, IdempotencyPolicy{})
	if !got.Allow || got.Reason != RetrySafetyDecisionSafe {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySafetyUnspecifiedSuppresses(t *testing.T) {
	check := RetrySafetyCheck{
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: Idempotency{},
	}
	got := DecideRetrySafety(context.Background(), check, IdempotencyPolicy{})
	if got.Allow || got.Reason != RetrySafetyDecisionUnsafe {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySafetySafeAllows(t *testing.T) {
	check := RetrySafetyCheck{
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: Idempotency{Safety: RetrySafetySafe},
	}
	got := DecideRetrySafety(context.Background(), check, IdempotencyPolicy{})
	if !got.Allow || got.Reason != RetrySafetyDecisionSafe {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySafetyUnsafeSuppresses(t *testing.T) {
	check := RetrySafetyCheck{
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: Idempotency{Safety: RetrySafetyUnsafe},
	}
	got := DecideRetrySafety(context.Background(), check, IdempotencyPolicy{})
	if got.Allow || got.Reason != RetrySafetyDecisionUnsafe {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySafetyAllowUnsafeRetryOverride(t *testing.T) {
	check := RetrySafetyCheck{
		Retry: RetryPolicy{Enabled: true},
		Idempotency: Idempotency{
			Safety:           RetrySafetyUnsafe,
			AllowUnsafeRetry: true,
		},
	}
	got := DecideRetrySafety(context.Background(), check, IdempotencyPolicy{})
	if !got.Allow || got.Reason != RetrySafetyDecisionExplicitOverride {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySafetyRequiresCheckNoHookSuppresses(t *testing.T) {
	check := RetrySafetyCheck{
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: Idempotency{Safety: RetrySafetyRequiresCheck, Key: "k"},
	}
	got := DecideRetrySafety(context.Background(), check, IdempotencyPolicy{})
	if got.Allow || got.Reason != RetrySafetyDecisionNoHook {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySafetyRequiresCheckHookAllow(t *testing.T) {
	check := RetrySafetyCheck{
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: Idempotency{Safety: RetrySafetyRequiresCheck, Key: "k"},
	}
	policy := IdempotencyPolicy{
		Hook: func(context.Context, RetrySafetyCheck) RetrySafetyDecision {
			return RetrySafetyDecision{Allow: true, Reason: RetrySafetyDecisionHookAllowed}
		},
	}
	got := DecideRetrySafety(context.Background(), check, policy)
	if !got.Allow || got.Reason != RetrySafetyDecisionHookAllowed {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySafetyRequiresCheckHookReject(t *testing.T) {
	check := RetrySafetyCheck{
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: Idempotency{Safety: RetrySafetyRequiresCheck, Key: "k"},
	}
	policy := IdempotencyPolicy{
		Hook: func(context.Context, RetrySafetyCheck) RetrySafetyDecision {
			return RetrySafetyDecision{Allow: false, Reason: RetrySafetyDecisionHookRejected}
		},
	}
	got := DecideRetrySafety(context.Background(), check, policy)
	if got.Allow || got.Reason != RetrySafetyDecisionHookRejected {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySafetyRequireForRetryMissingKey(t *testing.T) {
	check := RetrySafetyCheck{
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: Idempotency{Safety: RetrySafetyRequiresCheck},
	}
	policy := IdempotencyPolicy{
		RequireForRetry: true,
		Hook: func(context.Context, RetrySafetyCheck) RetrySafetyDecision {
			return RetrySafetyDecision{Allow: true}
		},
	}
	got := DecideRetrySafety(context.Background(), check, policy)
	if got.Allow || got.Reason != RetrySafetyDecisionMissingKey {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySafetyRequireForRetrySafeEmptyKeySuppresses(t *testing.T) {
	check := RetrySafetyCheck{
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: Idempotency{Safety: RetrySafetySafe},
	}
	got := DecideRetrySafety(context.Background(), check, IdempotencyPolicy{RequireForRetry: true})
	if got.Allow || got.Reason != RetrySafetyDecisionMissingKey {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySafetyRequireForRetrySafeWithKeyAllows(t *testing.T) {
	check := RetrySafetyCheck{
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: Idempotency{Safety: RetrySafetySafe, Key: "k"},
	}
	got := DecideRetrySafety(context.Background(), check, IdempotencyPolicy{RequireForRetry: true})
	if !got.Allow || got.Reason != RetrySafetyDecisionSafe {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySafetyAllowUnsafeRetryOverridesRequireForRetry(t *testing.T) {
	check := RetrySafetyCheck{
		Retry: RetryPolicy{Enabled: true},
		Idempotency: Idempotency{
			Safety:           RetrySafetyUnsafe,
			AllowUnsafeRetry: true,
		},
	}
	got := DecideRetrySafety(context.Background(), check, IdempotencyPolicy{RequireForRetry: true})
	if !got.Allow || got.Reason != RetrySafetyDecisionExplicitOverride {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySafetyHookNormalizesInconsistentReason(t *testing.T) {
	check := RetrySafetyCheck{
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: Idempotency{Safety: RetrySafetyRequiresCheck, Key: "k"},
	}
	policy := IdempotencyPolicy{
		Hook: func(context.Context, RetrySafetyCheck) RetrySafetyDecision {
			return RetrySafetyDecision{Allow: true, Reason: RetrySafetyDecisionUnsafe}
		},
	}
	got := DecideRetrySafety(context.Background(), check, policy)
	if !got.Allow || got.Reason != RetrySafetyDecisionHookAllowed {
		t.Fatalf("got %+v", got)
	}
}

func TestRunWithRetryPermanentFailureDoesNotCallSafetyHook(t *testing.T) {
	hookCalls, policy := idempotencyPolicyWithTrackingHook(t)
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false}
	attempts := 0
	now := time.Now()
	res := runWithRetry(context.Background(), FailurePolicy{}, p, runWithRetryRequiresCheckOpts(policy),
		NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
			attempts++
			return 0, PermanentFailure(errors.New("perm"))
		})
	if res.err == nil {
		t.Fatal("expected error")
	}
	if attempts != 1 || hookCalls.Load() != 0 {
		t.Fatalf("attempts=%d hookCalls=%d", attempts, hookCalls.Load())
	}
}

func TestRunWithRetryContextCancelDoesNotCallSafetyHook(t *testing.T) {
	hookCalls, policy := idempotencyPolicyWithTrackingHook(t)
	ctx, cancel := context.WithCancel(context.Background())
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false}
	now := time.Now()
	clock := &testRetryClock{now: now}
	attempts := 0
	res := runWithRetry(ctx, FailurePolicy{}, p, runWithRetryRequiresCheckOpts(policy),
		NewDeadlineBudget(ctx, now), clock, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
			attempts++
			cancel()
			return 0, RetryableFailure(errors.New("transient"))
		})
	if res.err == nil {
		t.Fatal("expected error")
	}
	if attempts != 1 || hookCalls.Load() != 0 {
		t.Fatalf("attempts=%d hookCalls=%d", attempts, hookCalls.Load())
	}
}

func TestRunWithRetryDeadlineExhaustedDoesNotCallSafetyHook(t *testing.T) {
	hookCalls, policy := idempotencyPolicyWithTrackingHook(t)
	now := time.Now()
	ctx, cancel := context.WithDeadline(context.Background(), now.Add(2*time.Millisecond))
	defer cancel()
	p := RetryPolicy{
		Enabled: true, MaxAttempts: 3, InitialBackoff: 100 * time.Millisecond,
		Jitter: false, MinRemainingBudget: time.Millisecond,
	}
	budget := NewDeadlineBudget(ctx, now)
	clock := &testRetryClock{now: now}
	attempts := 0
	res := runWithRetry(ctx, FailurePolicy{}, p, runWithRetryRequiresCheckOpts(policy), budget, clock, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
		attempts++
		return 0, RetryableFailure(errors.New("transient"))
	})
	if res.err == nil {
		t.Fatal("expected error")
	}
	if attempts != 1 || hookCalls.Load() != 0 {
		t.Fatalf("attempts=%d hookCalls=%d", attempts, hookCalls.Load())
	}
}

func TestDecideRetrySafetyHookPanicSuppresses(t *testing.T) {
	check := RetrySafetyCheck{
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: Idempotency{Safety: RetrySafetyRequiresCheck, Key: "k"},
	}
	policy := IdempotencyPolicy{
		Hook: func(context.Context, RetrySafetyCheck) RetrySafetyDecision {
			panic("hook")
		},
	}
	got := DecideRetrySafety(context.Background(), check, policy)
	if got.Allow || got.Reason != RetrySafetyDecisionHookFailed {
		t.Fatalf("got %+v", got)
	}
}

func TestRunWithRetryRecordsExplicitUnsafeOverride(t *testing.T) {
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false}
	now := time.Now()
	res := runWithRetry(context.Background(), FailurePolicy{}, p, runWithRetryOpts{
		Idempotency: Idempotency{Safety: RetrySafetyUnsafe, AllowUnsafeRetry: true},
	}, NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
		return 0, RetryableFailure(errors.New("transient"))
	})
	trace := RetryTrace{Attempts: res.retryAttempts}
	if len(trace.Attempts) == 0 || !trace.HadExplicitUnsafeRetry() {
		t.Fatalf("retryAttempts = %+v", res.retryAttempts)
	}
}

func TestRunWithRetrySuppressesWhenUnsafe(t *testing.T) {
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false}
	attempts := 0
	now := time.Now()
	res := runWithRetry(context.Background(), FailurePolicy{}, p, runWithRetryOpts{
		Idempotency: Idempotency{Safety: RetrySafetyUnsafe},
	}, NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
		attempts++
		return 0, RetryableFailure(errors.New("transient"))
	})
	if res.err == nil {
		t.Fatal("expected error")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d", attempts)
	}
}

func TestValidateIdempotencyPolicy(t *testing.T) {
	if err := ValidateIdempotencyPolicy(IdempotencyPolicy{}); err != nil {
		t.Fatal(err)
	}
}
