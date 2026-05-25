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

func healthyPressure() Pressure {
	return Pressure{TotalDepthRatio: 0.1, IsHealthy: true}
}

func pressuredPressure() Pressure {
	return Pressure{TotalDepthRatio: PressuredDepthRatio, IsPressured: true}
}

func overloadedPressure() Pressure {
	return Pressure{TotalDepthRatio: OverloadedDepthRatio, IsOverloaded: true}
}

func suppressionCheck() RetrySuppressionCheck {
	return RetrySuppressionCheck{
		Attempt:   1,
		Failure:   RetryableFailure(errors.New("transient")),
		Retry:     RetryPolicy{Enabled: true},
		Pressure:  healthyPressure(),
		LaneClass: LaneNormal,
	}
}

func enabledSuppressionPolicy() RetrySuppressionPolicy {
	p := RetrySuppressionPolicy{Enabled: true}
	NormalizeRetrySuppressionPolicy(&p)
	return p
}

func TestDecideRetrySuppressionDisabledAllows(t *testing.T) {
	got := DecideRetrySuppression(context.Background(), RetrySuppressionPolicy{}, suppressionCheck())
	if got.Suppress || got.Reason != RetrySuppressionDisabled {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySuppressionHealthyAllows(t *testing.T) {
	got := DecideRetrySuppression(context.Background(), enabledSuppressionPolicy(), suppressionCheck())
	if got.Suppress {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySuppressionGlobalOverloadSuppresses(t *testing.T) {
	check := suppressionCheck()
	check.Pressure = overloadedPressure()
	got := DecideRetrySuppression(context.Background(), enabledSuppressionPolicy(), check)
	if !got.Suppress || got.Reason != RetrySuppressionGlobalOverload {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySuppressionBestEffortPressuredSuppresses(t *testing.T) {
	check := suppressionCheck()
	check.Pressure = pressuredPressure()
	check.LaneClass = LaneBestEffort
	got := DecideRetrySuppression(context.Background(), enabledSuppressionPolicy(), check)
	if !got.Suppress || got.Reason != RetrySuppressionGlobalPressure {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySuppressionCriticalPressuredAllows(t *testing.T) {
	check := suppressionCheck()
	check.Pressure = pressuredPressure()
	check.LaneClass = LaneCritical
	got := DecideRetrySuppression(context.Background(), enabledSuppressionPolicy(), check)
	if got.Suppress {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySuppressionCriticalOverloadedSuppresses(t *testing.T) {
	check := suppressionCheck()
	check.Pressure = overloadedPressure()
	check.LaneClass = LaneCritical
	got := DecideRetrySuppression(context.Background(), enabledSuppressionPolicy(), check)
	if !got.Suppress || got.Reason != RetrySuppressionGlobalOverload {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySuppressionOverloadFailureSuppresses(t *testing.T) {
	check := suppressionCheck()
	check.Failure = OverloadedFailure(errors.New("overload"))
	got := DecideRetrySuppression(context.Background(), enabledSuppressionPolicy(), check)
	if !got.Suppress || got.Reason != RetrySuppressionOverloadFailure {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySuppressionAdmissionFailureSuppresses(t *testing.T) {
	check := suppressionCheck()
	check.Failure = RejectedFailure(ErrAdmissionRejected)
	got := DecideRetrySuppression(context.Background(), enabledSuppressionPolicy(), check)
	if !got.Suppress || got.Reason != RetrySuppressionAdmissionFailure {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySuppressionPerKeyAdmissionSuppresses(t *testing.T) {
	check := suppressionCheck()
	check.Failure = RejectedFailure(ErrPerKeyAdmissionThrottled)
	got := DecideRetrySuppression(context.Background(), enabledSuppressionPolicy(), check)
	if !got.Suppress || got.Reason != RetrySuppressionPerKeyAdmission {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySuppressionHotKeyBackgroundSuppresses(t *testing.T) {
	check := suppressionCheck()
	check.LaneClass = LaneBackground
	check.HotKeyCandidate = true
	got := DecideRetrySuppression(context.Background(), enabledSuppressionPolicy(), check)
	if !got.Suppress || got.Reason != RetrySuppressionHotKey {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySuppressionCriticalHotKeyDefaultSuppresses(t *testing.T) {
	check := suppressionCheck()
	check.LaneClass = LaneCritical
	check.HotKeyCandidate = true
	got := DecideRetrySuppression(context.Background(), enabledSuppressionPolicy(), check)
	if !got.Suppress || got.Reason != RetrySuppressionHotKey {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySuppressionCriticalHotKeyAllowedAttempt1(t *testing.T) {
	policy := enabledSuppressionPolicy()
	policy.AllowCriticalHotKeyRetry = true
	check := suppressionCheck()
	check.LaneClass = LaneCritical
	check.HotKeyCandidate = true
	check.Attempt = 1
	got := DecideRetrySuppression(context.Background(), policy, check)
	if got.Suppress {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySuppressionCriticalHotKeyAllowedAttempt2Suppresses(t *testing.T) {
	policy := enabledSuppressionPolicy()
	policy.AllowCriticalHotKeyRetry = true
	check := suppressionCheck()
	check.LaneClass = LaneCritical
	check.HotKeyCandidate = true
	check.Attempt = 2
	got := DecideRetrySuppression(context.Background(), policy, check)
	if !got.Suppress || got.Reason != RetrySuppressionHotKey {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySuppressionScaleOutBackgroundSuppresses(t *testing.T) {
	check := suppressionCheck()
	check.LaneClass = LaneBestEffort
	check.ScaleSignal = ScaleSignal{Recommended: true}
	got := DecideRetrySuppression(context.Background(), enabledSuppressionPolicy(), check)
	if !got.Suppress || got.Reason != RetrySuppressionScaleOutRecommended {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySuppressionHookAllow(t *testing.T) {
	policy := enabledSuppressionPolicy()
	policy.SuppressWhenOverloaded = false
	policy.SuppressOverloadFailures = false
	policy.Hook = func(context.Context, RetrySuppressionCheck) RetrySuppressionDecision {
		return RetrySuppressionDecision{Suppress: false, Reason: RetrySuppressionNone}
	}
	check := suppressionCheck()
	check.Pressure = overloadedPressure()
	check.Failure = OverloadedFailure(errors.New("overload"))
	got := DecideRetrySuppression(context.Background(), policy, check)
	if got.Suppress {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySuppressionHookReject(t *testing.T) {
	policy := enabledSuppressionPolicy()
	policy.Hook = func(context.Context, RetrySuppressionCheck) RetrySuppressionDecision {
		return RetrySuppressionDecision{Suppress: true, Reason: RetrySuppressionHookRejected}
	}
	got := DecideRetrySuppression(context.Background(), policy, suppressionCheck())
	if !got.Suppress || got.Reason != RetrySuppressionHookRejected {
		t.Fatalf("got %+v", got)
	}
}

func TestDecideRetrySuppressionHookPanicSuppresses(t *testing.T) {
	policy := enabledSuppressionPolicy()
	policy.Hook = func(context.Context, RetrySuppressionCheck) RetrySuppressionDecision {
		panic("hook")
	}
	got := DecideRetrySuppression(context.Background(), policy, suppressionCheck())
	if !got.Suppress || got.Reason != RetrySuppressionHookFailed {
		t.Fatalf("got %+v", got)
	}
}

func TestResolveRetrySuppressionPolicyPrecedence(t *testing.T) {
	queue := RetrySuppressionPolicy{Enabled: true, SuppressWhenOverloaded: false}
	override := RetrySuppressionPolicy{Enabled: true, SuppressWhenOverloaded: true}
	got := resolveRetrySuppressionPolicy(queue, &override)
	if !got.SuppressWhenOverloaded {
		t.Fatal("expected override")
	}
}

func suppressionPolicyWithHook(t *testing.T) (*atomic.Int32, RetrySuppressionPolicy) {
	t.Helper()
	var calls atomic.Int32
	return &calls, RetrySuppressionPolicy{
		Enabled: true,
		Hook: func(context.Context, RetrySuppressionCheck) RetrySuppressionDecision {
			calls.Add(1)
			return RetrySuppressionDecision{Suppress: false}
		},
	}
}

func runWithRetrySuppressionOpts(policy RetrySuppressionPolicy) runWithRetryOpts {
	return runWithRetryOpts{
		Idempotency:       Idempotency{Safety: RetrySafetySafe},
		SuppressionPolicy: policy,
	}
}

func TestRunWithRetryPermanentFailureDoesNotCallSuppressionHook(t *testing.T) {
	hookCalls, policy := suppressionPolicyWithHook(t)
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false}
	attempts := 0
	now := time.Now()
	res := runWithRetry(context.Background(), FailurePolicy{}, p, runWithRetrySuppressionOpts(policy),
		NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), func(int) (int, error) {
			attempts++
			return 0, PermanentFailure(errors.New("perm"))
		})
	if res.err == nil || attempts != 1 || hookCalls.Load() != 0 {
		t.Fatalf("err=%v attempts=%d hookCalls=%d", res.err, attempts, hookCalls.Load())
	}
}

func TestRunWithRetryDeadlineExhaustedDoesNotCallSuppressionHook(t *testing.T) {
	hookCalls, policy := suppressionPolicyWithHook(t)
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
	res := runWithRetry(ctx, FailurePolicy{}, p, runWithRetrySuppressionOpts(policy),
		budget, clock, fixedJitterSource(0.5), func(int) (int, error) {
			attempts++
			return 0, RetryableFailure(errors.New("transient"))
		})
	if res.err == nil {
		t.Fatal("expected error")
	}
	if attempts != 1 || hookCalls.Load() != 0 {
		t.Fatalf("err=%v attempts=%d hookCalls=%d", res.err, attempts, hookCalls.Load())
	}
}

func TestRunWithRetryContextCancelDoesNotCallSuppressionHook(t *testing.T) {
	hookCalls, policy := suppressionPolicyWithHook(t)
	ctx, cancel := context.WithCancel(context.Background())
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false}
	attempts := 0
	res := runWithRetry(ctx, FailurePolicy{}, p, runWithRetrySuppressionOpts(policy),
		NewDeadlineBudget(ctx, time.Now()), &testRetryClock{now: time.Now()}, fixedJitterSource(0.5), func(int) (int, error) {
			attempts++
			cancel()
			return 0, RetryableFailure(errors.New("transient"))
		})
	if res.err == nil || attempts != 1 || hookCalls.Load() != 0 {
		t.Fatalf("err=%v attempts=%d hookCalls=%d", res.err, attempts, hookCalls.Load())
	}
}

func TestRunWithRetrySuppressionRecordsReason(t *testing.T) {
	policy := enabledSuppressionPolicy()
	check := suppressionCheck()
	check.Pressure = overloadedPressure()
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false}
	now := time.Now()
	snapshot := func(string, Lane, int) RetrySuppressionSnapshot {
		return RetrySuppressionSnapshot{Pressure: overloadedPressure(), LaneClass: LaneNormal}
	}
	opts := runWithRetrySuppressionOpts(policy)
	opts.Snapshot = snapshot
	res := runWithRetry(context.Background(), FailurePolicy{}, p, opts,
		NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), func(int) (int, error) {
			return 0, RetryableFailure(errors.New("transient"))
		})
	if len(res.retryAttempts) != 1 {
		t.Fatalf("attempts = %d", len(res.retryAttempts))
	}
	if !res.retryAttempts[0].Suppressed || res.retryAttempts[0].SuppressionReason != RetrySuppressionGlobalOverload {
		t.Fatalf("attempt = %+v", res.retryAttempts[0])
	}
	trace := RetryTrace{Attempts: res.retryAttempts}
	if !trace.HadSuppression(RetrySuppressionGlobalOverload) {
		t.Fatal("expected suppression in trace")
	}
}

func TestValidateRetrySuppressionPolicy(t *testing.T) {
	if err := ValidateRetrySuppressionPolicy(RetrySuppressionPolicy{}); err != nil {
		t.Fatal(err)
	}
	err := ValidateRetrySuppressionPolicy(RetrySuppressionPolicy{
		Enabled: true, SuppressLaneAboveRatio: 1.5,
	})
	if !errors.Is(err, ErrInvalidRetrySuppressionPolicy) {
		t.Fatalf("err = %v", err)
	}
}
