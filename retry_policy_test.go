// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fixedJitterSource float64

func (f fixedJitterSource) Float64() float64 { return float64(f) }

type testRetryClock struct {
	now   time.Time
	slept []time.Duration
}

func (c *testRetryClock) Now() time.Time { return c.now }

func (c *testRetryClock) Sleep(ctx context.Context, d time.Duration) error {
	c.slept = append(c.slept, d)
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		if !t.Stop() {
			<-t.C
		}
		return ctx.Err()
	}
}

// deadlineOnSleepClock simulates deadline expiry during backoff sleep.
type deadlineOnSleepClock struct {
	testRetryClock
}

func (deadlineOnSleepClock) Sleep(context.Context, time.Duration) error {
	return context.DeadlineExceeded
}

func TestRetryPolicyZeroValueDisabled(t *testing.T) {
	if (RetryPolicy{}).Enabled {
		t.Fatal("zero policy should be disabled")
	}
	if err := ValidateRetryPolicy(RetryPolicy{}); err != nil {
		t.Fatal(err)
	}
}

func TestNormalizeRetryPolicyDefaults(t *testing.T) {
	p := RetryPolicy{Enabled: true}
	NormalizeRetryPolicy(&p)
	if p.MaxAttempts != 3 {
		t.Fatalf("MaxAttempts = %d", p.MaxAttempts)
	}
	if p.InitialBackoff != 10*time.Millisecond {
		t.Fatalf("InitialBackoff = %v", p.InitialBackoff)
	}
	if p.MaxBackoff != 250*time.Millisecond {
		t.Fatalf("MaxBackoff = %v", p.MaxBackoff)
	}
	if p.Multiplier != 2.0 {
		t.Fatalf("Multiplier = %v", p.Multiplier)
	}
	if !p.Jitter {
		t.Fatal("expected jitter enabled")
	}
	if p.MinRemainingBudget != p.InitialBackoff {
		t.Fatalf("MinRemainingBudget = %v", p.MinRemainingBudget)
	}
}

func TestValidateRetryPolicyErrors(t *testing.T) {
	tests := []struct {
		name string
		p    RetryPolicy
	}{
		{"max attempts", RetryPolicy{Enabled: true, MaxAttempts: 0, InitialBackoff: time.Millisecond}},
		{"negative backoff", RetryPolicy{Enabled: true, MaxAttempts: 1, InitialBackoff: -1}},
		{"max backoff", RetryPolicy{Enabled: true, MaxAttempts: 1, InitialBackoff: 100 * time.Millisecond, MaxBackoff: 10 * time.Millisecond}},
		{"multiplier", RetryPolicy{Enabled: true, MaxAttempts: 1, InitialBackoff: time.Millisecond, Multiplier: 0.5}},
		{"jitter fraction", RetryPolicy{Enabled: true, MaxAttempts: 1, InitialBackoff: time.Millisecond, JitterFraction: 1.5}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateRetryPolicy(tt.p); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestDecideRetryRetryableFailure(t *testing.T) {
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: 10 * time.Millisecond, MinRemainingBudget: 1 * time.Millisecond}
	f := RetryableFailure(errors.New("transient"))
	d := DecideRetry(p, RetryState{
		Ctx:     context.Background(),
		Attempt: 1,
		Failure: f,
		Budget:  DeadlineBudget{},
		Now:     time.Now(),
	}, fixedJitterSource(0.5))
	if !d.Retry || d.Reason != RetryDecisionRetryableFailure {
		t.Fatalf("decision = %+v", d)
	}
}

func TestDecideRetryDeadlineExhaustedNotRetryable(t *testing.T) {
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond}
	d := DecideRetry(p, RetryState{
		Attempt: 1,
		Failure: DeadlineExhaustedFailure(errors.New("exhausted")),
		Budget:  DeadlineBudget{},
	}, nil)
	if d.Retry || d.Reason != RetryDecisionPermanentFailure {
		t.Fatalf("decision = %+v", d)
	}
}

func TestDecideRetryPermanentFailure(t *testing.T) {
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: 10 * time.Millisecond}
	d := DecideRetry(p, RetryState{
		Attempt: 1,
		Failure: PermanentFailure(errors.New("nope")),
		Budget:  DeadlineBudget{},
	}, nil)
	if d.Retry || d.Reason != RetryDecisionPermanentFailure {
		t.Fatalf("decision = %+v", d)
	}
}

func TestDecideRetryUnknownNotRetryable(t *testing.T) {
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond}
	d := DecideRetry(p, RetryState{
		Attempt: 1,
		Failure: UnknownFailure(errors.New("x")),
		Budget:  DeadlineBudget{},
	}, nil)
	if d.Retry {
		t.Fatalf("decision = %+v", d)
	}
}

func TestDecideRetryMaxAttempts(t *testing.T) {
	p := RetryPolicy{Enabled: true, MaxAttempts: 2, InitialBackoff: time.Millisecond}
	d := DecideRetry(p, RetryState{
		Attempt: 2,
		Failure: RetryableFailure(errors.New("x")),
		Budget:  DeadlineBudget{},
	}, nil)
	if d.Retry || d.Reason != RetryDecisionMaxAttempts {
		t.Fatalf("decision = %+v", d)
	}
}

func TestDecideRetryContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond}
	d := DecideRetry(p, RetryState{
		Ctx:     ctx,
		Attempt: 1,
		Failure: RetryableFailure(errors.New("x")),
		Budget:  DeadlineBudget{},
	}, nil)
	if d.Retry || d.Reason != RetryDecisionContextCancelled {
		t.Fatalf("decision = %+v", d)
	}
}

func TestDecideRetryInsufficientBudget(t *testing.T) {
	now := time.Now()
	deadline := now.Add(12 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	budget := NewDeadlineBudget(ctx, now)
	p := RetryPolicy{
		Enabled:            true,
		MaxAttempts:        3,
		InitialBackoff:     10 * time.Millisecond,
		MinRemainingBudget: 5 * time.Millisecond,
		Jitter:             false,
	}
	d := DecideRetry(p, RetryState{
		Ctx:     ctx,
		Attempt: 1,
		Failure: RetryableFailure(errors.New("x")),
		Budget:  budget,
		Now:     now,
	}, fixedJitterSource(0.5))
	if d.Retry {
		t.Fatalf("expected no retry, got %+v", d)
	}
	if d.Reason != RetryDecisionBudgetTooSmall && d.Reason != RetryDecisionDeadlineExhausted {
		t.Fatalf("reason = %q", d.Reason)
	}
}

func TestDecideRetryRetryableKindsOverride(t *testing.T) {
	p := RetryPolicy{
		Enabled:        true,
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		RetryableKinds: []FailureKind{FailureRejected},
	}
	d := DecideRetry(p, RetryState{
		Attempt: 1,
		Failure: RejectedFailure(errors.New("rej")),
		Budget:  DeadlineBudget{},
	}, nil)
	if !d.Retry {
		t.Fatalf("decision = %+v", d)
	}
}

func TestRetryDelayGrowthAndCap(t *testing.T) {
	p := RetryPolicy{
		Enabled:        true,
		MaxAttempts:    5,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     25 * time.Millisecond,
		Multiplier:     2.0,
		Jitter:         false,
	}
	if d := RetryDelay(p, 1, nil); d != 10*time.Millisecond {
		t.Fatalf("attempt 1 = %v", d)
	}
	if d := RetryDelay(p, 2, nil); d != 20*time.Millisecond {
		t.Fatalf("attempt 2 = %v", d)
	}
	if d := RetryDelay(p, 3, nil); d != 25*time.Millisecond {
		t.Fatalf("attempt 3 cap = %v", d)
	}
}

func TestRetryDelayJitterNeverNegative(t *testing.T) {
	p := RetryPolicy{
		Enabled:        true,
		InitialBackoff: 1,
		Jitter:         true,
		JitterFraction: 1.0,
	}
	d := RetryDelay(p, 1, fixedJitterSource(0))
	if d < 0 {
		t.Fatalf("delay = %v, want non-negative", d)
	}
	if d != 0 {
		t.Fatalf("delay = %v, want 0", d)
	}
}

func TestNormalizeRetryPolicyPreservesJitterFalse(t *testing.T) {
	p := RetryPolicy{
		Enabled:        true,
		MaxAttempts:    3,
		InitialBackoff: 10 * time.Millisecond,
		Jitter:         false,
		JitterFraction: 0.2,
	}
	NormalizeRetryPolicy(&p)
	if p.Jitter {
		t.Fatal("expected Jitter to remain false when explicitly set")
	}
}

func TestRetryDelayJitterRange(t *testing.T) {
	p := RetryPolicy{
		Enabled:        true,
		MaxAttempts:    3,
		InitialBackoff: 100 * time.Millisecond,
		Jitter:         true,
		JitterFraction: 0.2,
	}
	low := RetryDelay(p, 1, fixedJitterSource(0))
	high := RetryDelay(p, 1, fixedJitterSource(1))
	if low != 80*time.Millisecond {
		t.Fatalf("low = %v", low)
	}
	if high != 120*time.Millisecond {
		t.Fatalf("high = %v", high)
	}
}

func TestResolveRetryPolicyPrecedence(t *testing.T) {
	queue := RetryPolicy{Enabled: true, MaxAttempts: 5}
	override := RetryPolicy{Enabled: true, MaxAttempts: 2}
	got := resolveRetryPolicy(queue, override)
	if got.MaxAttempts != 2 {
		t.Fatalf("got %d", got.MaxAttempts)
	}
	disabled := resolveRetryPolicy(queue, RetryPolicy{})
	if disabled.MaxAttempts != 5 {
		t.Fatalf("got %d", disabled.MaxAttempts)
	}
}

func TestRunWithRetrySucceedsOnSecondAttempt(t *testing.T) {
	clock := &testRetryClock{now: time.Now()}
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false}
	attempts := 0
	res := runWithRetry(context.Background(), FailurePolicy{}, p, runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetySafe}}, NewDeadlineBudget(context.Background(), clock.Now()), clock, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
		attempts++
		if attempts < 2 {
			return 0, RetryableFailure(errors.New("transient"))
		}
		return 42, nil
	})
	if res.err != nil || res.val != 42 {
		t.Fatalf("res = %+v attempts=%d", res, attempts)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d", attempts)
	}
	if len(clock.slept) != 1 {
		t.Fatalf("slept = %v", clock.slept)
	}
}

func TestRunWithRetryCarriesRuntimeAcrossAttempts(t *testing.T) {
	clock := &testRetryClock{now: time.Now()}
	p := RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false}
	var runtimeAtStart []time.Duration
	attempts := 0
	_ = runWithRetry(context.Background(), FailurePolicy{}, p, runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetySafe}}, NewDeadlineBudget(context.Background(), clock.Now()), clock, fixedJitterSource(0.5), func(_ int, b DeadlineBudget) (int, error) {
		runtimeAtStart = append(runtimeAtStart, b.Runtime)
		attempts++
		if attempts < 2 {
			clock.now = clock.now.Add(12 * time.Millisecond)
			return 0, RetryableFailure(errors.New("transient"))
		}
		return 1, nil
	})
	if attempts != 2 {
		t.Fatalf("attempts = %d", attempts)
	}
	if len(runtimeAtStart) != 2 {
		t.Fatalf("runtime snapshots = %v", runtimeAtStart)
	}
	if runtimeAtStart[1] < 10*time.Millisecond {
		t.Fatalf("attempt 2 runtime_so_far = %v, want >= 10ms from attempt 1", runtimeAtStart[1])
	}
}

func TestRunWithRetryDeadlineExhaustedDuringSleep(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	p := RetryPolicy{
		Enabled: true, MaxAttempts: 3, InitialBackoff: 10 * time.Millisecond,
		Jitter: false, MinRemainingBudget: time.Nanosecond,
	}
	budget := NewDeadlineBudget(ctx, now)
	clock := &deadlineOnSleepClock{testRetryClock: testRetryClock{now: now}}
	res := runWithRetry(ctx, FailurePolicy{}, p, runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetySafe}}, budget, clock, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
		return 0, RetryableFailure(errors.New("transient"))
	})
	if !errors.Is(res.err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", res.err)
	}
	if !res.beforeHandler {
		t.Fatal("expected beforeHandler=true for inter-attempt deadline")
	}
}

func TestRunWithRetryStopsAtMaxAttempts(t *testing.T) {
	p := RetryPolicy{Enabled: true, MaxAttempts: 2, InitialBackoff: time.Millisecond, Jitter: false}
	attempts := 0
	now := time.Now()
	res := runWithRetry(context.Background(), FailurePolicy{}, p, runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetySafe}}, NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), func(int, DeadlineBudget) (int, error) {
		attempts++
		return 0, RetryableFailure(errors.New("transient"))
	})
	if res.err == nil {
		t.Fatal("expected error")
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d", attempts)
	}
}
