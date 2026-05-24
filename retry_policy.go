// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"
)

// ErrInvalidRetryPolicy indicates retry policy validation failed.
var ErrInvalidRetryPolicy = errors.New("keylane: invalid retry policy")

// RetryPolicy configures bounded in-worker retry for SubmitValue and SubmitRequest.
// Zero value disables retry.
type RetryPolicy struct {
	Enabled bool

	// MaxAttempts includes the first attempt (3 means 1 initial + up to 2 retries).
	MaxAttempts int

	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64

	Jitter         bool
	JitterFraction float64

	// MinRemainingBudget is the minimum caller deadline budget required before another attempt.
	MinRemainingBudget time.Duration

	// RetryableKinds optionally narrows which FailureKind values are retryable.
	// Empty means use default retryability rules (FailureRetryable and Failure.Retryable).
	RetryableKinds []FailureKind
}

// RetryDecisionReason explains why a retry was or was not scheduled.
type RetryDecisionReason string

const (
	RetryDecisionNone              RetryDecisionReason = "none"
	RetryDecisionDisabled          RetryDecisionReason = "disabled"
	RetryDecisionRetryableFailure  RetryDecisionReason = "retryable_failure"
	RetryDecisionPermanentFailure  RetryDecisionReason = "permanent_failure"
	RetryDecisionMaxAttempts       RetryDecisionReason = "max_attempts"
	RetryDecisionContextCancelled  RetryDecisionReason = "context_cancelled"
	RetryDecisionDeadlineExhausted RetryDecisionReason = "deadline_exhausted"
	RetryDecisionBudgetTooSmall    RetryDecisionReason = "budget_too_small"
)

// RetryDecision is the outcome of DecideRetry.
type RetryDecision struct {
	Retry   bool
	Attempt int
	Delay   time.Duration
	Reason  RetryDecisionReason
	Failure Failure
	Budget  DeadlineBudget
}

// RetryState carries inputs for DecideRetry.
type RetryState struct {
	Ctx     context.Context
	Attempt int
	Failure Failure
	Budget  DeadlineBudget
	Now     time.Time
}

// RetryAttempt is an internal observability record for a scheduled retry (KL-1605 seam).
type RetryAttempt struct {
	Lane    Lane
	Key     string
	ShardID int
	Attempt int
	Delay   time.Duration
	Failure Failure
	Reason  RetryDecisionReason
}

type retryClock interface {
	Now() time.Time
	Sleep(context.Context, time.Duration) error
}

type retryJitterSource interface {
	Float64() float64
}

type realRetryClock struct{}

func (realRetryClock) Now() time.Time { return time.Now() }

func (realRetryClock) Sleep(ctx context.Context, d time.Duration) error {
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

type randJitterSource struct{}

func (randJitterSource) Float64() float64 { return rand.Float64() }

var (
	defaultRetryClock  retryClock        = realRetryClock{}
	defaultRetryJitter retryJitterSource = randJitterSource{}
)

// NormalizeRetryPolicy applies conservative defaults when retry is enabled.
func NormalizeRetryPolicy(cfg *RetryPolicy) {
	if cfg == nil || !cfg.Enabled {
		return
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = 10 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 250 * time.Millisecond
	}
	if cfg.Multiplier <= 0 {
		cfg.Multiplier = 2.0
	}
	if cfg.JitterFraction <= 0 {
		cfg.JitterFraction = 0.2
		cfg.Jitter = true
	}
	if cfg.MinRemainingBudget <= 0 {
		cfg.MinRemainingBudget = cfg.InitialBackoff
	}
}

// ValidateRetryPolicy validates retry settings after normalization.
func ValidateRetryPolicy(cfg RetryPolicy) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.MaxAttempts < 1 {
		return fmt.Errorf("%w: MaxAttempts must be at least 1 when retry is enabled", ErrInvalidRetryPolicy)
	}
	if cfg.InitialBackoff < 0 {
		return fmt.Errorf("%w: InitialBackoff must be non-negative", ErrInvalidRetryPolicy)
	}
	if cfg.MaxBackoff > 0 && cfg.MaxBackoff < cfg.InitialBackoff {
		return fmt.Errorf("%w: MaxBackoff must be >= InitialBackoff", ErrInvalidRetryPolicy)
	}
	if cfg.Multiplier > 0 && cfg.Multiplier < 1.0 {
		return fmt.Errorf("%w: Multiplier must be at least 1.0", ErrInvalidRetryPolicy)
	}
	if cfg.JitterFraction < 0 || cfg.JitterFraction > 1 {
		return fmt.Errorf("%w: JitterFraction must be between 0 and 1", ErrInvalidRetryPolicy)
	}
	if cfg.MinRemainingBudget < 0 {
		return fmt.Errorf("%w: MinRemainingBudget must be non-negative", ErrInvalidRetryPolicy)
	}
	return nil
}

func resolveRetryPolicy(queue, override RetryPolicy) RetryPolicy {
	if override.Enabled {
		p := override
		NormalizeRetryPolicy(&p)
		return p
	}
	if queue.Enabled {
		p := queue
		NormalizeRetryPolicy(&p)
		return p
	}
	return RetryPolicy{}
}

// DecideRetry determines whether another attempt should run after a classified failure.
func DecideRetry(policy RetryPolicy, state RetryState, jitter retryJitterSource) RetryDecision {
	decision := RetryDecision{
		Attempt: state.Attempt,
		Failure: state.Failure,
		Budget:  state.Budget,
		Reason:  RetryDecisionNone,
	}
	if !policy.Enabled {
		decision.Reason = RetryDecisionDisabled
		return decision
	}
	if state.Ctx != nil {
		if err := state.Ctx.Err(); err != nil {
			decision.Reason = RetryDecisionContextCancelled
			decision.Failure = ClassifyContextError(err, state.Budget, false)
			return decision
		}
	}
	if state.Attempt >= policy.MaxAttempts {
		decision.Reason = RetryDecisionMaxAttempts
		return decision
	}
	if !isRetryableFailure(policy, state.Failure) {
		decision.Reason = RetryDecisionPermanentFailure
		return decision
	}
	now := state.Now
	if now.IsZero() {
		now = time.Now()
	}
	if state.Budget.HasDeadline && state.Budget.IsExhaustedAt(now) {
		decision.Reason = RetryDecisionDeadlineExhausted
		return decision
	}
	if jitter == nil {
		jitter = defaultRetryJitter
	}
	delay := RetryDelay(policy, state.Attempt, jitter)
	decision.Delay = delay
	if state.Budget.HasDeadline {
		remaining := state.Budget.RemainingAt(now)
		need := delay + policy.MinRemainingBudget
		if remaining < need {
			if remaining == 0 {
				decision.Reason = RetryDecisionDeadlineExhausted
			} else {
				decision.Reason = RetryDecisionBudgetTooSmall
			}
			return decision
		}
	}
	decision.Retry = true
	decision.Reason = RetryDecisionRetryableFailure
	return decision
}

func isRetryableFailure(policy RetryPolicy, failure Failure) bool {
	if failure.Kind == FailureNone {
		return false
	}
	if len(policy.RetryableKinds) > 0 {
		for _, k := range policy.RetryableKinds {
			if failure.Kind == k {
				return true
			}
		}
		return false
	}
	switch failure.Kind {
	case FailurePermanent, FailureTimeout, FailureCancelled, FailureOverloaded,
		FailureRejected, FailureDeadlineExhausted, FailurePanic, FailureUnknown:
		return false
	case FailureRetryable:
		return true
	default:
		return failure.Retryable
	}
}

// RetryDelay returns backoff before the next attempt after attempt N failed (1-based).
func RetryDelay(policy RetryPolicy, attempt int, jitter retryJitterSource) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	p := policy
	if p.InitialBackoff <= 0 {
		p.InitialBackoff = 10 * time.Millisecond
	}
	if p.MaxBackoff <= 0 {
		p.MaxBackoff = 250 * time.Millisecond
	}
	if p.Multiplier <= 0 {
		p.Multiplier = 2.0
	}

	base := float64(p.InitialBackoff)
	if p.Multiplier > 0 && attempt > 1 {
		base *= math.Pow(p.Multiplier, float64(attempt-1))
	}
	if p.MaxBackoff > 0 && time.Duration(base) > p.MaxBackoff {
		base = float64(p.MaxBackoff)
	}
	delay := time.Duration(base)
	if !p.Jitter || p.JitterFraction <= 0 {
		if delay < 0 {
			return 0
		}
		return delay
	}
	if jitter == nil {
		jitter = defaultRetryJitter
	}
	frac := p.JitterFraction
	spread := float64(delay) * frac
	offset := (jitter.Float64()*2 - 1) * spread
	delay = time.Duration(float64(delay) + offset)
	if delay < 0 {
		return 0
	}
	return delay
}

type runWithRetryResult[T any] struct {
	val           T
	err           error
	budget        DeadlineBudget
	beforeHandler bool
}

// runWithRetry executes run with bounded retry. retryPolicy must have Enabled set.
// startBudget carries queue-wait and deadline state from the submit path; runtime accumulates across attempts.
func runWithRetry[T any](
	ctx context.Context,
	failurePolicy FailurePolicy,
	retryPolicy RetryPolicy,
	startBudget DeadlineBudget,
	clock retryClock,
	jitter retryJitterSource,
	run func(attempt int) (T, error),
) runWithRetryResult[T] {
	var zero T
	if clock == nil {
		clock = defaultRetryClock
	}
	policy := retryPolicy
	NormalizeRetryPolicy(&policy)

	var lastFailure Failure
	var lastErr error
	budget := startBudget

	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		now := clock.Now()
		budget = startBudget.refreshAt(now)

		if err := ctx.Err(); err != nil {
			return runWithRetryResult[T]{
				err:           err,
				budget:        budget,
				beforeHandler: true,
			}
		}

		attemptStart := now
		val, err := run(attempt)
		attemptRuntime := clock.Now().Sub(attemptStart)
		totalRuntime := budget.Runtime + attemptRuntime
		budget = budget.WithRuntimeAt(totalRuntime, clock.Now())

		if err == nil {
			return runWithRetryResult[T]{val: val, budget: budget, beforeHandler: false}
		}

		lastErr = err
		lastFailure = classifyFailureWithPolicy(err, failurePolicy)
		if lastFailure.Kind == FailureNone {
			return runWithRetryResult[T]{val: val, err: err, budget: budget, beforeHandler: false}
		}

		decision := DecideRetry(policy, RetryState{
			Ctx:     ctx,
			Attempt: attempt,
			Failure: lastFailure,
			Budget:  budget,
			Now:     now,
		}, jitter)
		if !decision.Retry {
			return runWithRetryResult[T]{
				err:           failureAsError(lastFailure),
				budget:        budget,
				beforeHandler: false,
			}
		}

		if sleepErr := clock.Sleep(ctx, decision.Delay); sleepErr != nil {
			return runWithRetryResult[T]{
				err:           sleepErr,
				budget:        budget,
				beforeHandler: true,
			}
		}
	}

	if lastErr != nil {
		return runWithRetryResult[T]{
			err:           failureAsError(lastFailure),
			budget:        budget.refreshAt(clock.Now()),
			beforeHandler: false,
		}
	}
	return runWithRetryResult[T]{val: zero, err: lastErr, beforeHandler: false}
}

func failureAsError(f Failure) error {
	if f.Kind == FailureNone {
		return nil
	}
	return f
}
