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

// RetryAttempt is an internal observability record for a scheduled retry.
type RetryAttempt struct {
	Lane    Lane
	Key     string
	ShardID int
	Attempt int
	Delay   time.Duration
	Failure Failure
	Reason  RetryDecisionReason

	// IdempotencyKey is for internal events only; must not be used as a Prometheus label.
	IdempotencyKey string
	// IdempotencyScope is low-cardinality domain metadata suitable for metrics when bounded.
	IdempotencyScope string
	// IdempotencyOperation is side-effect operation name from job metadata (low cardinality when bounded).
	IdempotencyOperation string
	RetrySafety          RetrySafety
	SafetyReason         RetrySafetyDecisionReason

	Suppressed          bool
	SuppressionReason   RetrySuppressionReason
	PressureRatio       float64
	LaneDepthRatioSnap  float64
	ShardDepthRatioSnap float64
}

// HadExplicitUnsafeRetry reports whether any recorded attempt used AllowUnsafeRetry override.
func (t RetryTrace) HadExplicitUnsafeRetry() bool {
	for _, a := range t.Attempts {
		if a.SafetyReason == RetrySafetyDecisionExplicitOverride {
			return true
		}
	}
	return false
}

// HadSuppression reports whether any attempt was suppressed for the given reason.
func (t RetryTrace) HadSuppression(reason RetrySuppressionReason) bool {
	for _, a := range t.Attempts {
		if a.Suppressed && a.SuppressionReason == reason {
			return true
		}
	}
	return false
}

// LastSuppressionReason returns the suppression reason from the most recent suppressed attempt.
func (t RetryTrace) LastSuppressionReason() (RetrySuppressionReason, bool) {
	for i := len(t.Attempts) - 1; i >= 0; i-- {
		if t.Attempts[i].Suppressed {
			return t.Attempts[i].SuppressionReason, true
		}
	}
	return "", false
}

// runWithRetryOpts carries routing, idempotency, and suppression context for runWithRetry.
type runWithRetryOpts struct {
	Key               string
	Lane              Lane
	ShardID           int
	Idempotency       Idempotency
	IdempotencyPolicy IdempotencyPolicy
	SuppressionPolicy RetrySuppressionPolicy
	Snapshot          RetrySuppressionSnapshotFunc
	Observer          retryObserver
}

func observeRetry(opts runWithRetryOpts, rec retryObsRecord) {
	if opts.Observer != nil {
		opts.Observer(rec)
	}
}

func retryObsMeta(opts runWithRetryOpts, attempt int, failure Failure, budget DeadlineBudget) retryObsRecord {
	return retryObsRecord{
		Key:                  opts.Key,
		Lane:                 opts.Lane,
		ShardID:              opts.ShardID,
		Attempt:              attempt,
		Failure:              failure,
		Budget:               budget,
		IdempotencyScope:     opts.Idempotency.Scope,
		IdempotencyOperation: opts.Idempotency.Operation,
	}
}

func finalStateFromFailure(failure Failure, exhausted bool, stopped RetryDecisionReason, safety RetrySafetyDecisionReason, suppress RetrySuppressionReason) RetryFinalState {
	kind := failure.Kind
	if kind == "" {
		kind = FailureUnknown
	}
	return RetryFinalState{
		Exhausted:         exhausted,
		StoppedReason:     stopped,
		SafetyReason:      safety,
		SuppressionReason: suppress,
		FailureKind:       kind,
	}
}

func retryTerminalExhausted(reason RetryDecisionReason) bool {
	return reason == RetryDecisionMaxAttempts
}

func observeRetryTerminalStop(opts runWithRetryOpts, rec retryObsRecord, reason RetryDecisionReason) {
	rec.RetryReason = reason
	switch reason {
	case RetryDecisionMaxAttempts:
		rec.Kind = RetryEventMaxAttemptsStopped
		observeRetry(opts, rec)
		rec.Kind = RetryEventExhausted
		observeRetry(opts, rec)
	case RetryDecisionDeadlineExhausted, RetryDecisionBudgetTooSmall:
		rec.Kind = RetryEventDeadlineStopped
		observeRetry(opts, rec)
	case RetryDecisionContextCancelled:
		rec.Kind = RetryEventContextCancelled
		observeRetry(opts, rec)
	case RetryDecisionPermanentFailure, RetryDecisionDisabled:
		rec.Kind = RetryEventRetryStopped
		observeRetry(opts, rec)
	}
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
	retryAttempts []RetryAttempt
	retryFinal    RetryFinalState
	retryTracked  bool
}

// runWithRetry executes run with bounded retry. retryPolicy must have Enabled set.
// startBudget carries queue-wait and deadline state from the submit path; runtime accumulates across attempts.
func runWithRetry[T any](
	ctx context.Context,
	failurePolicy FailurePolicy,
	retryPolicy RetryPolicy,
	opts runWithRetryOpts,
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
	var retryAttempts []RetryAttempt

	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		now := clock.Now()
		budget = startBudget.refreshAt(now)

		rec := retryObsMeta(opts, attempt, Failure{}, budget)

		if err := ctx.Err(); err != nil {
			fail := ClassifyContextError(err, budget, true)
			rec = retryObsMeta(opts, attempt, fail, budget)
			rec.Kind = RetryEventFailureClassified
			observeRetry(opts, rec)
			observeRetryTerminalStop(opts, rec, RetryDecisionContextCancelled)
			final := finalStateFromFailure(fail, false, RetryDecisionContextCancelled, "", "")
			return runWithRetryResult[T]{
				err: err, budget: budget, beforeHandler: true,
				retryAttempts: retryAttempts, retryFinal: final, retryTracked: true,
			}
		}

		rec.Kind = RetryEventAttemptStarted
		observeRetry(opts, rec)

		attemptStart := now
		val, err := run(attempt)
		attemptRuntime := clock.Now().Sub(attemptStart)
		totalRuntime := budget.Runtime + attemptRuntime
		budget = budget.WithRuntimeAt(totalRuntime, clock.Now())

		if err == nil {
			rec := retryObsMeta(opts, attempt, Failure{}, budget)
			rec.Kind = RetryEventSucceeded
			observeRetry(opts, rec)
			final := RetryFinalState{Succeeded: true}
			return runWithRetryResult[T]{
				val: val, budget: budget, beforeHandler: false,
				retryAttempts: retryAttempts, retryFinal: final, retryTracked: true,
			}
		}

		lastErr = err
		lastFailure = classifyFailureWithPolicy(err, failurePolicy)
		if lastFailure.Kind == FailureNone {
			return runWithRetryResult[T]{
				val: val, err: err, budget: budget, beforeHandler: false,
				retryAttempts: retryAttempts, retryTracked: true,
			}
		}

		rec = retryObsMeta(opts, attempt, lastFailure, budget)
		rec.Kind = RetryEventFailureClassified
		observeRetry(opts, rec)

		decision := DecideRetry(policy, RetryState{
			Ctx:     ctx,
			Attempt: attempt,
			Failure: lastFailure,
			Budget:  budget,
			Now:     now,
		}, jitter)
		if !decision.Retry {
			observeRetryTerminalStop(opts, rec, decision.Reason)
			final := finalStateFromFailure(lastFailure, retryTerminalExhausted(decision.Reason), decision.Reason, "", "")
			return runWithRetryResult[T]{
				err: retryHandlerError(lastErr, lastFailure), budget: budget, beforeHandler: false,
				retryAttempts: retryAttempts, retryFinal: final, retryTracked: true,
			}
		}

		safety := DecideRetrySafety(ctx, RetrySafetyCheck{
			Key:         opts.Key,
			Lane:        opts.Lane,
			ShardID:     opts.ShardID,
			Attempt:     attempt,
			Failure:     lastFailure,
			Retry:       policy,
			Idempotency: opts.Idempotency,
			Budget:      budget,
		}, opts.IdempotencyPolicy)
		retryAttempts = append(retryAttempts, RetryAttempt{
			Lane:                 opts.Lane,
			Key:                  opts.Key,
			ShardID:              opts.ShardID,
			Attempt:              attempt,
			Delay:                decision.Delay,
			Failure:              lastFailure,
			Reason:               decision.Reason,
			IdempotencyKey:       opts.Idempotency.Key,
			IdempotencyScope:     opts.Idempotency.Scope,
			IdempotencyOperation: opts.Idempotency.Operation,
			RetrySafety:          opts.Idempotency.Safety,
			SafetyReason:         safety.Reason,
		})
		if !safety.Allow {
			rec.Kind = RetryEventSafetySuppressed
			rec.RetryReason = decision.Reason
			rec.SafetyReason = safety.Reason
			observeRetry(opts, rec)
			final := finalStateFromFailure(lastFailure, false, decision.Reason, safety.Reason, "")
			return runWithRetryResult[T]{
				err: retryHandlerError(lastErr, lastFailure), budget: budget, beforeHandler: false,
				retryAttempts: retryAttempts, retryFinal: final, retryTracked: true,
			}
		}

		var snap RetrySuppressionSnapshot
		if opts.Snapshot != nil {
			snap = opts.Snapshot(opts.Key, opts.Lane, opts.ShardID)
		}
		suppress := DecideRetrySuppression(ctx, opts.SuppressionPolicy, RetrySuppressionCheck{
			Key:             opts.Key,
			Lane:            opts.Lane,
			ShardID:         opts.ShardID,
			Attempt:         attempt,
			Failure:         lastFailure,
			Retry:           policy,
			Budget:          budget,
			Pressure:        snap.Pressure,
			LaneDepthRatio:  snap.LaneDepthRatio,
			ShardDepthRatio: snap.ShardDepthRatio,
			ScaleSignal:     snap.ScaleSignal,
			Idempotency:     opts.Idempotency,
			LaneClass:       snap.LaneClass,
			HotKeyCandidate: snap.HotKeyCandidate,
		})
		if len(retryAttempts) > 0 {
			last := &retryAttempts[len(retryAttempts)-1]
			last.Suppressed = suppress.Suppress
			last.SuppressionReason = suppress.Reason
			last.PressureRatio = snap.Pressure.TotalDepthRatio
			last.LaneDepthRatioSnap = snap.LaneDepthRatio
			last.ShardDepthRatioSnap = snap.ShardDepthRatio
		}
		if suppress.Suppress {
			rec.Kind = RetryEventSuppressed
			rec.RetryReason = decision.Reason
			rec.SafetyReason = safety.Reason
			rec.SuppressionReason = suppress.Reason
			rec.Pressure = snap.Pressure
			rec.Delay = decision.Delay
			observeRetry(opts, rec)
			final := finalStateFromFailure(lastFailure, false, decision.Reason, safety.Reason, suppress.Reason)
			return runWithRetryResult[T]{
				err: retryHandlerError(lastErr, lastFailure), budget: budget, beforeHandler: false,
				retryAttempts: retryAttempts, retryFinal: final, retryTracked: true,
			}
		}

		rec.Kind = RetryEventScheduled
		rec.RetryReason = decision.Reason
		rec.SafetyReason = safety.Reason
		rec.Delay = decision.Delay
		rec.Pressure = snap.Pressure
		observeRetry(opts, rec)

		if sleepErr := clock.Sleep(ctx, decision.Delay); sleepErr != nil {
			fail := ClassifyContextError(sleepErr, budget, true)
			rec = retryObsMeta(opts, attempt, fail, budget)
			rec.Kind = RetryEventFailureClassified
			observeRetry(opts, rec)
			observeRetryTerminalStop(opts, rec, RetryDecisionContextCancelled)
			final := finalStateFromFailure(fail, false, RetryDecisionContextCancelled, safety.Reason, "")
			return runWithRetryResult[T]{
				err: sleepErr, budget: budget, beforeHandler: true,
				retryAttempts: retryAttempts, retryFinal: final, retryTracked: true,
			}
		}
	}

	return runWithRetryResult[T]{
		val: zero, err: lastErr, beforeHandler: false,
		retryAttempts: retryAttempts, retryTracked: true,
	}
}

// runSubmitRequestHandlerWithRetry runs a typed request handler with bounded retry.
func runSubmitRequestHandlerWithRetry[I, O any](
	reqCtx context.Context,
	policy FailurePolicy,
	retryPolicy RetryPolicy,
	opts runWithRetryOpts,
	budget DeadlineBudget,
	handle func(context.Context, I) (O, error),
	input I,
) runWithRetryResult[O] {
	return runWithRetry(reqCtx, policy, retryPolicy, opts, budget, nil, nil, func(int) (O, error) {
		return handle(reqCtx, input)
	})
}

// runValueJobWithRetry runs a ValueJob handler with bounded retry.
func runValueJobWithRetry[T any](
	ctx context.Context,
	policy FailurePolicy,
	retryPolicy RetryPolicy,
	opts runWithRetryOpts,
	budget DeadlineBudget,
	run func(context.Context) (T, error),
) runWithRetryResult[T] {
	return runWithRetry(ctx, policy, retryPolicy, opts, budget, nil, nil, func(int) (T, error) {
		return run(ctx)
	})
}

func buildRunWithRetryOpts(
	q *Queue,
	key string,
	lane Lane,
	shardID int,
	idempotency Idempotency,
	suppressionOverride *RetrySuppressionPolicy,
) runWithRetryOpts {
	suppressionPolicy := resolveRetrySuppressionPolicy(q.retrySuppression, suppressionOverride)
	opts := runWithRetryOpts{
		Key: key, Lane: lane, ShardID: shardID,
		Idempotency: idempotency, IdempotencyPolicy: q.idempotencyPolicy,
		SuppressionPolicy: suppressionPolicy,
	}
	if suppressionPolicy.Enabled {
		opts.Snapshot = q.RetrySuppressionSnapshot
	}
	opts.Observer = q.retryObserver()
	return opts
}

func failureAsError(f Failure) error {
	if f.Kind == FailureNone {
		return nil
	}
	return f
}

// retryHandlerError returns the handler error for retry termination.
// Preserves *StageFailure from pipeline stages; otherwise returns classified Failure as error.
func retryHandlerError(lastErr error, lastFailure Failure) error {
	if lastErr != nil {
		if _, ok := AsStageFailure(lastErr); ok {
			return lastErr
		}
	}
	return failureAsError(lastFailure)
}
