// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
)

// ErrRetryUnsafe indicates retry was suppressed because the job is not duplicate-safe.
// The original handler failure is preserved on the Future; this sentinel is for documentation and tests.
var ErrRetryUnsafe = errors.New("keylane: retry suppressed because job is not duplicate-safe")

// RetrySafety declares whether an operation may be retried safely.
type RetrySafety string

const (
	// RetrySafetyUnspecified is the zero value and is treated as unsafe when retry is enabled.
	RetrySafetyUnspecified RetrySafety = ""

	// RetrySafetySafe means the operation is safe to retry (read-only, idempotent mutation, etc.).
	RetrySafetySafe RetrySafety = "safe"

	// RetrySafetyUnsafe means automatic retry must not run.
	RetrySafetyUnsafe RetrySafety = "unsafe"

	// RetrySafetyRequiresCheck means retry may run only when IdempotencyPolicy.Hook allows it.
	RetrySafetyRequiresCheck RetrySafety = "requires_check"
)

// Idempotency carries caller-provided duplicate-safety metadata for retry decisions.
type Idempotency struct {
	// Key is a stable caller-provided idempotency key across retries of the same logical operation.
	Key string

	// Safety declares whether this job is safe to retry.
	Safety RetrySafety

	// Scope optionally identifies the domain boundary (e.g. "payment", "order").
	Scope string

	// Operation optionally identifies the side-effect operation (e.g. "charge", "send-webhook").
	Operation string

	// AllowUnsafeRetry is an explicit escape hatch; dangerous and should be rare.
	// When true on RetrySafetyUnsafe jobs, it overrides RequireForRetry missing-key suppression.
	AllowUnsafeRetry bool
}

// RetrySafetyDecisionReason explains why a retry was allowed or suppressed for duplicate safety.
type RetrySafetyDecisionReason string

const (
	RetrySafetyDecisionSafe             RetrySafetyDecisionReason = "safe"
	RetrySafetyDecisionUnsafe           RetrySafetyDecisionReason = "unsafe"
	RetrySafetyDecisionMissingKey       RetrySafetyDecisionReason = "missing_idempotency_key"
	RetrySafetyDecisionHookAllowed      RetrySafetyDecisionReason = "hook_allowed"
	RetrySafetyDecisionHookRejected     RetrySafetyDecisionReason = "hook_rejected"
	RetrySafetyDecisionHookFailed       RetrySafetyDecisionReason = "hook_failed"
	RetrySafetyDecisionNoHook           RetrySafetyDecisionReason = "no_hook"
	RetrySafetyDecisionExplicitOverride RetrySafetyDecisionReason = "explicit_override"
)

// RetrySafetyDecision is the outcome of DecideRetrySafety.
type RetrySafetyDecision struct {
	Allow   bool
	Reason  RetrySafetyDecisionReason
	Message string
}

// RetrySafetyCheck carries inputs for duplicate-safety evaluation before a retry sleep.
type RetrySafetyCheck struct {
	Key         string
	Lane        Lane
	ShardID     int
	Attempt     int
	Failure     Failure
	Retry       RetryPolicy
	Idempotency Idempotency
	Budget      DeadlineBudget
}

// RetrySafetyHook allows the application to approve or reject a retry for requires_check jobs.
type RetrySafetyHook func(context.Context, RetrySafetyCheck) RetrySafetyDecision

// IdempotencyPolicy configures duplicate-safety checks for bounded retry.
type IdempotencyPolicy struct {
	// RequireForRetry suppresses retry when Idempotency.Key is empty for any job safety value.
	// Jobs with RetrySafetyRequiresCheck and a key but no Hook still suppress with reason no_hook.
	RequireForRetry bool

	// Hook is invoked for RetrySafetyRequiresCheck when set.
	Hook RetrySafetyHook
}

// NormalizeIdempotencyPolicy reserves defaults for future policy fields; intentionally empty in KL-1603.
func NormalizeIdempotencyPolicy(cfg *IdempotencyPolicy) {
	if cfg == nil {
		return
	}
}

// ValidateIdempotencyPolicy validates idempotency policy settings.
// RequireForRetry without Hook is valid (missing-key enforcement only); requires_check with a key but no Hook suppresses at runtime with no_hook.
func ValidateIdempotencyPolicy(cfg IdempotencyPolicy) error {
	_ = cfg
	return nil
}

// DecideRetrySafety determines whether a retry should proceed after DecideRetry allows it.
func DecideRetrySafety(ctx context.Context, check RetrySafetyCheck, policy IdempotencyPolicy) RetrySafetyDecision {
	if !check.Retry.Enabled {
		return RetrySafetyDecision{Allow: true, Reason: RetrySafetyDecisionSafe}
	}

	idem := check.Idempotency

	// AllowUnsafeRetry overrides RequireForRetry for explicitly marked unsafe jobs.
	if idem.Safety == RetrySafetyUnsafe && idem.AllowUnsafeRetry {
		return RetrySafetyDecision{
			Allow:   true,
			Reason:  RetrySafetyDecisionExplicitOverride,
			Message: "explicit unsafe retry override",
		}
	}

	if policy.RequireForRetry && idem.Key == "" {
		return RetrySafetyDecision{Allow: false, Reason: RetrySafetyDecisionMissingKey}
	}

	switch idem.Safety {
	case RetrySafetySafe:
		return RetrySafetyDecision{Allow: true, Reason: RetrySafetyDecisionSafe}
	case RetrySafetyUnsafe:
		return RetrySafetyDecision{Allow: false, Reason: RetrySafetyDecisionUnsafe}
	case RetrySafetyRequiresCheck:
		if policy.Hook == nil {
			return RetrySafetyDecision{Allow: false, Reason: RetrySafetyDecisionNoHook}
		}
		var decision RetrySafetyDecision
		hookFailed := false
		callHook(func() {
			defer func() {
				if recover() != nil {
					hookFailed = true
				}
			}()
			decision = policy.Hook(ctx, check)
		})
		if hookFailed {
			return RetrySafetyDecision{Allow: false, Reason: RetrySafetyDecisionHookFailed}
		}
		return normalizeRetrySafetyHookDecision(decision)
	default:
		// RetrySafetyUnspecified and unknown values are unsafe when retry is enabled.
		return RetrySafetyDecision{Allow: false, Reason: RetrySafetyDecisionUnsafe}
	}
}

func normalizeRetrySafetyHookDecision(decision RetrySafetyDecision) RetrySafetyDecision {
	if decision.Allow && decision.Reason == RetrySafetyDecisionUnsafe {
		decision.Reason = RetrySafetyDecisionHookAllowed
	}
	if !decision.Allow && decision.Reason == RetrySafetyDecisionSafe {
		decision.Reason = RetrySafetyDecisionHookRejected
	}
	if decision.Reason == "" {
		if decision.Allow {
			decision.Reason = RetrySafetyDecisionHookAllowed
		} else {
			decision.Reason = RetrySafetyDecisionHookRejected
		}
	}
	return decision
}
