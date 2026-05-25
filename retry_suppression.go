// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrInvalidRetrySuppressionPolicy = errors.New("keylane: invalid retry suppression policy")

// RetrySuppressionPolicy configures runtime-health checks before scheduling a retry.
type RetrySuppressionPolicy struct {
	Enabled bool

	SuppressWhenOverloaded           bool
	SuppressNonCriticalWhenPressured bool
	SuppressLaneAboveRatio           float64
	SuppressShardAboveRatio          float64

	SuppressOverloadFailures        bool
	SuppressAdmissionFailures       bool
	SuppressPerKeyAdmissionFailures bool

	SuppressHotKeyRetry bool
	// AllowCriticalHotKeyRetry permits at most one retry for critical lanes when hot-key mitigation is active.
	// Zero value denies critical hot-key retry (opt-in only; not set by Normalize).
	AllowCriticalHotKeyRetry        bool
	SuppressWhenScaleOutRecommended bool

	Hook RetrySuppressionHook
}

// RetrySuppressionReason explains why a retry was suppressed for runtime health.
type RetrySuppressionReason string

const (
	RetrySuppressionNone                RetrySuppressionReason = "none"
	RetrySuppressionDisabled            RetrySuppressionReason = "disabled"
	RetrySuppressionGlobalPressure      RetrySuppressionReason = "global_pressure"
	RetrySuppressionGlobalOverload      RetrySuppressionReason = "global_overload"
	RetrySuppressionLanePressure        RetrySuppressionReason = "lane_pressure"
	RetrySuppressionShardPressure       RetrySuppressionReason = "shard_pressure"
	RetrySuppressionOverloadFailure     RetrySuppressionReason = "overload_failure"
	RetrySuppressionAdmissionFailure    RetrySuppressionReason = "admission_failure"
	RetrySuppressionPerKeyAdmission     RetrySuppressionReason = "per_key_admission"
	RetrySuppressionHotKey              RetrySuppressionReason = "hot_key"
	RetrySuppressionScaleOutRecommended RetrySuppressionReason = "scale_out_recommended"
	RetrySuppressionHookRejected        RetrySuppressionReason = "hook_rejected"
	RetrySuppressionHookFailed          RetrySuppressionReason = "hook_failed"
)

// RetrySuppressionDecision is the outcome of DecideRetrySuppression.
type RetrySuppressionDecision struct {
	Suppress bool
	Reason   RetrySuppressionReason
	Message  string

	Pressure Pressure

	Lane      Lane
	LaneClass LaneClass
	ShardID   int

	Attempt int
	Failure Failure
	Budget  DeadlineBudget

	RetryAfter time.Duration
}

// RetrySuppressionCheck carries inputs for runtime-health retry evaluation.
type RetrySuppressionCheck struct {
	Key     string
	Lane    Lane
	ShardID int

	Attempt int
	Failure Failure
	Retry   RetryPolicy
	Budget  DeadlineBudget

	Pressure Pressure

	LaneDepthRatio  float64
	ShardDepthRatio float64

	ScaleSignal ScaleSignal

	Idempotency Idempotency
	LaneClass   LaneClass

	HotKeyCandidate bool
}

// RetrySuppressionHook allows application-specific retry suppression decisions.
type RetrySuppressionHook func(context.Context, RetrySuppressionCheck) RetrySuppressionDecision

// RetrySuppressionSnapshot is a cheap runtime snapshot for suppression decisions.
type RetrySuppressionSnapshot struct {
	Pressure        Pressure
	LaneDepthRatio  float64
	ShardDepthRatio float64
	ScaleSignal     ScaleSignal
	LaneClass       LaneClass
	HotKeyCandidate bool
}

// RetrySuppressionSnapshotFunc captures runtime state for a retry suppression check.
type RetrySuppressionSnapshotFunc func(key string, lane Lane, shardID int) RetrySuppressionSnapshot

// NormalizeRetrySuppressionPolicy applies defaults when suppression is enabled.
func NormalizeRetrySuppressionPolicy(cfg *RetrySuppressionPolicy) {
	if cfg == nil || !cfg.Enabled {
		return
	}
	if !cfg.SuppressWhenOverloaded &&
		!cfg.SuppressNonCriticalWhenPressured &&
		!cfg.SuppressOverloadFailures &&
		!cfg.SuppressAdmissionFailures &&
		!cfg.SuppressPerKeyAdmissionFailures &&
		!cfg.SuppressHotKeyRetry &&
		!cfg.SuppressWhenScaleOutRecommended &&
		cfg.SuppressLaneAboveRatio == 0 &&
		cfg.SuppressShardAboveRatio == 0 &&
		cfg.Hook == nil {
		cfg.SuppressWhenOverloaded = true
		cfg.SuppressNonCriticalWhenPressured = true
		cfg.SuppressOverloadFailures = true
		cfg.SuppressAdmissionFailures = true
		cfg.SuppressPerKeyAdmissionFailures = true
		cfg.SuppressHotKeyRetry = true
		cfg.SuppressWhenScaleOutRecommended = true
	}
	if cfg.SuppressLaneAboveRatio == 0 {
		cfg.SuppressLaneAboveRatio = PressuredDepthRatio
	}
	if cfg.SuppressShardAboveRatio == 0 {
		cfg.SuppressShardAboveRatio = PressuredDepthRatio
	}
}

// ValidateRetrySuppressionPolicy validates retry suppression policy settings.
func ValidateRetrySuppressionPolicy(cfg RetrySuppressionPolicy) error {
	if !cfg.Enabled {
		return nil
	}
	if err := validateSuppressionRatio(cfg.SuppressLaneAboveRatio, "SuppressLaneAboveRatio"); err != nil {
		return err
	}
	if err := validateSuppressionRatio(cfg.SuppressShardAboveRatio, "SuppressShardAboveRatio"); err != nil {
		return err
	}
	return nil
}

func validateSuppressionRatio(r float64, name string) error {
	if r == 0 {
		return nil
	}
	if r <= 0 || r > 1 {
		return fmt.Errorf("%w: %s must be in (0, 1]", ErrInvalidRetrySuppressionPolicy, name)
	}
	return nil
}

func resolveRetrySuppressionPolicy(queue RetrySuppressionPolicy, override *RetrySuppressionPolicy) RetrySuppressionPolicy {
	if override != nil && override.Enabled {
		p := *override
		NormalizeRetrySuppressionPolicy(&p)
		return p
	}
	if queue.Enabled {
		p := queue
		NormalizeRetrySuppressionPolicy(&p)
		return p
	}
	return RetrySuppressionPolicy{}
}

// DecideRetrySuppression determines whether a retry should proceed after eligibility and safety checks.
func DecideRetrySuppression(ctx context.Context, policy RetrySuppressionPolicy, check RetrySuppressionCheck) RetrySuppressionDecision {
	decision := RetrySuppressionDecision{
		Suppress:  false,
		Reason:    RetrySuppressionNone,
		Pressure:  check.Pressure,
		Lane:      check.Lane,
		LaneClass: check.LaneClass,
		ShardID:   check.ShardID,
		Attempt:   check.Attempt,
		Failure:   check.Failure,
		Budget:    check.Budget,
	}
	if !policy.Enabled {
		decision.Reason = RetrySuppressionDisabled
		return decision
	}

	if policy.SuppressOverloadFailures && check.Failure.Kind == FailureOverloaded {
		return suppressDecision(decision, RetrySuppressionOverloadFailure, "overload failure")
	}
	if policy.SuppressAdmissionFailures && isGeneralAdmissionFailure(check.Failure) {
		return suppressDecision(decision, RetrySuppressionAdmissionFailure, "admission failure")
	}
	if policy.SuppressPerKeyAdmissionFailures && isPerKeyAdmissionFailure(check.Failure) {
		return suppressDecision(decision, RetrySuppressionPerKeyAdmission, "per-key admission failure")
	}

	if policy.SuppressWhenOverloaded && check.Pressure.IsOverloaded {
		return suppressDecision(decision, RetrySuppressionGlobalOverload, "global queue overloaded")
	}
	if policy.SuppressNonCriticalWhenPressured && check.Pressure.IsPressured && isNonCriticalLaneClass(check.LaneClass) {
		return suppressDecision(decision, RetrySuppressionGlobalPressure, "global queue pressured for non-critical lane")
	}
	if policy.SuppressLaneAboveRatio > 0 && check.LaneDepthRatio >= policy.SuppressLaneAboveRatio {
		return suppressDecision(decision, RetrySuppressionLanePressure, "lane depth above threshold")
	}
	if policy.SuppressShardAboveRatio > 0 && check.ShardDepthRatio >= policy.SuppressShardAboveRatio {
		return suppressDecision(decision, RetrySuppressionShardPressure, "shard depth above threshold")
	}

	if policy.SuppressHotKeyRetry && check.HotKeyCandidate {
		if isNonCriticalLaneClass(check.LaneClass) {
			return suppressDecision(decision, RetrySuppressionHotKey, "hot key on non-critical lane")
		}
		if check.LaneClass == LaneCritical {
			if !policy.AllowCriticalHotKeyRetry {
				return suppressDecision(decision, RetrySuppressionHotKey, "hot key on critical lane without explicit permission")
			}
			if check.Attempt > 1 {
				return suppressDecision(decision, RetrySuppressionHotKey, "hot key retry budget exhausted for critical lane")
			}
		}
	}

	if policy.SuppressWhenScaleOutRecommended && check.ScaleSignal.Recommended && isNonCriticalLaneClass(check.LaneClass) {
		return suppressDecision(decision, RetrySuppressionScaleOutRecommended, "scale-out recommended for non-critical lane")
	}

	if policy.Hook == nil {
		return decision
	}

	var hookDecision RetrySuppressionDecision
	hookFailed := false
	callHook(func() {
		defer func() {
			if recover() != nil {
				hookFailed = true
			}
		}()
		hookDecision = policy.Hook(ctx, check)
	})
	if hookFailed {
		return suppressDecision(decision, RetrySuppressionHookFailed, "retry suppression hook failed")
	}
	if hookDecision.Suppress {
		if hookDecision.Reason == "" {
			hookDecision.Reason = RetrySuppressionHookRejected
		}
		return suppressDecision(decision, hookDecision.Reason, hookDecision.Message)
	}
	if hookDecision.Reason == "" {
		hookDecision.Reason = RetrySuppressionNone
	}
	decision.Reason = hookDecision.Reason
	decision.Message = hookDecision.Message
	return decision
}

func suppressDecision(base RetrySuppressionDecision, reason RetrySuppressionReason, message string) RetrySuppressionDecision {
	base.Suppress = true
	base.Reason = reason
	base.Message = message
	return base
}

func isNonCriticalLaneClass(c LaneClass) bool {
	return c == LaneBackground || c == LaneBestEffort
}

func isGeneralAdmissionFailure(f Failure) bool {
	if f.Kind != FailureRejected || f.Err == nil {
		return false
	}
	if isPerKeyAdmissionFailure(f) {
		return false
	}
	if errors.Is(f.Err, ErrAdmissionRejected) {
		return true
	}
	var admission AdmissionRejectedError
	return errors.As(f.Err, &admission)
}

func isPerKeyAdmissionFailure(f Failure) bool {
	if f.Err == nil {
		return false
	}
	if errors.Is(f.Err, ErrPerKeyAdmissionRejected) ||
		errors.Is(f.Err, ErrPerKeyAdmissionThrottled) ||
		errors.Is(f.Err, ErrPerKeyAdmissionShed) {
		return true
	}
	var perKey PerKeyAdmissionError
	return errors.As(f.Err, &perKey)
}
