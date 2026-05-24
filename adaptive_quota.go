// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Adaptive quota is an optional periodic controller that adjusts per-lane drain
// quotas conservatively based on runtime pressure and queue-wait signals. It is
// disabled by default. Quota changes use the same safe UpdateQuotaPolicy path as
// manual updates; the submit hot path does not run adaptive evaluation.
package keylane

import (
	"fmt"
	"time"

	"github.com/haluan/go-keylane/internal/core"
)

// QuotaAdjustmentAction is the action the adaptive controller chose for a lane.
type QuotaAdjustmentAction string

const (
	QuotaAdjustmentHold     QuotaAdjustmentAction = QuotaAdjustmentAction(core.QuotaAdjustmentHold)
	QuotaAdjustmentIncrease QuotaAdjustmentAction = QuotaAdjustmentAction(core.QuotaAdjustmentIncrease)
	QuotaAdjustmentDecrease QuotaAdjustmentAction = QuotaAdjustmentAction(core.QuotaAdjustmentDecrease)
)

// QuotaAdjustmentReason is a stable reason code for adaptive quota decisions.
type QuotaAdjustmentReason = core.QuotaAdjustmentReason

const (
	QuotaReasonNone                    = core.QuotaReasonNone
	QuotaReasonCriticalQueueWaitHigh   = core.QuotaReasonCriticalQueueWaitHigh
	QuotaReasonNormalQueueWaitHigh     = core.QuotaReasonNormalQueueWaitHigh
	QuotaReasonGlobalPressureHigh      = core.QuotaReasonGlobalPressureHigh
	QuotaReasonBackgroundPressureHigh  = core.QuotaReasonBackgroundPressureHigh
	QuotaReasonBestEffortPressureHigh  = core.QuotaReasonBestEffortPressureHigh
	QuotaReasonRunTimeTooHigh          = core.QuotaReasonRunTimeTooHigh
	QuotaReasonCooldownActive          = core.QuotaReasonCooldownActive
	QuotaReasonAtMinBound              = core.QuotaReasonAtMinBound
	QuotaReasonAtMaxBound              = core.QuotaReasonAtMaxBound
	QuotaReasonInsufficientSamples     = core.QuotaReasonInsufficientSamples
	QuotaReasonIncreaseDisabled        = core.QuotaReasonIncreaseDisabled
	QuotaReasonDecreaseDisabled        = core.QuotaReasonDecreaseDisabled
	QuotaReasonWarmupActive            = core.QuotaReasonWarmupActive
	QuotaReasonNeutralPressure         = core.QuotaReasonNeutralPressure
	QuotaReasonBackgroundQueueWaitHigh = core.QuotaReasonBackgroundQueueWaitHigh
	QuotaReasonQueueFull               = core.QuotaReasonQueueFull
	QuotaReasonUpdateFailed            = core.QuotaReasonUpdateFailed
)

// AdaptiveQuotaConfig configures the adaptive quota evaluation loop.
type AdaptiveQuotaConfig struct {
	Enabled bool

	EvaluationInterval time.Duration
	WarmupDuration     time.Duration
	CooldownDuration   time.Duration

	PressureHigh float64
	PressureLow  float64

	QueueWaitHigh time.Duration
	RunTimeHigh   time.Duration

	IncreaseStep int
	DecreaseStep int

	MaxAdjustmentsPerTick int

	EnableIncrease bool
	EnableDecrease bool
}

// LaneAdaptivePolicy describes per-lane adaptive quota bounds and permissions.
type LaneAdaptivePolicy struct {
	Lane  Lane
	Class LaneClass

	Enabled bool

	MinQuota int
	MaxQuota int

	AllowIncrease bool
	AllowDecrease bool

	TargetQueueWait time.Duration
	TargetRunTime   time.Duration
}

// AdaptiveQuotaPolicy bundles controller config and per-lane policies.
type AdaptiveQuotaPolicy struct {
	Config AdaptiveQuotaConfig
	Lanes  []LaneAdaptivePolicy
}

// QuotaAdjustmentDecision is the outcome of one lane evaluation tick.
type QuotaAdjustmentDecision struct {
	Lane  Lane
	Class LaneClass

	Action QuotaAdjustmentAction
	Reason QuotaAdjustmentReason

	OldQuota int
	NewQuota int

	GlobalPressure float64
	QueueDepth     uint32
	InFlight       uint32
	QueueWaitP50   time.Duration
	QueueWaitP95   time.Duration
	QueueWaitP99   time.Duration
	RunP50         time.Duration
	RunP95         time.Duration
	RunP99         time.Duration

	PolicyVersion uint64
	QuotaVersion  uint64
}

// AdaptiveQuotaDecisionEvent is the spec name for adaptive quota decision payloads.
type AdaptiveQuotaDecisionEvent struct {
	Time time.Time

	Lane  Lane
	Class LaneClass

	Action QuotaAdjustmentAction
	Reason QuotaAdjustmentReason

	OldQuota int
	NewQuota int

	GlobalPressure float64
	QueueDepth     uint32
	InFlight       uint32

	QueueWaitP50 time.Duration
	QueueWaitP95 time.Duration
	QueueWaitP99 time.Duration
	RunP50       time.Duration
	RunP95       time.Duration
	RunP99       time.Duration

	PolicyVersion uint64
	QuotaVersion  uint64
}

// AdaptiveQuotaEvent is an alias for AdaptiveQuotaDecisionEvent (legacy name).
type AdaptiveQuotaEvent = AdaptiveQuotaDecisionEvent

// LaneAdaptiveStats holds per-lane adaptive quota observability counters.
type LaneAdaptiveStats struct {
	Lane Lane

	KeepTotal    uint64
	RejectTotal  uint64
	ShedTotal    uint64
	DegradeTotal uint64

	QueueFullTotal uint64

	QuotaChangeTotal      uint64
	AdaptiveIncreaseTotal uint64
	AdaptiveDecreaseTotal uint64
	AdaptiveHoldTotal     uint64

	LastQuotaChange time.Time
	LastDecision    QuotaAdjustmentReason
}

// AdaptiveDebugSnapshot is a diagnostic view of adaptive quota controller state and per-lane stats.
type AdaptiveDebugSnapshot struct {
	Enabled bool
	Running bool

	LastEvaluation time.Time
	TickCount      uint64

	LastDecisions []QuotaAdjustmentDecision

	Lanes []LaneAdaptiveStats

	PolicyVersion uint64
	QuotaVersion  uint64
}

// AdaptiveControllerSnapshot is a read-only view of controller state.
//
// Deprecated: use AdaptiveDebugSnapshot for operator diagnostics; this type is
// retained for compatibility and omits per-lane stats.
type AdaptiveControllerSnapshot struct {
	Enabled bool
	Running bool

	LastEvaluation time.Time
	TickCount      uint64

	LastDecisions []QuotaAdjustmentDecision

	PolicyVersion uint64
	QuotaVersion  uint64
}

// DefaultAdaptiveQuotaConfig returns conservative defaults (controller disabled).
func DefaultAdaptiveQuotaConfig() AdaptiveQuotaConfig {
	return AdaptiveQuotaConfig{
		Enabled:               false,
		EvaluationInterval:    time.Second,
		WarmupDuration:        5 * time.Second,
		CooldownDuration:      5 * time.Second,
		PressureHigh:          0.85,
		PressureLow:           0.60,
		QueueWaitHigh:         25 * time.Millisecond,
		RunTimeHigh:           250 * time.Millisecond,
		IncreaseStep:          1,
		DecreaseStep:          1,
		MaxAdjustmentsPerTick: 1,
		EnableIncrease:        true,
		EnableDecrease:        true,
	}
}

// NormalizeAdaptiveQuotaConfig applies defaults for unset zero-valued fields.
// Call only after ValidateAdaptiveQuotaPolicy when enabled; invalid negatives and
// MaxAdjustmentsPerTick < 1 must be rejected before normalization.
func NormalizeAdaptiveQuotaConfig(cfg *AdaptiveQuotaConfig) {
	if cfg == nil {
		return
	}
	def := DefaultAdaptiveQuotaConfig()
	if cfg.EvaluationInterval <= 0 {
		cfg.EvaluationInterval = def.EvaluationInterval
	}
	if cfg.WarmupDuration <= 0 {
		cfg.WarmupDuration = def.WarmupDuration
	}
	if cfg.CooldownDuration <= 0 {
		cfg.CooldownDuration = def.CooldownDuration
	}
	if cfg.PressureHigh <= 0 {
		cfg.PressureHigh = def.PressureHigh
	}
	if cfg.PressureLow <= 0 {
		cfg.PressureLow = def.PressureLow
	}
	if cfg.QueueWaitHigh <= 0 {
		cfg.QueueWaitHigh = def.QueueWaitHigh
	}
	if cfg.RunTimeHigh <= 0 {
		cfg.RunTimeHigh = def.RunTimeHigh
	}
	if cfg.IncreaseStep <= 0 {
		cfg.IncreaseStep = def.IncreaseStep
	}
	if cfg.DecreaseStep <= 0 {
		cfg.DecreaseStep = def.DecreaseStep
	}
	if cfg.MaxAdjustmentsPerTick <= 0 {
		cfg.MaxAdjustmentsPerTick = def.MaxAdjustmentsPerTick
	}
	// Both false is the zero-value pair; treat as unset and apply defaults.
	if !cfg.EnableIncrease && !cfg.EnableDecrease {
		cfg.EnableIncrease = def.EnableIncrease
		cfg.EnableDecrease = def.EnableDecrease
	}
}

// ValidateAdaptiveQuotaPolicy validates adaptive quota settings against registered lanes.
func ValidateAdaptiveQuotaPolicy(policy AdaptiveQuotaPolicy, laneQuotas map[Lane]int) error {
	raw := policy.Config
	if !raw.Enabled {
		return nil
	}
	if raw.EvaluationInterval <= 0 {
		return fmt.Errorf("%w: EvaluationInterval must be positive when enabled", ErrInvalidAdaptiveQuotaConfig)
	}
	if raw.PressureLow < 0 {
		return fmt.Errorf("%w: PressureLow must be non-negative when enabled", ErrInvalidAdaptiveQuotaConfig)
	}
	if raw.PressureHigh < 0 {
		return fmt.Errorf("%w: PressureHigh must be non-negative when enabled", ErrInvalidAdaptiveQuotaConfig)
	}
	if raw.IncreaseStep < 0 {
		return fmt.Errorf("%w: IncreaseStep must be non-negative when enabled", ErrInvalidAdaptiveQuotaConfig)
	}
	if raw.DecreaseStep < 0 {
		return fmt.Errorf("%w: DecreaseStep must be non-negative when enabled", ErrInvalidAdaptiveQuotaConfig)
	}
	if raw.MaxAdjustmentsPerTick < 1 {
		return fmt.Errorf("%w: MaxAdjustmentsPerTick must be at least 1 when enabled", ErrInvalidAdaptiveQuotaConfig)
	}

	cfg := raw
	NormalizeAdaptiveQuotaConfig(&cfg)
	if cfg.PressureLow < 0 || cfg.PressureLow > 1 {
		return fmt.Errorf("%w: PressureLow must be in [0,1]", ErrInvalidAdaptiveQuotaConfig)
	}
	if cfg.PressureHigh < 0 || cfg.PressureHigh > 1 {
		return fmt.Errorf("%w: PressureHigh must be in [0,1]", ErrInvalidAdaptiveQuotaConfig)
	}
	if cfg.PressureLow >= cfg.PressureHigh {
		return fmt.Errorf("%w: PressureLow must be less than PressureHigh", ErrInvalidAdaptiveQuotaConfig)
	}
	seen := make(map[Lane]struct{}, len(policy.Lanes))
	for _, lp := range policy.Lanes {
		if err := lp.Lane.Validate(); err != nil {
			return err
		}
		if _, ok := laneQuotas[lp.Lane]; !ok {
			return fmt.Errorf("%w: unknown lane %q", ErrInvalidAdaptiveQuotaConfig, lp.Lane)
		}
		if _, dup := seen[lp.Lane]; dup {
			return fmt.Errorf("%w: duplicate lane %q", ErrInvalidAdaptiveQuotaConfig, lp.Lane)
		}
		seen[lp.Lane] = struct{}{}
		if lp.Class != "" {
			if err := lp.Class.Validate(); err != nil {
				return err
			}
		}
		if lp.MinQuota < 1 {
			return fmt.Errorf("%w: MinQuota for lane %q must be at least 1", ErrInvalidAdaptiveQuotaConfig, lp.Lane)
		}
		if lp.MaxQuota < lp.MinQuota {
			return fmt.Errorf("%w: MaxQuota must be >= MinQuota for lane %q", ErrInvalidAdaptiveQuotaConfig, lp.Lane)
		}
		if lp.MaxQuota > int(MaxLaneQuota) {
			return fmt.Errorf("%w: MaxQuota for lane %q exceeds %d", ErrInvalidAdaptiveQuotaConfig, lp.Lane, MaxLaneQuota)
		}
	}
	return nil
}

func adaptiveQuotaConfigToCore(cfg AdaptiveQuotaConfig, emitHoldDecisions bool) core.AdaptiveQuotaConfig {
	return core.AdaptiveQuotaConfig{
		Enabled:               cfg.Enabled,
		EvaluationInterval:    cfg.EvaluationInterval,
		WarmupDuration:        cfg.WarmupDuration,
		CooldownDuration:      cfg.CooldownDuration,
		PressureHigh:          cfg.PressureHigh,
		PressureLow:           cfg.PressureLow,
		QueueWaitHigh:         cfg.QueueWaitHigh,
		RunTimeHigh:           cfg.RunTimeHigh,
		IncreaseStep:          cfg.IncreaseStep,
		DecreaseStep:          cfg.DecreaseStep,
		MaxAdjustmentsPerTick: cfg.MaxAdjustmentsPerTick,
		EnableIncrease:        cfg.EnableIncrease,
		EnableDecrease:        cfg.EnableDecrease,
		EmitHoldDecisions:     emitHoldDecisions,
	}
}

func laneAdaptivePolicyToCore(lp LaneAdaptivePolicy) core.LaneAdaptivePolicy {
	return core.LaneAdaptivePolicy{
		Lane:            string(lp.Lane),
		Class:           string(lp.Class),
		Enabled:         lp.Enabled,
		MinQuota:        lp.MinQuota,
		MaxQuota:        lp.MaxQuota,
		AllowIncrease:   lp.AllowIncrease,
		AllowDecrease:   lp.AllowDecrease,
		TargetQueueWait: lp.TargetQueueWait,
		TargetRunTime:   lp.TargetRunTime,
	}
}

func quotaAdjustmentDecisionFromCore(d core.QuotaAdjustmentDecision) QuotaAdjustmentDecision {
	return QuotaAdjustmentDecision{
		Lane:           Lane(d.Lane),
		Class:          LaneClass(d.Class),
		Action:         QuotaAdjustmentAction(d.Action),
		Reason:         QuotaAdjustmentReason(d.Reason),
		OldQuota:       d.OldQuota,
		NewQuota:       d.NewQuota,
		GlobalPressure: d.GlobalPressure,
		QueueDepth:     d.QueueDepth,
		InFlight:       d.InFlight,
		QueueWaitP50:   d.QueueWaitP50,
		QueueWaitP95:   d.QueueWaitP95,
		QueueWaitP99:   d.QueueWaitP99,
		RunP50:         d.RunP50,
		RunP95:         d.RunP95,
		RunP99:         d.RunP99,
		PolicyVersion:  d.PolicyVersion,
		QuotaVersion:   d.QuotaVersion,
	}
}
