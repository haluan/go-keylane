// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "time"

type QuotaAdjustmentAction string

const (
	QuotaAdjustmentHold     QuotaAdjustmentAction = "hold"
	QuotaAdjustmentIncrease QuotaAdjustmentAction = "increase"
	QuotaAdjustmentDecrease QuotaAdjustmentAction = "decrease"
)

type QuotaAdjustmentReason string

const (
	QuotaReasonNone                    QuotaAdjustmentReason = "none"
	QuotaReasonCriticalQueueWaitHigh   QuotaAdjustmentReason = "critical_queue_wait_high"
	QuotaReasonNormalQueueWaitHigh     QuotaAdjustmentReason = "normal_queue_wait_high"
	QuotaReasonGlobalPressureHigh      QuotaAdjustmentReason = "global_pressure_high"
	QuotaReasonBackgroundPressureHigh  QuotaAdjustmentReason = "background_pressure_high"
	QuotaReasonBestEffortPressureHigh  QuotaAdjustmentReason = "best_effort_pressure_high"
	QuotaReasonRunTimeTooHigh          QuotaAdjustmentReason = "runtime_too_high"
	QuotaReasonCooldownActive          QuotaAdjustmentReason = "cooldown_active"
	QuotaReasonAtMinBound              QuotaAdjustmentReason = "at_min_bound"
	QuotaReasonAtMaxBound              QuotaAdjustmentReason = "at_max_bound"
	QuotaReasonInsufficientSamples     QuotaAdjustmentReason = "insufficient_samples"
	QuotaReasonIncreaseDisabled        QuotaAdjustmentReason = "increase_disabled"
	QuotaReasonDecreaseDisabled        QuotaAdjustmentReason = "decrease_disabled"
	QuotaReasonWarmupActive            QuotaAdjustmentReason = "warmup_active"
	QuotaReasonNeutralPressure         QuotaAdjustmentReason = "neutral_pressure"
	QuotaReasonBackgroundQueueWaitHigh QuotaAdjustmentReason = "background_queue_wait_high"
	QuotaReasonQueueFull               QuotaAdjustmentReason = "queue_full"
	QuotaReasonUpdateFailed            QuotaAdjustmentReason = "quota_update_failed"
)

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

	// EmitHoldDecisions enables hooks/counters for hold outcomes (debug tracing).
	EmitHoldDecisions bool
}

type LaneAdaptivePolicy struct {
	Lane  string
	Class string

	Enabled bool

	MinQuota int
	MaxQuota int

	AllowIncrease bool
	AllowDecrease bool

	TargetQueueWait time.Duration
	TargetRunTime   time.Duration
}

type AdaptiveQuotaPolicyInput struct {
	Config AdaptiveQuotaConfig
	Lanes  []LaneAdaptivePolicy
}

// resolvedLaneAdaptivePolicy is indexed by LaneID after merge with defaults.
type resolvedLaneAdaptivePolicy struct {
	LaneID LaneID
	Lane   string
	Class  string

	Enabled bool

	MinQuota int
	MaxQuota int

	AllowIncrease bool
	AllowDecrease bool

	TargetQueueWait time.Duration
	TargetRunTime   time.Duration
}

type AdaptiveSignalSnapshot struct {
	Time time.Time

	GlobalPressure float64

	Lanes []LaneAdaptiveSignal

	PolicyVersion uint64
	QuotaVersion  uint64
}

type LaneAdaptiveSignal struct {
	LaneID LaneID
	Lane   string
	Class  string

	CurrentQuota int
	MinQuota     int
	MaxQuota     int

	QueueDepth uint32
	InFlight   uint32

	QueueWaitAvg     time.Duration
	QueueWaitMax     time.Duration
	QueueWaitSamples uint64

	RunAvg     time.Duration
	RunMax     time.Duration
	RunSamples uint64

	OverloadRejectCount  uint64
	OverloadShedCount    uint64
	OverloadDegradeCount uint64
	QueueFullCount       uint64
}

type QuotaAdjustmentDecision struct {
	Lane  string
	Class string

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

type AdaptiveControllerState struct {
	StartedAt     time.Time
	LastAdjusted  map[LaneID]time.Time
	LastDecisions []QuotaAdjustmentDecision
	TickCount     uint64
}

const maxLastDecisions = 16

func newAdaptiveControllerState(startedAt time.Time, laneCount int) *AdaptiveControllerState {
	return &AdaptiveControllerState{
		StartedAt:     startedAt,
		LastAdjusted:  make(map[LaneID]time.Time, laneCount),
		LastDecisions: make([]QuotaAdjustmentDecision, 0, maxLastDecisions),
	}
}

func (s *AdaptiveControllerState) recordApplied(laneID LaneID, now time.Time) {
	s.LastAdjusted[laneID] = now
}

func (s *AdaptiveControllerState) appendDecision(d QuotaAdjustmentDecision) {
	if len(s.LastDecisions) >= maxLastDecisions {
		trimmed := make([]QuotaAdjustmentDecision, len(s.LastDecisions)-1)
		copy(trimmed, s.LastDecisions[1:])
		s.LastDecisions = trimmed
	}
	s.LastDecisions = append(s.LastDecisions, d)
}
