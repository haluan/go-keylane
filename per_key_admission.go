// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"errors"
	"fmt"
	"time"

	"github.com/haluan/go-keylane/internal/core"
)

// PerKeyMitigationAction is the mitigation outcome for a single key hash.
type PerKeyMitigationAction = core.PerKeyMitigationAction

const (
	PerKeyMitigationAllow    = core.PerKeyMitigationAllow
	PerKeyMitigationThrottle = core.PerKeyMitigationThrottle
	PerKeyMitigationReject   = core.PerKeyMitigationReject
	PerKeyMitigationShed     = core.PerKeyMitigationShed
)

// PerKeyAdmissionReason explains why a per-key decision was made.
type PerKeyAdmissionReason = core.PerKeyAdmissionReason

const (
	PerKeyAdmissionReasonNone              = core.PerKeyAdmissionReasonNone
	PerKeyAdmissionReasonHotKeyCandidate   = core.PerKeyAdmissionReasonHotKeyCandidate
	PerKeyAdmissionReasonDominantHotKey    = core.PerKeyAdmissionReasonDominantHotKey
	PerKeyAdmissionReasonMaxQueuedPerKey   = core.PerKeyAdmissionReasonMaxQueuedPerKey
	PerKeyAdmissionReasonMaxInflightPerKey = core.PerKeyAdmissionReasonMaxInflightPerKey
	PerKeyAdmissionReasonCooldownActive    = core.PerKeyAdmissionReasonCooldownActive
	PerKeyAdmissionReasonShardOverloaded   = core.PerKeyAdmissionReasonShardOverloaded
)

// PerKeyAdmissionConfig controls targeted hot key mitigation (KL-1502).
// Zero value disables per-key admission. Requires HotKey tracking when enabled.
type PerKeyAdmissionConfig struct {
	Enabled bool

	MinStatus HotKeyStatus

	DefaultAction PerKeyMitigationAction

	MaxQueuedPerKey   int
	MaxInflightPerKey int

	PressureRatioThreshold float64
	RejectRatioThreshold   float64

	Cooldown       time.Duration
	RecoveryWindow time.Duration

	MaxSnapshotsPerShard int

	// MaxSnapshotsTotal caps total per-key mitigation entries across all shards in DebugSnapshot.
	MaxSnapshotsTotal int
}

// DefaultPerKeyAdmissionConfig returns conservative recommended defaults.
func DefaultPerKeyAdmissionConfig() PerKeyAdmissionConfig {
	return PerKeyAdmissionConfig{
		Enabled:                true,
		MinStatus:              HotKeyStatusCandidate,
		DefaultAction:          PerKeyMitigationThrottle,
		PressureRatioThreshold: 0.40,
		RejectRatioThreshold:   0.20,
		Cooldown:               10 * time.Second,
		RecoveryWindow:         30 * time.Second,
		MaxSnapshotsPerShard:   5,
		MaxSnapshotsTotal:      25,
	}
}

// PerKeyAdmissionDecision is the outcome of per-key policy evaluation.
type PerKeyAdmissionDecision struct {
	Action PerKeyMitigationAction
	Reason PerKeyAdmissionReason

	ShardID int
	LaneID  uint16
	KeyHash uint64

	HotKeyStatus  HotKeyStatus
	PressureRatio float64

	RetryAfter        time.Duration
	CooldownRemaining time.Duration
}

// PerKeyMitigationSnapshot is the spec-aligned per-key mitigation debug view (KL-1505).
// Cumulative per-action breakdown for a single key is approximate; use Prometheus
// per_key_admission_decisions_total for global action/reason totals.
type PerKeyMitigationSnapshot struct {
	ShardID int
	LaneID  uint16
	KeyHash uint64

	Action string
	Reason string

	AllowedApprox   uint64
	DelayedApprox   uint64
	RejectedApprox  uint64
	ShedApprox      uint64
	ThrottledApprox uint64

	QueuedApprox   int64
	InflightApprox int64
	PressureRatio  float64
}

// PerKeyAdmissionSnapshot is a copy-out view of active per-key mitigation state.
type PerKeyAdmissionSnapshot struct {
	ShardID int
	LaneID  uint16
	KeyHash uint64

	Action PerKeyMitigationAction
	Reason PerKeyAdmissionReason

	QueuedApprox   int64
	InflightApprox int64
	PressureRatio  float64

	CooldownRemaining time.Duration
	LastDecisionAt    time.Time

	// RejectedApprox is cumulative rejects observed for this key hash on the shard.
	RejectedApprox uint64
}

var (
	ErrPerKeyAdmissionRejected  = errors.New("keylane: per-key admission rejected")
	ErrPerKeyAdmissionThrottled = errors.New("keylane: per-key admission throttled")
	ErrPerKeyAdmissionShed      = errors.New("keylane: per-key admission shed")
)

// PerKeyAdmissionError carries structured per-key admission decision details.
type PerKeyAdmissionError struct {
	Decision PerKeyAdmissionDecision
}

func (e PerKeyAdmissionError) Error() string {
	switch e.Decision.Action {
	case PerKeyMitigationThrottle:
		return fmt.Sprintf("keylane: per-key admission throttled (shard %d key_hash %x reason %s)",
			e.Decision.ShardID, e.Decision.KeyHash, e.Decision.Reason)
	case PerKeyMitigationShed:
		return fmt.Sprintf("keylane: per-key admission shed (shard %d key_hash %x reason %s)",
			e.Decision.ShardID, e.Decision.KeyHash, e.Decision.Reason)
	default:
		return fmt.Sprintf("keylane: per-key admission rejected (shard %d key_hash %x reason %s)",
			e.Decision.ShardID, e.Decision.KeyHash, e.Decision.Reason)
	}
}

func (e PerKeyAdmissionError) Unwrap() error {
	switch e.Decision.Action {
	case PerKeyMitigationThrottle:
		return ErrPerKeyAdmissionThrottled
	case PerKeyMitigationShed:
		return ErrPerKeyAdmissionShed
	default:
		return ErrPerKeyAdmissionRejected
	}
}

// NormalizePerKeyAdmissionConfig applies defaults for unset fields when enabled.
func NormalizePerKeyAdmissionConfig(cfg *PerKeyAdmissionConfig) {
	if cfg == nil || !cfg.Enabled {
		return
	}
	def := DefaultPerKeyAdmissionConfig()
	if cfg.MinStatus == "" {
		cfg.MinStatus = def.MinStatus
	}
	if cfg.DefaultAction == "" {
		cfg.DefaultAction = def.DefaultAction
	}
	if cfg.PressureRatioThreshold <= 0 {
		cfg.PressureRatioThreshold = def.PressureRatioThreshold
	}
	if cfg.RejectRatioThreshold <= 0 {
		cfg.RejectRatioThreshold = def.RejectRatioThreshold
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = def.Cooldown
	}
	if cfg.RecoveryWindow <= 0 {
		cfg.RecoveryWindow = def.RecoveryWindow
	}
	if cfg.MaxSnapshotsPerShard <= 0 {
		cfg.MaxSnapshotsPerShard = def.MaxSnapshotsPerShard
	}
	if cfg.MaxSnapshotsTotal <= 0 {
		cfg.MaxSnapshotsTotal = def.MaxSnapshotsTotal
	}
}

// ValidatePerKeyAdmissionConfig validates per-key admission settings.
func ValidatePerKeyAdmissionConfig(cfg PerKeyAdmissionConfig, hotKey HotKeyConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if !hotKey.Enabled || hotKey.MaxTrackedKeysPerShard <= 0 {
		return fmt.Errorf("%w: per-key admission requires hot key tracking with MaxTrackedKeysPerShard > 0", ErrInvalidPerKeyAdmissionConfig)
	}
	if cfg.MinStatus != HotKeyStatusCandidate && cfg.MinStatus != HotKeyStatusDominant {
		return fmt.Errorf("%w: MinStatus must be candidate or dominant", ErrInvalidPerKeyAdmissionConfig)
	}
	switch cfg.DefaultAction {
	case PerKeyMitigationAllow, PerKeyMitigationThrottle, PerKeyMitigationReject, PerKeyMitigationShed, "":
	default:
		return fmt.Errorf("%w: invalid DefaultAction %q", ErrInvalidPerKeyAdmissionConfig, cfg.DefaultAction)
	}
	if cfg.PressureRatioThreshold < 0 || cfg.PressureRatioThreshold > 1 {
		return fmt.Errorf("%w: PressureRatioThreshold must be between 0 and 1", ErrInvalidPerKeyAdmissionConfig)
	}
	if cfg.RejectRatioThreshold < 0 || cfg.RejectRatioThreshold > 1 {
		return fmt.Errorf("%w: RejectRatioThreshold must be between 0 and 1", ErrInvalidPerKeyAdmissionConfig)
	}
	if cfg.MaxQueuedPerKey < 0 || cfg.MaxInflightPerKey < 0 {
		return fmt.Errorf("%w: MaxQueuedPerKey and MaxInflightPerKey must be non-negative", ErrInvalidPerKeyAdmissionConfig)
	}
	if cfg.MaxSnapshotsPerShard < 0 {
		return fmt.Errorf("%w: MaxSnapshotsPerShard must be non-negative", ErrInvalidPerKeyAdmissionConfig)
	}
	if cfg.MaxSnapshotsTotal < 0 {
		return fmt.Errorf("%w: MaxSnapshotsTotal must be non-negative", ErrInvalidPerKeyAdmissionConfig)
	}
	return nil
}

// CheckPerKeyAdmission evaluates per-key mitigation before enqueue.
func CheckPerKeyAdmission(q *Queue, cfg PerKeyAdmissionConfig, meta RequestMeta) error {
	if q == nil {
		return ErrNilQueue
	}
	if !cfg.Enabled {
		return nil
	}
	if err := meta.Lane.Validate(); err != nil {
		return err
	}

	var coreCfg core.PerKeyAdmissionConfig
	if q.perKeyAdmissionEnabled && perKeyAdmissionConfigEqual(cfg, q.config.PerKeyAdmission) {
		coreCfg = q.perKeyAdmissionCore
	} else {
		normalized := cfg
		NormalizePerKeyAdmissionConfig(&normalized)
		if err := ValidatePerKeyAdmissionConfig(normalized, q.config.HotKey); err != nil {
			return err
		}
		coreCfg = toCorePerKeyAdmissionConfig(normalized)
	}

	laneID, ok := q.reg.Lookup(string(meta.Lane))
	if !ok {
		return ErrInvalidLane
	}
	keyHash := core.HashKey(meta.Key)
	shardID := q.ShardIDForKey(meta.Key)
	dec := q.sched.EvaluatePerKeyAdmissionWithConfig(shardID, keyHash, laneID, coreCfg)
	if dec.Action == core.PerKeyMitigationAllow {
		return nil
	}

	q.sched.RecordHotKeyReject(keyHash, shardID)

	pub := copyPerKeyAdmissionDecision(dec)
	if q.hooksEnabled() && q.config.Observability.Hooks.OnPerKeyAdmissionDecision != nil {
		callHook(func() {
			q.config.Observability.Hooks.OnPerKeyAdmissionDecision(PerKeyAdmissionDecisionEvent{Decision: pub})
		})
	}
	return PerKeyAdmissionError{Decision: pub}
}

func perKeyAdmissionConfigEqual(a, b PerKeyAdmissionConfig) bool {
	return a.Enabled == b.Enabled &&
		a.MinStatus == b.MinStatus &&
		a.DefaultAction == b.DefaultAction &&
		a.MaxQueuedPerKey == b.MaxQueuedPerKey &&
		a.MaxInflightPerKey == b.MaxInflightPerKey &&
		a.PressureRatioThreshold == b.PressureRatioThreshold &&
		a.RejectRatioThreshold == b.RejectRatioThreshold &&
		a.Cooldown == b.Cooldown &&
		a.RecoveryWindow == b.RecoveryWindow &&
		a.MaxSnapshotsPerShard == b.MaxSnapshotsPerShard &&
		a.MaxSnapshotsTotal == b.MaxSnapshotsTotal
}
