// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"time"
)

// Overload action names.
const (
	OverloadActionKeep    = "keep"
	OverloadActionReject  = "reject"
	OverloadActionShed    = "shed"
	OverloadActionDegrade = "degrade"
)

// Overload reason codes (stable for tests, metrics, and hooks).
const (
	OverloadReasonNone               = "none"
	OverloadReasonGlobalPressureHigh = "global_pressure_high"
	OverloadReasonLanePressureHigh   = "lane_pressure_high"
	OverloadReasonLaneDepthExceeded  = "lane_depth_exceeded"
	OverloadReasonQueueFull          = "queue_full"
	OverloadReasonQueueClosed        = "queue_closed"
	OverloadReasonBestEffortShedding = "best_effort_shedding"
	OverloadReasonBackgroundShedding = "background_shedding"
	OverloadReasonDegradePreferred   = "degrade_preferred"
)

// LaneOverloadPolicyInput is the scheduler-facing per-lane overload policy entry.
type LaneOverloadPolicyInput struct {
	Lane              string
	Class             string
	RejectAboveRatio  float64
	ShedAboveRatio    float64
	DegradeAboveRatio float64
	MaxQueueDepth     uint32
	RetryAfter        time.Duration
	MinBackoff        time.Duration
	MaxBackoff        time.Duration
	Jitter            bool
}

// OverloadPolicyInput is the scheduler-facing overload policy.
type OverloadPolicyInput struct {
	Default LaneOverloadPolicyInput
	Lanes   []LaneOverloadPolicyInput
}

// OverloadSignals are runtime inputs for overload evaluation.
type OverloadSignals struct {
	GlobalPressure float64
	LanePressure   float64
	LaneDepth      uint32
	QueueFull      bool
	QueueClosed    bool
}

type laneOverloadEntry struct {
	class             string
	rejectAboveRatio  float64
	shedAboveRatio    float64
	degradeAboveRatio float64
	maxQueueDepth     uint32
	retryAfter        time.Duration
	minBackoff        time.Duration
	maxBackoff        time.Duration
	jitter            bool
}

type overloadPolicySnapshot struct {
	version       uint64
	defaultPolicy laneOverloadEntry
	lanes         []laneOverloadEntry
}

// OverloadEvalResult is the outcome of evaluating overload for one lane.
type OverloadEvalResult struct {
	Action        string
	Reason        string
	Class         string
	Pressure      float64
	LaneDepth     uint32
	MaxDepth      uint32
	RetryAfter    time.Duration
	MinBackoff    time.Duration
	MaxBackoff    time.Duration
	Jitter        bool
	PolicyVersion uint64
}

func validateOverloadRatio(name string, ratio float64) error {
	if ratio < 0 || ratio > 1 {
		return fmt.Errorf("%w: %s must be between 0.0 and 1.0", ErrInvalidOverloadPolicy, name)
	}
	return nil
}

func validateBackoffDurations(retryAfter, minBackoff, maxBackoff time.Duration) error {
	if retryAfter < 0 {
		return fmt.Errorf("%w: RetryAfter must be non-negative", ErrInvalidOverloadPolicy)
	}
	if minBackoff < 0 {
		return fmt.Errorf("%w: MinBackoff must be non-negative", ErrInvalidOverloadPolicy)
	}
	if maxBackoff < 0 {
		return fmt.Errorf("%w: MaxBackoff must be non-negative", ErrInvalidOverloadPolicy)
	}
	if maxBackoff > 0 && minBackoff > maxBackoff {
		return fmt.Errorf("%w: MaxBackoff must be >= MinBackoff", ErrInvalidOverloadPolicy)
	}
	if maxBackoff > 0 && retryAfter > maxBackoff {
		return fmt.Errorf("%w: RetryAfter must not exceed MaxBackoff", ErrInvalidOverloadPolicy)
	}
	return nil
}

func validateLaneOverloadPolicyInput(lp LaneOverloadPolicyInput, reg *LaneRegistry, requireLane bool) error {
	if requireLane {
		if lp.Lane == "" {
			return ErrInvalidLane
		}
		if _, ok := reg.Lookup(lp.Lane); !ok {
			return fmt.Errorf("%w: unknown lane %q", ErrInvalidOverloadPolicy, lp.Lane)
		}
	}
	class := lp.Class
	if class == "" {
		class = LaneClassNormal
	}
	if err := ValidateLaneClass(class); err != nil {
		return err
	}
	if err := validateOverloadRatio("RejectAboveRatio", lp.RejectAboveRatio); err != nil {
		return err
	}
	if err := validateOverloadRatio("ShedAboveRatio", lp.ShedAboveRatio); err != nil {
		return err
	}
	if err := validateOverloadRatio("DegradeAboveRatio", lp.DegradeAboveRatio); err != nil {
		return err
	}
	if lp.ShedAboveRatio < 1.0 && lp.ShedAboveRatio > lp.RejectAboveRatio {
		return fmt.Errorf("%w: ShedAboveRatio must be <= RejectAboveRatio when shedding is enabled", ErrInvalidOverloadPolicy)
	}
	if lp.DegradeAboveRatio < 1.0 && lp.DegradeAboveRatio > lp.RejectAboveRatio {
		return fmt.Errorf("%w: DegradeAboveRatio must be <= RejectAboveRatio when degrade is enabled", ErrInvalidOverloadPolicy)
	}
	if lp.MaxQueueDepth < 1 {
		return fmt.Errorf("%w: MaxQueueDepth must be at least 1", ErrInvalidOverloadPolicy)
	}
	return validateBackoffDurations(lp.RetryAfter, lp.MinBackoff, lp.MaxBackoff)
}

func laneEntryFromInput(lp LaneOverloadPolicyInput) laneOverloadEntry {
	class := lp.Class
	if class == "" {
		class = LaneClassNormal
	}
	shed := lp.ShedAboveRatio
	if shed == 0 {
		shed = 1.0
	}
	degrade := lp.DegradeAboveRatio
	if degrade == 0 {
		degrade = 1.0
	}
	return laneOverloadEntry{
		class:             class,
		rejectAboveRatio:  lp.RejectAboveRatio,
		shedAboveRatio:    shed,
		degradeAboveRatio: degrade,
		maxQueueDepth:     lp.MaxQueueDepth,
		retryAfter:        lp.RetryAfter,
		minBackoff:        lp.MinBackoff,
		maxBackoff:        lp.MaxBackoff,
		jitter:            lp.Jitter,
	}
}

func buildOverloadPolicySnapshot(reg *LaneRegistry, policy OverloadPolicyInput) (*overloadPolicySnapshot, error) {
	if err := validateLaneOverloadPolicyInput(policy.Default, reg, false); err != nil {
		return nil, err
	}

	seen := make(map[string]struct{}, len(policy.Lanes))
	for _, lp := range policy.Lanes {
		if _, dup := seen[lp.Lane]; dup {
			return nil, fmt.Errorf("%w: duplicate lane policy %q", ErrInvalidOverloadPolicy, lp.Lane)
		}
		seen[lp.Lane] = struct{}{}
		if err := validateLaneOverloadPolicyInput(lp, reg, true); err != nil {
			return nil, err
		}
	}

	override := make(map[string]LaneOverloadPolicyInput, len(policy.Lanes))
	for _, lp := range policy.Lanes {
		override[lp.Lane] = lp
	}

	defaultEntry := laneEntryFromInput(policy.Default)
	lanes := make([]laneOverloadEntry, reg.Len())
	for i := 0; i < reg.Len(); i++ {
		name := reg.Name(LaneID(i))
		entry := defaultEntry
		if lp, ok := override[name]; ok {
			entry = laneEntryFromInput(lp)
		}
		lanes[i] = entry
	}

	return &overloadPolicySnapshot{
		defaultPolicy: defaultEntry,
		lanes:         lanes,
	}, nil
}

func defaultOverloadPolicy(shardCount, queueSizePerLane int) OverloadPolicyInput {
	maxDepth := uint32(shardCount * queueSizePerLane)
	if maxDepth < 1 {
		maxDepth = 1
	}
	return OverloadPolicyInput{
		Default: LaneOverloadPolicyInput{
			Class:             LaneClassNormal,
			RejectAboveRatio:  0.90,
			ShedAboveRatio:    1.00,
			DegradeAboveRatio: 1.00,
			MaxQueueDepth:     maxDepth,
			RetryAfter:        250 * time.Millisecond,
			MinBackoff:        100 * time.Millisecond,
			MaxBackoff:        2 * time.Second,
			Jitter:            true,
		},
	}
}

func (s *Scheduler) initOverloadPolicy(reg *LaneRegistry, shardCount, queueSizePerLane int) {
	policy := defaultOverloadPolicy(shardCount, queueSizePerLane)
	snap, err := buildOverloadPolicySnapshot(reg, policy)
	if err != nil {
		panic("keylane: default overload policy: " + err.Error())
	}
	s.overloadPolicy.Store(snap)
}

func (s *Scheduler) loadOverloadPolicy() *overloadPolicySnapshot {
	return s.overloadPolicy.Load()
}

func (s *Scheduler) publishOverloadPolicy(snap *overloadPolicySnapshot) uint64 {
	s.overloadMu.Lock()
	defer s.overloadMu.Unlock()
	v := s.overloadVersion.Add(1)
	snap.version = v
	s.overloadPolicy.Store(snap)
	return v
}

// UpdateOverloadPolicy validates and atomically publishes a new overload policy snapshot.
func (s *Scheduler) UpdateOverloadPolicy(policy OverloadPolicyInput) (uint64, error) {
	snap, err := buildOverloadPolicySnapshot(s.laneReg, policy)
	if err != nil {
		return 0, err
	}

	s.mu.RLock()
	state := s.state
	s.mu.RUnlock()
	if state == stateStopping || state == stateStopped {
		return 0, ErrStopped
	}

	return s.publishOverloadPolicy(snap), nil
}

// CurrentOverloadPolicyView returns a copy of the active overload policy.
func (s *Scheduler) CurrentOverloadPolicyView() (version uint64, policy OverloadPolicyInput) {
	snap := s.loadOverloadPolicy()
	policy.Default = laneOverloadPolicyInputFromEntry(snap.defaultPolicy, "")
	policy.Lanes = make([]LaneOverloadPolicyInput, 0, len(snap.lanes))
	for i, entry := range snap.lanes {
		name := s.laneReg.Name(LaneID(i))
		policy.Lanes = append(policy.Lanes, laneOverloadPolicyInputFromEntry(entry, name))
	}
	return snap.version, policy
}

func laneOverloadPolicyInputFromEntry(entry laneOverloadEntry, lane string) LaneOverloadPolicyInput {
	return LaneOverloadPolicyInput{
		Lane:              lane,
		Class:             entry.class,
		RejectAboveRatio:  entry.rejectAboveRatio,
		ShedAboveRatio:    entry.shedAboveRatio,
		DegradeAboveRatio: entry.degradeAboveRatio,
		MaxQueueDepth:     entry.maxQueueDepth,
		RetryAfter:        entry.retryAfter,
		MinBackoff:        entry.minBackoff,
		MaxBackoff:        entry.maxBackoff,
		Jitter:            entry.jitter,
	}
}

func (s *Scheduler) isQueueClosedForOverload() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state == stateStopping || s.state == stateStopped
}

func (s *Scheduler) wouldQueueFull(keyHash uint64, laneID LaneID) bool {
	if int(laneID) < 0 {
		return false
	}
	shardID := routeShardID(keyHash, len(s.shards))
	shard := &s.shards[shardID]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if int(laneID) >= len(shard.Lanes) {
		return false
	}
	lane := &shard.Lanes[laneID]
	return lane.depth() >= lane.capacity()
}

// OverloadSignalsForLane gathers runtime signals for overload evaluation.
func (s *Scheduler) OverloadSignalsForLane(laneID LaneID, keyHash uint64) OverloadSignals {
	p := s.Pressure()
	return OverloadSignals{
		GlobalPressure: p.TotalDepthRatio,
		LanePressure:   p.TotalDepthRatio,
		LaneDepth:      s.laneQueueDepth(laneID),
		QueueClosed:    s.isQueueClosedForOverload(),
		QueueFull:      s.wouldQueueFull(keyHash, laneID),
	}
}

func evaluateOverload(snap *overloadPolicySnapshot, laneID LaneID, signals OverloadSignals) OverloadEvalResult {
	keep := func(entry laneOverloadEntry) OverloadEvalResult {
		return OverloadEvalResult{
			Action:        OverloadActionKeep,
			Reason:        OverloadReasonNone,
			Class:         entry.class,
			Pressure:      signals.GlobalPressure,
			LaneDepth:     signals.LaneDepth,
			MaxDepth:      entry.maxQueueDepth,
			PolicyVersion: snap.version,
		}
	}

	if signals.QueueClosed {
		return overloadDecisionFromEntry(snap, laneID, signals, OverloadActionReject, OverloadReasonQueueClosed)
	}
	if signals.QueueFull {
		return overloadDecisionFromEntry(snap, laneID, signals, OverloadActionReject, OverloadReasonQueueFull)
	}

	if int(laneID) < 0 || int(laneID) >= len(snap.lanes) {
		return overloadDecisionFromEntry(snap, laneID, signals, OverloadActionReject, OverloadReasonGlobalPressureHigh)
	}
	entry := snap.lanes[laneID]

	if signals.LaneDepth >= entry.maxQueueDepth {
		return overloadDecisionFromEntry(snap, laneID, signals, OverloadActionReject, OverloadReasonLaneDepthExceeded)
	}

	pressure := signals.GlobalPressure

	if entry.shedAboveRatio < 1.0 && pressure >= entry.shedAboveRatio {
		switch entry.class {
		case LaneClassBestEffort:
			return overloadDecisionFromEntry(snap, laneID, signals, OverloadActionShed, OverloadReasonBestEffortShedding)
		case LaneClassBackground:
			return overloadDecisionFromEntry(snap, laneID, signals, OverloadActionShed, OverloadReasonBackgroundShedding)
		}
	}

	if entry.degradeAboveRatio < 1.0 && pressure >= entry.degradeAboveRatio {
		return overloadDecisionFromEntry(snap, laneID, signals, OverloadActionDegrade, OverloadReasonDegradePreferred)
	}

	if pressure >= entry.rejectAboveRatio {
		reason := OverloadReasonLanePressureHigh
		if pressure >= 0.90 {
			reason = OverloadReasonGlobalPressureHigh
		}
		return overloadDecisionFromEntry(snap, laneID, signals, OverloadActionReject, reason)
	}

	return keep(entry)
}

func overloadDecisionFromEntry(
	snap *overloadPolicySnapshot,
	laneID LaneID,
	signals OverloadSignals,
	action, reason string,
) OverloadEvalResult {
	entry := snap.defaultPolicy
	if int(laneID) >= 0 && int(laneID) < len(snap.lanes) {
		entry = snap.lanes[laneID]
	}
	return OverloadEvalResult{
		Action:        action,
		Reason:        reason,
		Class:         entry.class,
		Pressure:      signals.GlobalPressure,
		LaneDepth:     signals.LaneDepth,
		MaxDepth:      entry.maxQueueDepth,
		RetryAfter:    entry.retryAfter,
		MinBackoff:    entry.minBackoff,
		MaxBackoff:    entry.maxBackoff,
		Jitter:        entry.jitter,
		PolicyVersion: snap.version,
	}
}

// EvaluateOverloadForLane runs overload evaluation using the current policy snapshot.
func (s *Scheduler) EvaluateOverloadForLane(laneID LaneID, signals OverloadSignals) OverloadEvalResult {
	return evaluateOverload(s.loadOverloadPolicy(), laneID, signals)
}

// RecordOverloadDecision increments lane counters for a non-keep overload decision.
func (s *Scheduler) RecordOverloadDecision(laneID LaneID, action string) {
	if !s.Obs.EnableCounters {
		return
	}
	if int(laneID) < 0 || int(laneID) >= len(s.laneCounters) {
		return
	}
	c := &s.laneCounters[laneID]
	c.recordPressureAdmissionRejected()
	switch action {
	case OverloadActionShed:
		c.overloadShed.Add(1)
	case OverloadActionDegrade:
		c.overloadDegrade.Add(1)
	case OverloadActionReject:
		c.overloadReject.Add(1)
	}
}
