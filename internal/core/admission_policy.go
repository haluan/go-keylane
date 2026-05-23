// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "fmt"

// Lane class names (admission priority, not strict scheduling priority).
// Critical delays rejection but does not mean unlimited. Best-effort is shed
// earlier under pressure but does not mean work never runs.
const (
	LaneClassCritical   = "critical"
	LaneClassNormal     = "normal"
	LaneClassBackground = "background"
	LaneClassBestEffort = "best_effort"
)

// Admission rejection reasons.
const (
	AdmissionReasonPressureAboveThreshold = "pressure_above_lane_threshold"
	AdmissionReasonLaneQueueDepthExceeded = "lane_queue_depth_exceeded"
)

// LanePolicyInput is the scheduler-facing per-lane admission policy entry.
type LanePolicyInput struct {
	Lane             string
	Class            string
	RejectAboveRatio float64
	MaxQueueDepth    uint32 // per-lane cap; protects scheduler capacity during overload
}

// AdmissionPolicyInput is the scheduler-facing admission policy.
type AdmissionPolicyInput struct {
	DefaultClass            string
	DefaultRejectAboveRatio float64
	DefaultMaxQueueDepth    uint32
	Lanes                   []LanePolicyInput
}

type laneAdmissionEntry struct {
	class            string
	rejectAboveRatio float64
	maxQueueDepth    uint32
}

type admissionPolicySnapshot struct {
	version                 uint64
	defaultClass            string
	defaultRejectAboveRatio float64
	defaultMaxQueueDepth    uint32
	lanes                   []laneAdmissionEntry
}

// AdmissionEvalResult is the outcome of evaluating admission for one lane.
type AdmissionEvalResult struct {
	Admit     bool
	Reason    string
	Class     string
	Threshold float64
	MaxDepth  uint32
}

// ValidateLaneClass returns an error if class is not a supported lane class name.
func ValidateLaneClass(class string) error {
	switch class {
	case LaneClassCritical, LaneClassNormal, LaneClassBackground, LaneClassBestEffort:
		return nil
	default:
		return fmt.Errorf("%w: invalid lane class %q", ErrInvalidLaneClass, class)
	}
}

func validateRejectRatio(ratio float64) error {
	if ratio < 0 || ratio > 1 {
		return fmt.Errorf("%w: RejectAboveRatio must be between 0.0 and 1.0", ErrInvalidAdmissionPolicy)
	}
	return nil
}

func buildAdmissionPolicySnapshot(reg *LaneRegistry, policy AdmissionPolicyInput) (*admissionPolicySnapshot, error) {
	if err := ValidateLaneClass(policy.DefaultClass); err != nil {
		return nil, err
	}
	if err := validateRejectRatio(policy.DefaultRejectAboveRatio); err != nil {
		return nil, err
	}
	if policy.DefaultMaxQueueDepth < 1 {
		return nil, fmt.Errorf("%w: DefaultMaxQueueDepth must be at least 1", ErrInvalidAdmissionPolicy)
	}

	seen := make(map[string]struct{}, len(policy.Lanes))
	for _, lp := range policy.Lanes {
		if lp.Lane == "" {
			return nil, ErrInvalidLane
		}
		if _, ok := reg.Lookup(lp.Lane); !ok {
			return nil, fmt.Errorf("%w: unknown lane %q", ErrInvalidAdmissionPolicy, lp.Lane)
		}
		if _, dup := seen[lp.Lane]; dup {
			return nil, fmt.Errorf("%w: duplicate lane policy %q", ErrInvalidAdmissionPolicy, lp.Lane)
		}
		seen[lp.Lane] = struct{}{}
		if err := ValidateLaneClass(lp.Class); err != nil {
			return nil, err
		}
		if err := validateRejectRatio(lp.RejectAboveRatio); err != nil {
			return nil, err
		}
		if lp.MaxQueueDepth < 1 {
			return nil, fmt.Errorf("%w: MaxQueueDepth for lane %q must be at least 1", ErrInvalidAdmissionPolicy, lp.Lane)
		}
	}

	override := make(map[string]LanePolicyInput, len(policy.Lanes))
	for _, lp := range policy.Lanes {
		override[lp.Lane] = lp
	}

	lanes := make([]laneAdmissionEntry, reg.Len())
	for i := 0; i < reg.Len(); i++ {
		name := reg.Name(LaneID(i))
		entry := laneAdmissionEntry{
			class:            policy.DefaultClass,
			rejectAboveRatio: policy.DefaultRejectAboveRatio,
			maxQueueDepth:    policy.DefaultMaxQueueDepth,
		}
		if lp, ok := override[name]; ok {
			entry.class = lp.Class
			entry.rejectAboveRatio = lp.RejectAboveRatio
			entry.maxQueueDepth = lp.MaxQueueDepth
		}
		lanes[i] = entry
	}

	return &admissionPolicySnapshot{
		defaultClass:            policy.DefaultClass,
		defaultRejectAboveRatio: policy.DefaultRejectAboveRatio,
		defaultMaxQueueDepth:    policy.DefaultMaxQueueDepth,
		lanes:                   lanes,
	}, nil
}

func defaultAdmissionPolicy(shardCount, queueSizePerLane int) AdmissionPolicyInput {
	maxDepth := uint32(shardCount * queueSizePerLane)
	if maxDepth < 1 {
		maxDepth = 1
	}
	return AdmissionPolicyInput{
		DefaultClass:            LaneClassNormal,
		DefaultRejectAboveRatio: 0.90,
		DefaultMaxQueueDepth:    maxDepth,
	}
}

func (s *Scheduler) initAdmissionPolicy(reg *LaneRegistry, shardCount, queueSizePerLane int) {
	policy := defaultAdmissionPolicy(shardCount, queueSizePerLane)
	snap, err := buildAdmissionPolicySnapshot(reg, policy)
	if err != nil {
		panic("keylane: default admission policy: " + err.Error())
	}
	s.admissionPolicy.Store(snap)
}

func (s *Scheduler) loadAdmissionPolicy() *admissionPolicySnapshot {
	return s.admissionPolicy.Load()
}

func (s *Scheduler) publishAdmissionPolicy(snap *admissionPolicySnapshot) uint64 {
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	v := s.admissionVersion.Add(1)
	snap.version = v
	s.admissionPolicy.Store(snap)
	return v
}

// UpdateAdmissionPolicy validates and atomically publishes a new admission policy snapshot.
func (s *Scheduler) UpdateAdmissionPolicy(policy AdmissionPolicyInput) (uint64, error) {
	snap, err := buildAdmissionPolicySnapshot(s.laneReg, policy)
	if err != nil {
		return 0, err
	}

	s.mu.RLock()
	state := s.state
	s.mu.RUnlock()
	if state == stateStopping || state == stateStopped {
		return 0, ErrStopped
	}

	return s.publishAdmissionPolicy(snap), nil
}

// CurrentAdmissionPolicyView returns a copy of the active admission policy.
func (s *Scheduler) CurrentAdmissionPolicyView() (version uint64, policy AdmissionPolicyInput) {
	snap := s.loadAdmissionPolicy()
	policy = AdmissionPolicyInput{
		DefaultClass:            snap.defaultClass,
		DefaultRejectAboveRatio: snap.defaultRejectAboveRatio,
		DefaultMaxQueueDepth:    snap.defaultMaxQueueDepth,
		Lanes:                   make([]LanePolicyInput, 0, len(snap.lanes)),
	}
	for i, entry := range snap.lanes {
		policy.Lanes = append(policy.Lanes, LanePolicyInput{
			Lane:             s.laneReg.Name(LaneID(i)),
			Class:            entry.class,
			RejectAboveRatio: entry.rejectAboveRatio,
			MaxQueueDepth:    entry.maxQueueDepth,
		})
	}
	return snap.version, policy
}

// LaneQueueDepth returns the total queued job count for a lane across all shards.
func (s *Scheduler) LaneQueueDepth(laneID LaneID) uint32 {
	return s.laneQueueDepth(laneID)
}

func (s *Scheduler) laneQueueDepth(laneID LaneID) uint32 {
	var total int
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.Lock()
		total += shard.laneDepthLocked(laneID)
		shard.mu.Unlock()
	}
	if total < 0 {
		return 0
	}
	return uint32(total)
}

func evaluateAdmission(snap *admissionPolicySnapshot, laneID LaneID, pressure float64, depth uint32) AdmissionEvalResult {
	if int(laneID) < 0 || int(laneID) >= len(snap.lanes) {
		return AdmissionEvalResult{Admit: false, Reason: AdmissionReasonPressureAboveThreshold, Class: snap.defaultClass}
	}
	entry := snap.lanes[laneID]
	if depth >= entry.maxQueueDepth {
		return AdmissionEvalResult{
			Admit:     false,
			Reason:    AdmissionReasonLaneQueueDepthExceeded,
			Class:     entry.class,
			Threshold: entry.rejectAboveRatio,
			MaxDepth:  entry.maxQueueDepth,
		}
	}
	if pressure >= entry.rejectAboveRatio {
		return AdmissionEvalResult{
			Admit:     false,
			Reason:    AdmissionReasonPressureAboveThreshold,
			Class:     entry.class,
			Threshold: entry.rejectAboveRatio,
			MaxDepth:  entry.maxQueueDepth,
		}
	}
	return AdmissionEvalResult{
		Admit:     true,
		Class:     entry.class,
		Threshold: entry.rejectAboveRatio,
		MaxDepth:  entry.maxQueueDepth,
	}
}

// EvaluateAdmissionForLane runs admission using the current policy snapshot.
func (s *Scheduler) EvaluateAdmissionForLane(laneID LaneID, pressure float64, depth uint32) AdmissionEvalResult {
	return evaluateAdmission(s.loadAdmissionPolicy(), laneID, pressure, depth)
}
