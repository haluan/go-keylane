// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "fmt"

// MaxLaneQuota is the upper bound for per-lane drain quotas in a quota policy.
const MaxLaneQuota uint32 = 1024

// QuotaPolicyInput is the scheduler-facing quota policy (lane names as strings).
type QuotaPolicyInput struct {
	DefaultQuota uint32
	LaneQuotas   map[string]uint32
}

type quotaPolicySnapshot struct {
	version      uint64
	defaultQuota uint32
	laneQuotas   []int // indexed by LaneID, len == lane registry count
}

func snapshotFromRegistry(reg *LaneRegistry) *quotaPolicySnapshot {
	laneQuotas := make([]int, reg.Len())
	for i := 0; i < reg.Len(); i++ {
		laneQuotas[i] = reg.Quota(LaneID(i))
	}
	return &quotaPolicySnapshot{
		version:      0,
		defaultQuota: 1,
		laneQuotas:   laneQuotas,
	}
}

func buildQuotaPolicySnapshot(reg *LaneRegistry, policy QuotaPolicyInput) (*quotaPolicySnapshot, error) {
	if policy.DefaultQuota < 1 {
		return nil, fmt.Errorf("%w: default quota must be at least 1", ErrInvalidLaneQuota)
	}
	if policy.DefaultQuota > MaxLaneQuota {
		return nil, fmt.Errorf("%w: default quota exceeds maximum %d", ErrQuotaTooLarge, MaxLaneQuota)
	}

	for lane, q := range policy.LaneQuotas {
		if lane == "" {
			return nil, ErrInvalidLane
		}
		if _, ok := reg.Lookup(lane); !ok {
			return nil, fmt.Errorf("%w: unknown lane %q", ErrInvalidQuotaPolicy, lane)
		}
		if q < 1 {
			return nil, fmt.Errorf("%w: quota for lane %q must be at least 1", ErrInvalidLaneQuota, lane)
		}
		if q > MaxLaneQuota {
			return nil, fmt.Errorf("%w: quota for lane %q exceeds maximum %d", ErrQuotaTooLarge, lane, MaxLaneQuota)
		}
	}

	laneQuotas := make([]int, reg.Len())
	for i := 0; i < reg.Len(); i++ {
		name := reg.Name(LaneID(i))
		if q, ok := policy.LaneQuotas[name]; ok {
			laneQuotas[i] = int(q)
		} else {
			laneQuotas[i] = int(policy.DefaultQuota)
		}
	}

	return &quotaPolicySnapshot{
		defaultQuota: policy.DefaultQuota,
		laneQuotas:   laneQuotas,
	}, nil
}

func (s *Scheduler) initQuotaPolicy(reg *LaneRegistry) {
	s.quotaPolicy.Store(snapshotFromRegistry(reg))
}

func (s *Scheduler) loadQuotaPolicy() *quotaPolicySnapshot {
	return s.quotaPolicy.Load()
}

func (s *Scheduler) publishQuotaPolicy(snap *quotaPolicySnapshot) uint64 {
	s.quotaMu.Lock()
	defer s.quotaMu.Unlock()
	v := s.quotaVersion.Add(1)
	snap.version = v
	s.quotaPolicy.Store(snap)
	return v
}

// UpdateQuotaPolicy validates and atomically publishes a new quota policy snapshot.
// Updates are rejected when the scheduler is stopping or stopped.
// Allowed before Start and while running.
func (s *Scheduler) UpdateQuotaPolicy(policy QuotaPolicyInput) (uint64, error) {
	snap, err := buildQuotaPolicySnapshot(s.laneReg, policy)
	if err != nil {
		return 0, err
	}

	s.mu.RLock()
	state := s.state
	s.mu.RUnlock()
	if state == stateStopping || state == stateStopped {
		return 0, ErrStopped
	}

	return s.publishQuotaPolicy(snap), nil
}

// CurrentQuotaPolicyView returns a copy of the active policy for inspection.
func (s *Scheduler) CurrentQuotaPolicyView() (version uint64, defaultQuota uint32, laneQuotas map[string]uint32) {
	snap := s.loadQuotaPolicy()
	laneQuotas = make(map[string]uint32, s.laneReg.Len())
	for i := 0; i < s.laneReg.Len(); i++ {
		laneQuotas[s.laneReg.Name(LaneID(i))] = uint32(snap.laneQuotas[i])
	}
	return snap.version, snap.defaultQuota, laneQuotas
}
