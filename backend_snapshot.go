// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

// BackendResourceSnapshot reports pressure for one configured backend resource.
type BackendResourceSnapshot struct {
	Resource BackendResourceName
	Lanes    []BackendLaneSnapshot
}

// BackendLaneSnapshot reports in-flight pressure for one backend lane.
type BackendLaneSnapshot struct {
	Lane      BackendLane
	InFlight  int
	Capacity  int
	Queued    int
	Saturated bool
}

func (c *backendCoordinator) snapshot() []BackendResourceSnapshot {
	if !c.enabled || len(c.resources) == 0 {
		return nil
	}
	out := make([]BackendResourceSnapshot, 0, len(c.resources))
	for res, rs := range c.resources {
		snap := BackendResourceSnapshot{Resource: res}
		snap.Lanes = make([]BackendLaneSnapshot, 0, len(rs.lanes))
		for lane, ls := range rs.lanes {
			ls.mu.Lock()
			inflight := ls.inflight
			queued := ls.queued
			capacity := ls.policy.MaxInFlight
			ls.mu.Unlock()
			snap.Lanes = append(snap.Lanes, BackendLaneSnapshot{
				Lane:      lane,
				InFlight:  inflight,
				Capacity:  capacity,
				Queued:    queued,
				Saturated: inflight >= capacity,
			})
		}
		out = append(out, snap)
	}
	return out
}
