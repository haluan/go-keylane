package core

import (
	"fmt"
	"sort"

	"github.com/haluan/go-keylane"
)

// LaneID is a compact representation of a lane.
type LaneID uint16

// LaneRegistry manages the mapping between public Lane names and internal LaneIDs.
type LaneRegistry struct {
	ids    map[string]LaneID
	names  []string
	quotas []int
}

// NewLaneRegistry creates a new LaneRegistry from a map of lane quotas.
// It returns an error if any lane name is empty or any quota is less than 1.
func NewLaneRegistry(quotas map[keylane.Lane]int) (*LaneRegistry, error) {
	if len(quotas) == 0 {
		return nil, keylane.ErrMissingLaneQuotas
	}

	// Sort lane names for deterministic LaneID assignment.
	sortedLanes := make([]string, 0, len(quotas))
	for lane := range quotas {
		if lane == "" {
			return nil, keylane.ErrInvalidLane
		}
		sortedLanes = append(sortedLanes, string(lane))
	}
	sort.Strings(sortedLanes)

	r := &LaneRegistry{
		ids:    make(map[string]LaneID, len(quotas)),
		names:  make([]string, len(quotas)),
		quotas: make([]int, len(quotas)),
	}

	for i, name := range sortedLanes {
		quota := quotas[keylane.Lane(name)]
		if quota < 1 {
			return nil, fmt.Errorf("%w: quota for lane %q must be at least 1", keylane.ErrInvalidLaneQuota, name)
		}

		id := LaneID(i)
		r.ids[name] = id
		r.names[i] = name
		r.quotas[i] = quota
	}

	return r, nil
}

// Lookup returns the LaneID for a given lane name.
func (r *LaneRegistry) Lookup(lane keylane.Lane) (LaneID, bool) {
	id, ok := r.ids[string(lane)]
	return id, ok
}

// Quota returns the quota for a given LaneID.
func (r *LaneRegistry) Quota(id LaneID) int {
	if int(id) >= len(r.quotas) {
		return 0
	}
	return r.quotas[id]
}

// Name returns the name for a given LaneID.
func (r *LaneRegistry) Name(id LaneID) string {
	if int(id) >= len(r.names) {
		return ""
	}
	return r.names[id]
}

// Len returns the number of registered lanes.
func (r *LaneRegistry) Len() int {
	return len(r.names)
}
