// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"fmt"

	"github.com/haluan/go-keylane/internal/core"
)

// LaneClass is an admission priority classification for a lane.
// It affects when new work is rejected under pressure; it is not a strict
// scheduler priority and does not reorder queued work.
//
// LaneCritical delays rejection relative to lower classes but does not mean
// unlimited capacity — critical lanes still hit MaxQueueDepth and pressure limits.
// LaneBestEffort is rejected earlier under pressure but does not mean work never
// runs — admitted best-effort jobs execute normally.
type LaneClass string

const (
	// LaneCritical protects longer under pressure; it does not mean unlimited.
	LaneCritical LaneClass = LaneClass(core.LaneClassCritical)
	LaneNormal   LaneClass = LaneClass(core.LaneClassNormal)
	// LaneBackground is lower priority than normal; rejected earlier under pressure.
	LaneBackground LaneClass = LaneClass(core.LaneClassBackground)
	// LaneBestEffort is the earliest rejection under pressure; it does not mean never runs.
	LaneBestEffort LaneClass = LaneClass(core.LaneClassBestEffort)
)

// Validate ensures the lane class is one of the supported values.
func (c LaneClass) Validate() error {
	if err := core.ValidateLaneClass(string(c)); err != nil {
		return fmt.Errorf("%w", ErrInvalidLaneClass)
	}
	return nil
}
