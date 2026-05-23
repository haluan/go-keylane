// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"net/http"
	"strings"

	"github.com/haluan/go-keylane"
)

// Common lane names for MethodLaneMapper.
var (
	LaneRead  = keylane.Lane("read")
	LaneWrite = keylane.Lane("write")
)

var defaultMethodLanes = map[string]keylane.Lane{
	http.MethodGet:     LaneRead,
	http.MethodHead:    LaneRead,
	http.MethodOptions: LaneRead,
	http.MethodPost:    LaneWrite,
	http.MethodPut:     LaneWrite,
	http.MethodPatch:   LaneWrite,
	http.MethodDelete:  LaneWrite,
}

// StaticLane returns a LaneFunc that always returns the configured lane.
func StaticLane(lane keylane.Lane) LaneFunc {
	return func(*http.Request) keylane.Lane {
		return lane
	}
}

// MethodLaneMapper maps standard HTTP methods to read/write lanes.
// GET, HEAD, and OPTIONS map to LaneRead; POST, PUT, PATCH, and DELETE map to LaneWrite.
// Unknown methods map to LaneWrite (treated as potentially mutating).
func MethodLaneMapper() LaneFunc {
	return MethodLaneMapperWith(defaultMethodLanes, LaneWrite)
}

// MethodLaneMapperWith returns a LaneFunc that maps HTTP methods using the provided table.
// Method keys are normalized to uppercase at construction time. The mapping is copied
// and will not change if the caller mutates the original map. Unknown methods return fallback.
func MethodLaneMapperWith(mapping map[string]keylane.Lane, fallback keylane.Lane) LaneFunc {
	copied := make(map[string]keylane.Lane, len(mapping))
	for method, lane := range mapping {
		copied[strings.ToUpper(strings.TrimSpace(method))] = lane
	}
	return func(r *http.Request) keylane.Lane {
		method := strings.ToUpper(r.Method)
		if lane, ok := copied[method]; ok {
			return lane
		}
		return fallback
	}
}
