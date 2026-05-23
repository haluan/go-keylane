// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import "errors"

var (
	// ErrMissingKeyFunc indicates Middleware was created without KeyFunc.
	ErrMissingKeyFunc = errors.New("httpkeylane: missing KeyFunc")
	// ErrMissingLaneFunc indicates Middleware was created without LaneFunc.
	ErrMissingLaneFunc = errors.New("httpkeylane: missing LaneFunc")
)
