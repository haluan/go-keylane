// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "errors"

// StageName is a low-cardinality pipeline stage identifier for observability and failure attribution.
// Stage names must not contain tenant IDs, user IDs, raw URLs, request IDs, or arbitrary error text.
type StageName string

// Built-in stage names for common request pipeline phases.
const (
	StageValidate    StageName = "validate"
	StageAuthorize   StageName = "authorize"
	StageBusiness    StageName = "business"
	StageDBRead      StageName = "db_read"
	StageDBWrite     StageName = "db_write"
	StageExternalAPI StageName = "external_api"
	StageCacheRead   StageName = "cache_read"
	StageCacheWrite  StageName = "cache_write"
	StageResponse    StageName = "response"
)

// StageMeta holds stage identity for observations and failure attribution.
type StageMeta struct {
	// Name is the stage identifier (required, low cardinality).
	Name StageName
	// Operation is an optional low-cardinality override for observability.
	Operation string
}

// ErrEmptyStageName is returned when a stage name is empty.
var ErrEmptyStageName = errors.New("keylane: empty stage name")

// ValidateStageMeta reports whether meta has a non-empty stage name.
func ValidateStageMeta(meta StageMeta) error {
	if meta.Name == "" {
		return ErrEmptyStageName
	}
	return nil
}
