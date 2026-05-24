// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

// FailureEvent carries classified failure metadata for observability hooks.
type FailureEvent struct {
	Lane    Lane
	ShardID int
	Kind    FailureKind
	Err     error
}
