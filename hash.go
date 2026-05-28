// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "github.com/haluan/go-keylane/internal/core"

// HashKey returns a stable 64-bit FNV-1a hash of key for routing and redacted observability.
func HashKey(key string) uint64 {
	return core.HashKey(key)
}
