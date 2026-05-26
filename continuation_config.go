// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "fmt"

// DefaultContinuationMaxPending is applied when Continuation.Enabled is true and MaxPending <= 0.
const DefaultContinuationMaxPending = 256

// NormalizeContinuationConfig applies safe defaults for continuation configuration.
func NormalizeContinuationConfig(cfg *ContinuationConfig) {
	if cfg == nil || !cfg.Enabled {
		return
	}
	if cfg.MaxPending <= 0 {
		cfg.MaxPending = DefaultContinuationMaxPending
	}
}

// ValidateContinuationConfig validates continuation settings after normalization.
func ValidateContinuationConfig(cfg ContinuationConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.MaxPending < 1 {
		return fmt.Errorf("%w: MaxPending must be at least 1 when continuations are enabled", ErrInvalidContinuation)
	}
	if cfg.MaxPendingPerShard < 0 {
		return fmt.Errorf("%w: MaxPendingPerShard cannot be negative", ErrInvalidContinuation)
	}
	return nil
}
