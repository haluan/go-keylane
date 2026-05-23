// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

// DefaultObservabilityConfig returns full visibility defaults.
func DefaultObservabilityConfig() ObservabilityConfig {
	return ObservabilityConfig{
		EnableStats:           true,
		EnableCounters:        true,
		EnableQueueWaitTiming: true,
		EnableRunTiming:       true,
		EnableHooks:           true,
		EnableDebugSnapshot:   true,
		LowAllocationMode:     false,
	}
}

// LowAllocationObservabilityConfig returns the low-allocation preset:
// counters and pull APIs stay available; hot-path timing and hooks are off.
func LowAllocationObservabilityConfig() ObservabilityConfig {
	return ObservabilityConfig{
		EnableStats:           true,
		EnableCounters:        true,
		EnableQueueWaitTiming: false,
		EnableRunTiming:       false,
		EnableHooks:           false,
		EnableDebugSnapshot:   true,
		LowAllocationMode:     true,
	}
}

// ResolveObservabilityConfig applies defaults and the low-allocation preset.
// When LowAllocationMode is true, the preset wins. An all-zero struct gets
// DefaultObservabilityConfig. Partial configs that only set legacy fields
// (TrackQueueWait, Hooks, SlowJobThreshold) receive default Enable* flags.
func ResolveObservabilityConfig(in ObservabilityConfig) ObservabilityConfig {
	if in.LowAllocationMode {
		return LowAllocationObservabilityConfig()
	}
	if isUnsetObservabilityConfig(in) {
		return DefaultObservabilityConfig()
	}
	if legacyOnlyObservabilityConfig(in) {
		out := DefaultObservabilityConfig()
		out.TrackQueueWait = in.TrackQueueWait
		out.SlowJobThreshold = in.SlowJobThreshold
		out.Hooks = in.Hooks
		return out
	}
	return in
}

func isUnsetObservabilityConfig(c ObservabilityConfig) bool {
	if c.LowAllocationMode {
		return false
	}
	if anyEnableFlagExplicit(c) {
		return false
	}
	return !c.TrackQueueWait && c.SlowJobThreshold == 0 &&
		c.Hooks.OnJobTiming == nil && c.Hooks.OnSlowJob == nil
}

func anyEnableFlagExplicit(c ObservabilityConfig) bool {
	return c.EnableStats || c.EnableCounters || c.EnableQueueWaitTiming ||
		c.EnableRunTiming || c.EnableHooks || c.EnableDebugSnapshot
}

func legacyOnlyObservabilityConfig(c ObservabilityConfig) bool {
	if c.LowAllocationMode {
		return false
	}
	if c.EnableStats || c.EnableCounters || c.EnableQueueWaitTiming ||
		c.EnableRunTiming || c.EnableHooks || c.EnableDebugSnapshot {
		return false
	}
	return c.TrackQueueWait || c.SlowJobThreshold > 0 ||
		c.Hooks.OnJobTiming != nil || c.Hooks.OnSlowJob != nil
}
