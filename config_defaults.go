// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"fmt"
	"sort"
	"strconv"
)

// SafetyMode classifies which default policy ExplainDefaults applies when documenting entries.
type SafetyMode string

const (
	// SafetyModeProduction documents production-safe default expectations.
	SafetyModeProduction SafetyMode = "production"
	// SafetyModeDevelopment documents the same inspection paths with development-oriented notes.
	SafetyModeDevelopment SafetyMode = "development"
)

const (
	defaultStabilityStable       = "stable"
	defaultStabilityExperimental = "experimental"
)

// DefaultEntry describes one configuration default or effective gate value.
type DefaultEntry struct {
	Path      string
	Value     string
	Reason    string
	Stability string
}

// DefaultReport is the outcome of ExplainDefaults.
type DefaultReport struct {
	Version  ConfigVersion
	Mode     SafetyMode
	Defaults []DefaultEntry
	Warnings []ValidationIssue
}

// ProductionDefaults returns a conservative, bounded configuration suitable for production evaluation.
// Risky subsystems (retry, continuations, backend coordination) remain disabled; observability uses
// the low-allocation preset. Call ValidateConfig before New in production deployments.
func ProductionDefaults() Config {
	return Config{
		ShardCount:       8,
		WorkerCount:      4,
		QueueSizePerLane: 1000,
		LaneQuotas: map[Lane]int{
			"default": 2,
		},
		Observability: LowAllocationObservabilityConfig(),
	}
}

// ExplainDefaults returns a structured report of effective gates and validation warnings for cfg.
func ExplainDefaults(cfg Config) DefaultReport {
	return ExplainDefaultsWithMode(cfg, SafetyModeProduction)
}

// ExplainDefaultsWithMode returns ExplainDefaults tagged with the given safety mode.
func ExplainDefaultsWithMode(cfg Config, mode SafetyMode) DefaultReport {
	report := DefaultReport{
		Version: ConfigVersionV1,
		Mode:    mode,
	}
	norm := cfg
	normalizeConfigInPlace(&norm)
	obs := ResolveObservabilityConfig(norm.Observability)
	report.Defaults = buildDefaultEntries(cfg, norm, obs, mode)
	val := ValidateConfig(cfg)
	report.Warnings = validationWarningsFromReport(val)
	sortDefaultEntries(report.Defaults)
	return report
}

func buildDefaultEntries(raw, norm Config, obs ObservabilityConfig, mode SafetyMode) []DefaultEntry {
	var entries []DefaultEntry
	add := func(path, value, reason, stability string) {
		entries = append(entries, DefaultEntry{
			Path: path, Value: value, Reason: reason, Stability: stability,
		})
	}

	add("ShardCount", strconv.Itoa(norm.ShardCount), "core scheduler shard routing", defaultStabilityStable)
	add("WorkerCount", strconv.Itoa(norm.WorkerCount), "worker pool size", defaultStabilityStable)
	add("QueueSizePerLane", strconv.Itoa(norm.QueueSizePerLane), "bounded per-lane queue capacity", defaultStabilityStable)
	add("LaneQuotas", fmt.Sprintf("%d lanes", len(norm.LaneQuotas)), "lane quota policy", defaultStabilityStable)

	add("Retry.Enabled", boolString(norm.Retry.Enabled),
		"retry is opt-in; disabled by default in production", defaultStabilityStable)
	add("Continuation.Enabled", boolString(norm.Continuation.Enabled),
		"non-blocking continuations are experimental and opt-in", defaultStabilityExperimental)
	add("BackendResources.Enabled", boolString(norm.BackendResources.Enabled),
		"backend resource coordination is experimental and opt-in", defaultStabilityExperimental)
	add("BackendResources.PressureProviders", strconv.Itoa(len(norm.BackendResources.PressureProviders)),
		"pressure adapters are observational unless the application gates on Saturated", defaultStabilityExperimental)

	add("HotKey.Enabled", boolString(norm.HotKey.Enabled),
		"hot key tracking is opt-in; zero HotKey disables tracking", defaultStabilityStable)
	add("HotKey.ExposeRawKey", boolString(norm.HotKey.ExposeRawKey),
		"raw key exposure in snapshots is opt-in", defaultStabilityStable)
	add("PerKeyAdmission.Enabled", boolString(norm.PerKeyAdmission.Enabled),
		"per-key admission mitigation is opt-in", defaultStabilityStable)
	add("ShardPressure.Enabled", boolString(norm.ShardPressure.Enabled),
		"shard pressure diagnostics are opt-in", defaultStabilityStable)
	add("AutoscalingSignal.Enabled", boolString(norm.AutoscalingSignal.Enabled),
		"autoscaling signals are observational only (no built-in autoscaler)", defaultStabilityStable)
	add("AdaptiveQuota.Config.Enabled", boolString(norm.AdaptiveQuota.Config.Enabled),
		"adaptive quota controller is opt-in", defaultStabilityStable)

	add("Observability.LowAllocationMode", boolString(obs.LowAllocationMode),
		"low-allocation preset disables hot-path hooks and timing", defaultStabilityStable)
	add("Observability.EnableHooks", boolString(obs.EnableHooks),
		"hooks are opt-in on the hot path", defaultStabilityStable)
	add("Observability.EnableQueueWaitTiming", boolString(obs.EnableQueueWaitTiming),
		"queue-wait timing on workers is opt-in", defaultStabilityStable)
	add("Observability.EnableRunTiming", boolString(obs.EnableRunTiming),
		"run-duration timing on workers is opt-in", defaultStabilityStable)
	add("Observability.EnableDebugSnapshot", boolString(obs.EnableDebugSnapshot),
		"debug snapshots are pull/diagnostic APIs, not per-submit hot path", defaultStabilityStable)
	add("Observability.EnableCounters", boolString(obs.EnableCounters),
		"cumulative counters remain available in low-allocation mode", defaultStabilityStable)

	if isUnsetObservabilityConfig(raw.Observability) {
		add("Observability.resolved", "DefaultObservabilityConfig",
			"unset Observability resolves to full visibility defaults at New; prefer LowAllocationObservabilityConfig for production",
			defaultStabilityStable)
	}

	if mode == SafetyModeDevelopment {
		add("SafetyMode", string(mode),
			"development mode uses the same gates; enable subsystems explicitly for local experiments",
			defaultStabilityStable)
	}

	return entries
}

func boolString(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func sortDefaultEntries(entries []DefaultEntry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Path != entries[j].Path {
			return entries[i].Path < entries[j].Path
		}
		return entries[i].Value < entries[j].Value
	})
}
