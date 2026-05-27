// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

// ObservabilityStability classifies observability surfaces for compatibility guarantees.
type ObservabilityStability string

const (
	// ObservabilityStable marks surfaces safe for dashboards and alerts within v0.8.
	ObservabilityStable ObservabilityStability = "stable"
	// ObservabilityExperimental marks surfaces that may change before v1.0.
	ObservabilityExperimental ObservabilityStability = "experimental"
	// ObservabilityInternal marks surfaces not part of the user-facing contract.
	ObservabilityInternal ObservabilityStability = "internal"
)

// MetricDescriptor describes a Prometheus-style metric family exposed by stable adapters.
type MetricDescriptor struct {
	Name        string
	Description string
	Labels      []LabelDescriptor
	Stability   ObservabilityStability
}

// LabelDescriptor describes a metric label dimension and cardinality expectations.
type LabelDescriptor struct {
	Name        string
	Cardinality string // "low" or "bounded"
	Sensitive   bool
	Stability   ObservabilityStability
}

// AllowedDefaultMetricLabelNames returns label names permitted on stable prometheus adapter metrics.
func AllowedDefaultMetricLabelNames() []string {
	return []string{
		"scheduler",
		"lane",
		"shard_id",
		"action",
		"reason",
		"scope",
		"quantile", // prometheus summary quantile label
	}
}

// ForbiddenMetricLabelNames returns label names that must not appear on stable metrics by default.
func ForbiddenMetricLabelNames() []string {
	return []string{
		"key",
		"raw_key",
		"key_hash",
		"request_id",
		"idempotency_key",
		"customer_id",
		"user_id",
		"tenant_id",
		"backend_name",
		"route_pattern",
		"http_path",
		"error_message",
	}
}

// StableMetricDescriptors returns the canonical stable metric inventory for the prometheus adapter.
// Names match metrics/prometheus collector output (keylane_ prefix).
func StableMetricDescriptors() []MetricDescriptor {
	sched := labelSched()
	lane := labelLane()
	shard := labelShard()
	inflight := labelInflight()
	scaleRec := labelScaleRecommended()
	perKey := labelPerKeyDecision()

	return []MetricDescriptor{
		metric("keylane_jobs_submitted_total", "Cumulative enqueue attempts per lane since queue start.", lane),
		metric("keylane_jobs_completed_total", "Cumulative completed jobs per lane since queue start.", lane),
		metric("keylane_jobs_failed_total", "Cumulative failed jobs per lane since queue start.", lane),
		metric("keylane_queue_full_total", "Cumulative queue-full rejections per lane since queue start.", lane),
		metric("keylane_admission_rejected_total", "Cumulative pressure-based admission rejections per lane since queue start.", lane),
		metric("keylane_admission_shed_total", "Cumulative overload shed events per lane since queue start.", lane),
		metric("keylane_lane_depth", "Queued jobs per lane across all shards.", lane),
		metric("keylane_shard_depth", "Queued jobs per shard.", shard),
		metric("keylane_shard_queue_depth", "Queued jobs per shard (spec alias of shard_depth).", shard),
		metric("keylane_inflight_jobs", "Jobs currently executing.", inflight),
		metric("keylane_queue_wait_seconds", "Queue wait duration in seconds from cumulative scheduler stats at scrape (summary).", lane),
		metric("keylane_run_duration_seconds", "Run duration in seconds from cumulative scheduler stats at scrape (summary).", lane),
		metric("keylane_pressure_ratio", "Queued depth ratio (TotalDepth / TotalCapacity).", sched),
		metric("keylane_scale_pressure_ratio", "Composite pressure ratio for autoscaling signals.", sched),
		metric("keylane_scale_recommended", "Scale-out recommendation (1=recommended, 0=not).", scaleRec),
		metric("keylane_queue_depth_ratio", "Queue depth ratio component (scheduler aggregate).", sched),
		metric("keylane_queue_wait_max_seconds", "Max observed queue wait in seconds (scheduler aggregate).", sched),
		metric("keylane_admission_throttled_total", "Cumulative per-key throttle decisions since queue start.", sched),
		metric("keylane_worker_busy_ratio", "Worker busy ratio (in-flight / workers).", sched),
		metric("keylane_hot_shard_count", "Count of hot shards.", sched),
		metric("keylane_hot_key_candidate_count", "Bounded hot key candidate count across hot shards.", sched),
		metric("keylane_localized_hot_key_ratio", "Localized hot key pressure ratio.", sched),
		metric("keylane_hot_key_pressure_ratio", "Max localized hot key pressure ratio from scale signal.", sched),
		metric("keylane_hot_key_rejected_total", "Cumulative hot key reject observations since queue start.", sched),
		metric("keylane_per_key_admission_decisions_total", "Cumulative per-key admission decisions by action and reason.", perKey),
		metric("keylane_per_key_mitigation_actions_total", "Cumulative per-key mitigation actions (alias of per_key_admission_decisions_total).", perKey),
		metric("keylane_shard_pressure_ratio", "Global queue depth ratio from shard pressure summary.", sched),
	}
}

// ExperimentalMetricPatterns returns hook-adapter metric patterns documented for v0.7+ but not registered by core.
func ExperimentalMetricPatterns() []MetricDescriptor {
	backendLane := []LabelDescriptor{
		label("backend_resource", "bounded", false),
		label("backend_lane", "bounded", false),
	}
	pipeline := []LabelDescriptor{
		label("transport", "low", false),
		label("operation", "low", false),
		label("lane", "low", false),
		label("stage", "low", false),
	}
	return []MetricDescriptor{
		{Name: "keylane_pipeline_stage_started_total", Stability: ObservabilityExperimental, Labels: pipeline},
		{Name: "keylane_pipeline_stage_completed_total", Stability: ObservabilityExperimental, Labels: append(pipeline, label("outcome", "low", false))},
		{Name: "keylane_pipeline_stage_failed_total", Stability: ObservabilityExperimental, Labels: append(pipeline, label("failure_kind", "low", false))},
		{Name: "keylane_pipeline_stage_duration_seconds", Stability: ObservabilityExperimental, Labels: pipeline},
		{Name: "keylane_pipeline_continuation_yielded_total", Stability: ObservabilityExperimental, Labels: pipeline},
		{Name: "keylane_pipeline_continuation_resumed_total", Stability: ObservabilityExperimental, Labels: pipeline},
		{Name: "keylane_pipeline_continuation_completed_total", Stability: ObservabilityExperimental, Labels: pipeline},
		{Name: "keylane_pipeline_continuation_failed_total", Stability: ObservabilityExperimental, Labels: append(pipeline, label("failure_kind", "low", false))},
		{Name: "keylane_pipeline_continuation_cancelled_total", Stability: ObservabilityExperimental, Labels: pipeline},
		{Name: "keylane_pipeline_continuation_late_total", Stability: ObservabilityExperimental, Labels: pipeline},
		{Name: "keylane_backend_admission_total", Stability: ObservabilityExperimental, Labels: append(backendLane, label("stage", "low", false), label("backend_reason", "low", false))},
		{Name: "keylane_backend_admission_accepted_total", Stability: ObservabilityExperimental, Labels: append(backendLane, label("stage", "low", false))},
		{Name: "keylane_backend_admission_rejected_total", Stability: ObservabilityExperimental, Labels: append(backendLane, label("stage", "low", false), label("backend_reason", "low", false))},
		{Name: "keylane_backend_released_total", Stability: ObservabilityExperimental, Labels: append(backendLane, label("stage", "low", false))},
		{Name: "keylane_backend_inflight", Stability: ObservabilityExperimental, Labels: backendLane},
		{Name: "keylane_backend_capacity", Stability: ObservabilityExperimental, Labels: backendLane},
		{Name: "keylane_backend_held_duration_seconds", Stability: ObservabilityExperimental, Labels: append(backendLane, label("stage", "low", false))},
		{Name: "keylane_backend_pressure_ratio", Stability: ObservabilityExperimental, Labels: backendLane},
		{Name: "keylane_backend_in_use", Stability: ObservabilityExperimental, Labels: backendLane},
		{Name: "keylane_backend_wait_total", Stability: ObservabilityExperimental, Labels: backendLane},
		{Name: "keylane_backend_wait_duration_seconds", Stability: ObservabilityExperimental, Labels: backendLane},
		{Name: "keylane_backend_saturated", Stability: ObservabilityExperimental, Labels: backendLane},
	}
}

func metric(name, desc string, labels []LabelDescriptor) MetricDescriptor {
	return MetricDescriptor{
		Name:        name,
		Description: desc,
		Labels:      labels,
		Stability:   ObservabilityStable,
	}
}

func label(name, cardinality string, sensitive bool) LabelDescriptor {
	return LabelDescriptor{
		Name:        name,
		Cardinality: cardinality,
		Sensitive:   sensitive,
		Stability:   ObservabilityStable,
	}
}

func labelSched() []LabelDescriptor { return []LabelDescriptor{label("scheduler", "low", false)} }
func labelLane() []LabelDescriptor {
	return []LabelDescriptor{label("scheduler", "low", false), label("lane", "low", false)}
}
func labelShard() []LabelDescriptor {
	return []LabelDescriptor{label("scheduler", "low", false), label("shard_id", "low", false)}
}
func labelInflight() []LabelDescriptor {
	return []LabelDescriptor{
		label("scheduler", "low", false),
		label("shard_id", "low", false),
		label("lane", "low", false),
	}
}
func labelScaleRecommended() []LabelDescriptor {
	return []LabelDescriptor{
		label("scheduler", "low", false),
		label("reason", "low", false),
		label("scope", "low", false),
	}
}
func labelPerKeyDecision() []LabelDescriptor {
	return []LabelDescriptor{
		label("scheduler", "low", false),
		label("action", "low", false),
		label("reason", "low", false),
	}
}
