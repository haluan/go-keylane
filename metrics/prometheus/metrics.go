// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package prometheus

import (
	prom "github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "keylane"
)

var (
	labelScheduler = []string{"scheduler"}
	labelLane      = []string{"scheduler", "lane"}
	labelShard     = []string{"scheduler", "shard_id"}

	descJobsSubmitted = prom.NewDesc(
		prom.BuildFQName(namespace, "", "jobs_submitted_total"),
		"Cumulative enqueue attempts per lane since queue start.",
		labelLane, nil,
	)
	descJobsCompleted = prom.NewDesc(
		prom.BuildFQName(namespace, "", "jobs_completed_total"),
		"Cumulative completed jobs per lane since queue start.",
		labelLane, nil,
	)
	descJobsFailed = prom.NewDesc(
		prom.BuildFQName(namespace, "", "jobs_failed_total"),
		"Cumulative failed jobs per lane since queue start.",
		labelLane, nil,
	)
	descQueueFull = prom.NewDesc(
		prom.BuildFQName(namespace, "", "queue_full_total"),
		"Cumulative queue-full rejections per lane since queue start.",
		labelLane, nil,
	)
	descAdmissionRejected = prom.NewDesc(
		prom.BuildFQName(namespace, "", "admission_rejected_total"),
		"Cumulative pressure-based admission rejections per lane since queue start.",
		labelLane, nil,
	)
	descAdmissionShed = prom.NewDesc(
		prom.BuildFQName(namespace, "", "admission_shed_total"),
		"Cumulative overload shed events per lane since queue start.",
		labelLane, nil,
	)
	descLaneDepth = prom.NewDesc(
		prom.BuildFQName(namespace, "", "lane_depth"),
		"Queued jobs per lane across all shards.",
		labelLane, nil,
	)
	descShardDepth = prom.NewDesc(
		prom.BuildFQName(namespace, "", "shard_depth"),
		"Queued jobs per shard.",
		labelShard, nil,
	)
	descShardQueueDepth = prom.NewDesc(
		prom.BuildFQName(namespace, "", "shard_queue_depth"),
		"Queued jobs per shard.",
		labelShard, nil,
	)
	descInflight = prom.NewDesc(
		prom.BuildFQName(namespace, "", "inflight_jobs"),
		"Jobs currently executing.",
		[]string{"scheduler", "shard_id", "lane"}, nil,
	)
	descQueueWait = prom.NewDesc(
		prom.BuildFQName(namespace, "", "queue_wait_seconds"),
		"Queue wait duration in seconds from cumulative scheduler stats at scrape (summary).",
		labelLane, nil,
	)
	descRunDuration = prom.NewDesc(
		prom.BuildFQName(namespace, "", "run_duration_seconds"),
		"Run duration in seconds from cumulative scheduler stats at scrape (summary).",
		labelLane, nil,
	)
	descPressureRatio = prom.NewDesc(
		prom.BuildFQName(namespace, "", "pressure_ratio"),
		"Queued depth ratio (TotalDepth / TotalCapacity).",
		labelScheduler, nil,
	)

	labelScaleRecommended = []string{"scheduler", "reason", "scope"}
	labelPerKeyDecision   = []string{"scheduler", "action", "reason"}

	descScalePressureRatio = prom.NewDesc(
		prom.BuildFQName(namespace, "", "scale_pressure_ratio"),
		"Composite pressure ratio for autoscaling signals.",
		labelScheduler, nil,
	)
	descScaleRecommended = prom.NewDesc(
		prom.BuildFQName(namespace, "", "scale_recommended"),
		"Scale-out recommendation (1=recommended, 0=not).",
		labelScaleRecommended, nil,
	)
	descSignalQueueDepthRatio = prom.NewDesc(
		prom.BuildFQName(namespace, "", "queue_depth_ratio"),
		"Queue depth ratio component (scheduler aggregate).",
		labelScheduler, nil,
	)
	descSignalQueueWaitMaxSeconds = prom.NewDesc(
		prom.BuildFQName(namespace, "", "queue_wait_max_seconds"),
		"Max observed queue wait in seconds (scheduler aggregate).",
		labelScheduler, nil,
	)
	descSignalAdmissionThrottledTotal = prom.NewDesc(
		prom.BuildFQName(namespace, "", "admission_throttled_total"),
		"Cumulative per-key throttle decisions since queue start.",
		labelScheduler, nil,
	)
	descSignalWorkerBusyRatio = prom.NewDesc(
		prom.BuildFQName(namespace, "", "worker_busy_ratio"),
		"Worker busy ratio (in-flight / workers).",
		labelScheduler, nil,
	)
	descSignalHotShardCount = prom.NewDesc(
		prom.BuildFQName(namespace, "", "hot_shard_count"),
		"Count of hot shards.",
		labelScheduler, nil,
	)
	descSignalHotKeyCandidateCount = prom.NewDesc(
		prom.BuildFQName(namespace, "", "hot_key_candidate_count"),
		"Bounded hot key candidate count across hot shards.",
		labelScheduler, nil,
	)
	descSignalLocalizedHotKeyRatio = prom.NewDesc(
		prom.BuildFQName(namespace, "", "localized_hot_key_ratio"),
		"Localized hot key pressure ratio.",
		labelScheduler, nil,
	)
	descHotKeyPressureRatio = prom.NewDesc(
		prom.BuildFQName(namespace, "", "hot_key_pressure_ratio"),
		"Max localized hot key pressure ratio from scale signal.",
		labelScheduler, nil,
	)
	descHotKeyRejectedTotal = prom.NewDesc(
		prom.BuildFQName(namespace, "", "hot_key_rejected_total"),
		"Cumulative hot key reject observations since queue start.",
		labelScheduler, nil,
	)
	descPerKeyAdmissionDecisionsTotal = prom.NewDesc(
		prom.BuildFQName(namespace, "", "per_key_admission_decisions_total"),
		"Cumulative per-key admission decisions by action and reason.",
		labelPerKeyDecision, nil,
	)
	descPerKeyMitigationActionsTotal = prom.NewDesc(
		prom.BuildFQName(namespace, "", "per_key_mitigation_actions_total"),
		"Cumulative per-key mitigation actions (alias of per_key_admission_decisions_total).",
		labelPerKeyDecision, nil,
	)
	descShardPressureRatio = prom.NewDesc(
		prom.BuildFQName(namespace, "", "shard_pressure_ratio"),
		"Global queue depth ratio from shard pressure summary.",
		labelScheduler, nil,
	)
)

func allDescriptors() []*prom.Desc {
	return []*prom.Desc{
		descJobsSubmitted,
		descJobsCompleted,
		descJobsFailed,
		descQueueFull,
		descAdmissionRejected,
		descAdmissionShed,
		descLaneDepth,
		descShardDepth,
		descShardQueueDepth,
		descInflight,
		descQueueWait,
		descRunDuration,
		descPressureRatio,
		descScalePressureRatio,
		descScaleRecommended,
		descSignalQueueDepthRatio,
		descSignalQueueWaitMaxSeconds,
		descSignalAdmissionThrottledTotal,
		descSignalWorkerBusyRatio,
		descSignalHotShardCount,
		descSignalHotKeyCandidateCount,
		descSignalLocalizedHotKeyRatio,
		descHotKeyPressureRatio,
		descHotKeyRejectedTotal,
		descPerKeyAdmissionDecisionsTotal,
		descPerKeyMitigationActionsTotal,
		descShardPressureRatio,
	}
}
