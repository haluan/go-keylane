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

	descScalePressureRatio = prom.NewDesc(
		prom.BuildFQName(namespace, "", "scale_pressure_ratio"),
		"KL-1504 composite pressure ratio for autoscaling signals.",
		labelScheduler, nil,
	)
	descScaleRecommended = prom.NewDesc(
		prom.BuildFQName(namespace, "", "scale_recommended"),
		"KL-1504 scale-out recommendation (1=recommended, 0=not).",
		labelScaleRecommended, nil,
	)
	descSignalQueueDepthRatio = prom.NewDesc(
		prom.BuildFQName(namespace, "", "queue_depth_ratio"),
		"KL-1504 queue depth ratio component (scheduler aggregate).",
		labelScheduler, nil,
	)
	descSignalQueueWaitMaxSeconds = prom.NewDesc(
		prom.BuildFQName(namespace, "", "queue_wait_max_seconds"),
		"KL-1504 max observed queue wait in seconds (scheduler aggregate).",
		labelScheduler, nil,
	)
	descSignalAdmissionThrottledTotal = prom.NewDesc(
		prom.BuildFQName(namespace, "", "admission_throttled_total"),
		"KL-1504 cumulative per-key throttle decisions since queue start.",
		labelScheduler, nil,
	)
	descSignalWorkerBusyRatio = prom.NewDesc(
		prom.BuildFQName(namespace, "", "worker_busy_ratio"),
		"KL-1504 worker busy ratio (in-flight / workers).",
		labelScheduler, nil,
	)
	descSignalHotShardCount = prom.NewDesc(
		prom.BuildFQName(namespace, "", "hot_shard_count"),
		"KL-1504 count of hot shards.",
		labelScheduler, nil,
	)
	descSignalHotKeyCandidateCount = prom.NewDesc(
		prom.BuildFQName(namespace, "", "hot_key_candidate_count"),
		"KL-1504 bounded hot key candidate count across hot shards.",
		labelScheduler, nil,
	)
	descSignalLocalizedHotKeyRatio = prom.NewDesc(
		prom.BuildFQName(namespace, "", "localized_hot_key_ratio"),
		"KL-1504 localized hot key pressure ratio.",
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
	}
}
