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
)

func allDescriptors() []*prom.Desc {
	return []*prom.Desc{
		descJobsSubmitted,
		descJobsCompleted,
		descJobsFailed,
		descQueueFull,
		descLaneDepth,
		descShardDepth,
		descInflight,
		descQueueWait,
		descRunDuration,
		descPressureRatio,
	}
}
