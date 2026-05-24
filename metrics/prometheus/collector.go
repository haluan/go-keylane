// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Package prometheus exposes a pull-based Prometheus collector for keylane queues.
package prometheus

import (
	"strconv"

	"github.com/haluan/go-keylane"
	prom "github.com/prometheus/client_golang/prometheus"
)

// CollectorOptions configures the Prometheus collector.
type CollectorOptions struct {
	// SchedulerName is the low-cardinality value for the "scheduler" label.
	SchedulerName string
}

// Collector implements prom.Collector using Queue.StatsGCPressure and Queue.Pressure.
type Collector struct {
	q    *keylane.Queue
	name string
}

// NewCollector returns a prom.Collector that scrapes q on each Collect call.
func NewCollector(q *keylane.Queue, opts CollectorOptions) prom.Collector {
	name := opts.SchedulerName
	if name == "" {
		name = "default"
	}
	return &Collector{q: q, name: name}
}

// Describe implements prom.Collector.
func (c *Collector) Describe(ch chan<- *prom.Desc) {
	for _, d := range allDescriptors() {
		ch <- d
	}
}

// Collect implements prom.Collector.
func (c *Collector) Collect(ch chan<- prom.Metric) {
	snap := c.q.StatsGCPressure()
	pressure := c.q.Pressure()

	ch <- prom.MustNewConstMetric(
		descPressureRatio, prom.GaugeValue, pressure.TotalDepthRatio, c.name,
	)

	ch <- prom.MustNewConstMetric(
		descInflight, prom.GaugeValue, float64(snap.TotalInFlight),
		c.name, "", "",
	)

	for _, lane := range snap.Lanes {
		labels := []string{c.name, lane.Name}
		counters := lane.Counters
		ch <- prom.MustNewConstMetric(descJobsSubmitted, prom.CounterValue, float64(counters.Submitted), labels...)
		ch <- prom.MustNewConstMetric(descJobsCompleted, prom.CounterValue, float64(counters.Completed), labels...)
		ch <- prom.MustNewConstMetric(descJobsFailed, prom.CounterValue, float64(counters.Failed), labels...)
		ch <- prom.MustNewConstMetric(descQueueFull, prom.CounterValue, float64(counters.QueueFull), labels...)
		ch <- prom.MustNewConstMetric(descAdmissionRejected, prom.CounterValue, float64(counters.AdmissionRejected), labels...)
		ch <- prom.MustNewConstMetric(descAdmissionShed, prom.CounterValue, float64(counters.OverloadShed), labels...)
		ch <- prom.MustNewConstMetric(descLaneDepth, prom.GaugeValue, float64(lane.Queued), labels...)
		ch <- prom.MustNewConstMetric(descInflight, prom.GaugeValue, float64(lane.InFlight),
			c.name, "", lane.Name,
		)
		emitQueueWaitSummary(ch, c.name, lane.Name, lane.QueueWait)
		emitRunSummary(ch, c.name, lane.Name, lane.Run)
	}

	for _, shard := range snap.Shards {
		shardID := strconv.FormatUint(uint64(shard.ShardID), 10)
		ch <- prom.MustNewConstMetric(
			descShardDepth, prom.GaugeValue, float64(shard.Queued),
			c.name, shardID,
		)
		ch <- prom.MustNewConstMetric(
			descInflight, prom.GaugeValue, float64(shard.InFlight),
			c.name, shardID, "",
		)
	}

	emitQueueWaitSummary(ch, c.name, "", snap.QueueWait)
	emitRunSummary(ch, c.name, "", snap.Run)
	emitScaleSignal(ch, c.name, c.q.ScaleSignal(), c.q.ScaleAdmissionTotals())
}

const aggregateLaneLabel = "_all"

func emitScaleSignal(ch chan<- prom.Metric, scheduler string, sig keylane.ScaleSignal, totals keylane.ScaleAdmissionTotals) {
	rec := 0.0
	if sig.Recommended {
		rec = 1
	}
	reason := string(sig.Reason)
	if reason == "" {
		reason = "none"
	}
	scope := string(sig.Scope)
	if scope == "" {
		scope = "none"
	}
	ch <- prom.MustNewConstMetric(descScalePressureRatio, prom.GaugeValue, sig.PressureRatio, scheduler)
	ch <- prom.MustNewConstMetric(descScaleRecommended, prom.GaugeValue, rec, scheduler, reason, scope)
	ch <- prom.MustNewConstMetric(descSignalQueueDepthRatio, prom.GaugeValue, sig.QueueDepthRatio, scheduler)
	ch <- prom.MustNewConstMetric(descSignalQueueWaitMaxSeconds, prom.GaugeValue, sig.QueueWaitMax.Seconds(), scheduler)
	ch <- prom.MustNewConstMetric(descAdmissionRejected, prom.CounterValue, float64(totals.Rejected), scheduler, aggregateLaneLabel)
	ch <- prom.MustNewConstMetric(descAdmissionShed, prom.CounterValue, float64(totals.Shed), scheduler, aggregateLaneLabel)
	ch <- prom.MustNewConstMetric(descSignalAdmissionThrottledTotal, prom.CounterValue, float64(totals.Throttled), scheduler)
	ch <- prom.MustNewConstMetric(descSignalWorkerBusyRatio, prom.GaugeValue, sig.WorkerBusyRatio, scheduler)
	ch <- prom.MustNewConstMetric(descSignalHotShardCount, prom.GaugeValue, float64(sig.HotShardCount), scheduler)
	ch <- prom.MustNewConstMetric(descSignalHotKeyCandidateCount, prom.GaugeValue, float64(sig.HotKeyCandidateCount), scheduler)
	ch <- prom.MustNewConstMetric(descSignalLocalizedHotKeyRatio, prom.GaugeValue, sig.LocalizedHotKeyRatio, scheduler)
}

func emitQueueWaitSummary(ch chan<- prom.Metric, scheduler, lane string, stats keylane.QueueWaitStatsGCPressure) {
	labels := []string{scheduler, lane}
	if stats.Count == 0 {
		ch <- prom.MustNewConstSummary(descQueueWait, 0, 0, nil, labels...)
		return
	}
	sumSec := float64(stats.TotalNanos) / 1e9
	quantiles := map[float64]float64{
		0.5: float64(stats.AverageNanos()) / 1e9,
		1.0: float64(stats.MaxNanos) / 1e9,
	}
	ch <- prom.MustNewConstSummary(descQueueWait, stats.Count, sumSec, quantiles, labels...)
}

func emitRunSummary(ch chan<- prom.Metric, scheduler, lane string, stats keylane.RunStatsGCPressure) {
	labels := []string{scheduler, lane}
	if stats.Count == 0 {
		ch <- prom.MustNewConstSummary(descRunDuration, 0, 0, nil, labels...)
		return
	}
	sumSec := float64(stats.TotalNanos) / 1e9
	quantiles := map[float64]float64{
		0.5: float64(stats.AverageNanos()) / 1e9,
		1.0: float64(stats.MaxNanos) / 1e9,
	}
	ch <- prom.MustNewConstSummary(descRunDuration, stats.Count, sumSec, quantiles, labels...)
}
