// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/haluan/go-keylane"
)

// Standard workload shape for production benchmarks (documented in benchmarks/README.md).
const (
	benchShardCount       = 8
	benchWorkerCount      = 4
	benchQueueSizePerLane = 64
	benchFairnessJobCount = 400
)

func benchConfig() keylane.Config {
	return keylane.Config{
		ShardCount:       benchShardCount,
		WorkerCount:      benchWorkerCount,
		QueueSizePerLane: benchQueueSizePerLane,
		LaneQuotas: map[keylane.Lane]int{
			"default":   2,
			"noisy":     4,
			"sensitive": 1,
			"laneA":     2,
			"laneB":     2,
			"laneC":     2,
		},
	}
}

func benchConfigSingleLane() keylane.Config {
	return keylane.Config{
		ShardCount:       16,
		WorkerCount:      4,
		QueueSizePerLane: 10000,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
}

func startBenchmarkQueue(b *testing.B, cfg keylane.Config) (*keylane.Queue, context.CancelFunc) {
	b.Helper()
	q, err := keylane.New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := q.Start(ctx); err != nil {
		cancel()
		b.Fatal(err)
	}
	return q, cancel
}

func makeBenchmarkQueue(b *testing.B, cfg keylane.Config) (*keylane.Queue, context.CancelFunc) {
	b.Helper()
	q, cancel := startBenchmarkQueue(b, cfg)
	b.Cleanup(cancel)
	return q, cancel
}

func generateManyKeys(n int, seed int64) []string {
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("bench-key-%d-%d", seed, i)
	}
	return keys
}

func generateHotKey() string {
	return "bench-hot-key"
}

func percentile(samples []time.Duration, p float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func reportLatencyMetrics(b *testing.B, prefix string, samples []time.Duration) {
	b.Helper()
	if len(samples) == 0 {
		return
	}
	b.ReportMetric(float64(percentile(samples, 0.50).Nanoseconds()), prefix+"_p50_ns")
	b.ReportMetric(float64(percentile(samples, 0.95).Nanoseconds()), prefix+"_p95_ns")
	b.ReportMetric(float64(percentile(samples, 0.99).Nanoseconds()), prefix+"_p99_ns")
}

func reportCounterMetrics(b *testing.B, completed, failed, rejected uint64) {
	b.Helper()
	b.ReportMetric(float64(completed), "completed_jobs/op")
	b.ReportMetric(float64(failed), "failed_jobs/op")
	b.ReportMetric(float64(rejected), "rejected_jobs/op")
}

func dummyRun(ctx context.Context) error {
	return nil
}

func dummyValueRun(ctx context.Context) (int, error) {
	return 42, nil
}
