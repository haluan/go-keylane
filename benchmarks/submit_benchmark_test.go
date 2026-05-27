// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/haluan/go-keylane"
)

func BenchmarkKeylaneSubmit(b *testing.B) {
	q, _ := makeBenchmarkQueue(b, benchConfigSingleLane())
	job := keylane.Job{Key: "k", Lane: "default", Run: dummyRun}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(context.Background(), job)
	}
}

func BenchmarkKeylaneSubmitManyKeys(b *testing.B) {
	q, _ := makeBenchmarkQueue(b, benchConfig())
	keys := generateManyKeys(256, 1)
	job := keylane.Job{Lane: "default", Run: dummyRun}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		job.Key = keys[i%len(keys)]
		_ = q.Submit(context.Background(), job)
	}
}

func BenchmarkKeylaneSubmitOneHotKey(b *testing.B) {
	q, _ := makeBenchmarkQueue(b, benchConfig())
	job := keylane.Job{Key: generateHotKey(), Lane: "default", Run: dummyRun}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(context.Background(), job)
	}
}

func BenchmarkKeylaneSubmitQueueFull(b *testing.B) {
	cfg := benchConfigSingleLane()
	cfg.WorkerCount = 1
	cfg.QueueSizePerLane = 2
	q, cancel := startBenchmarkQueue(b, cfg)
	defer cancel()

	block := make(chan struct{})
	fill := keylane.Job{
		Key:  "fill",
		Lane: "default",
		Run: func(ctx context.Context) error {
			<-block
			return nil
		},
	}
	ctx := context.Background()
	_ = q.Submit(ctx, fill)
	_ = q.Submit(ctx, fill)

	reject := keylane.Job{Key: "reject", Lane: "default", Run: dummyRun}
	var rejected uint64

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := q.Submit(ctx, reject); errors.Is(err, keylane.ErrQueueFull) {
			rejected++
		}
	}
	b.StopTimer()
	close(block)
	b.ReportMetric(float64(rejected)/float64(b.N), "rejected_jobs/op")
}

func BenchmarkKeylaneSubmitManyLanes(b *testing.B) {
	q, _ := makeBenchmarkQueue(b, benchConfig())
	lanes := []keylane.Lane{"default", "noisy", "sensitive", "laneA", "laneB", "laneC"}
	job := keylane.Job{Key: "k", Run: dummyRun}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		job.Lane = lanes[i%len(lanes)]
		_ = q.Submit(context.Background(), job)
	}
}

func BenchmarkKeylaneSubmitObservabilityOff(b *testing.B) {
	q, _ := makeBenchmarkQueue(b, benchConfigSingleLane())
	job := keylane.Job{Key: "k", Lane: "default", Run: dummyRun}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(context.Background(), job)
	}
}

func BenchmarkKeylaneSubmitStatsEnabled(b *testing.B) {
	// Stats counters are always updated on the worker path; submit enqueue cost is unchanged.
	// Compare snapshot read cost with BenchmarkStatsGCPressure in the root package.
	q, _ := makeBenchmarkQueue(b, benchConfigSingleLane())
	job := keylane.Job{Key: "k", Lane: "default", Run: dummyRun}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(context.Background(), job)
	}
}

func BenchmarkKeylaneSubmitTimingEnabled(b *testing.B) {
	cfg := benchConfigSingleLane()
	cfg.Observability = keylane.ObservabilityConfig{
		TrackQueueWait: true,
	}
	q, _ := makeBenchmarkQueue(b, cfg)
	job := keylane.Job{Key: "k", Lane: "default", Run: dummyRun}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(context.Background(), job)
	}
}

func BenchmarkKeylaneSubmitContextCanceled(b *testing.B) {
	q, _ := makeBenchmarkQueue(b, benchConfigSingleLane())
	job := keylane.Job{Key: "k", Lane: "default", Run: dummyRun}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(ctx, job)
	}
}

func BenchmarkKeylaneSubmitDeadlineExpired(b *testing.B) {
	q, _ := makeBenchmarkQueue(b, benchConfigSingleLane())
	job := keylane.Job{Key: "k", Lane: "default", Run: dummyRun}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(ctx, job)
	}
}

func BenchmarkKeylaneSubmitDuringShutdown(b *testing.B) {
	cfg := benchConfigSingleLane()
	q, err := keylane.New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	if err := q.Start(runCtx); err != nil {
		runCancel()
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = q.Stop(context.Background())
		runCancel()
	})
	if err := q.Stop(context.Background()); err != nil {
		b.Fatal(err)
	}

	job := keylane.Job{Key: "k", Lane: "default", Run: dummyRun}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(ctx, job)
	}
}

func BenchmarkKeylaneSubmitNoopHookEnabled(b *testing.B) {
	cfg := benchConfigSingleLane()
	cfg.Observability = keylane.ObservabilityConfig{
		Hooks: keylane.Hooks{
			OnJobTiming: func(keylane.JobTimingEvent) {},
		},
	}
	q, _ := makeBenchmarkQueue(b, cfg)
	job := keylane.Job{Key: "k", Lane: "default", Run: dummyRun}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Submit(context.Background(), job)
	}
}
