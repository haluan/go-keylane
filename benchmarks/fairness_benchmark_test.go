// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haluan/go-keylane"
)

type fairnessRecorder struct {
	mu        sync.Mutex
	waits     []time.Duration
	latencies []time.Duration
}

func (r *fairnessRecorder) record(submitAt, startAt, doneAt time.Time) {
	wait := startAt.Sub(submitAt)
	e2e := doneAt.Sub(submitAt)
	r.mu.Lock()
	r.waits = append(r.waits, wait)
	r.latencies = append(r.latencies, e2e)
	r.mu.Unlock()
}

func (r *fairnessRecorder) snapshot() (waits, latencies []time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Duration(nil), r.waits...), append([]time.Duration(nil), r.latencies...)
}

func reportFairnessIteration(b *testing.B, rec *fairnessRecorder, completed, failed, rejected uint64) {
	waits, latencies := rec.snapshot()
	reportLatencyMetrics(b, "wait", waits)
	reportLatencyMetrics(b, "latency", latencies)
	reportCounterMetrics(b, completed, failed, rejected)
}

func BenchmarkFairnessGlobalFIFO(b *testing.B) {
	const jobDur = 50 * time.Microsecond
	for i := 0; i < b.N; i++ {
		rec := &fairnessRecorder{}
		fifo := newFifoBaseline(benchWorkerCount, benchFairnessJobCount*2)
		var completed uint64

		b.StopTimer()
		var wg sync.WaitGroup
		wg.Add(benchFairnessJobCount)
		for j := 0; j < benchFairnessJobCount; j++ {
			submitAt := time.Now()
			fifo.submit(func() {
				startAt := time.Now()
				time.Sleep(jobDur)
				doneAt := time.Now()
				rec.record(submitAt, startAt, doneAt)
				atomic.AddUint64(&completed, 1)
				wg.Done()
			})
		}
		b.StartTimer()
		wg.Wait()
		b.StopTimer()
		fifo.close()
		reportFairnessIteration(b, rec, completed, 0, 0)
	}
}

func runKeylaneFairness(b *testing.B, cfg keylane.Config, buildJob func(i int, rec *fairnessRecorder) keylane.Job) {
	for i := 0; i < b.N; i++ {
		rec := &fairnessRecorder{}
		q, cancel := startBenchmarkQueue(b, cfg)
		ctx := context.Background()
		var completed, failed, rejected uint64

		b.StopTimer()
		var wg sync.WaitGroup
		wg.Add(benchFairnessJobCount)
		for j := 0; j < benchFairnessJobCount; j++ {
			idx := j
			job := buildJob(idx, rec)
			orig := job.Run
			submitAt := time.Now()
			job.Run = func(ctx context.Context) error {
				startAt := time.Now()
				err := orig(ctx)
				doneAt := time.Now()
				rec.record(submitAt, startAt, doneAt)
				if err != nil {
					atomic.AddUint64(&failed, 1)
				} else {
					atomic.AddUint64(&completed, 1)
				}
				wg.Done()
				return err
			}
			if err := q.Submit(ctx, job); err != nil {
				atomic.AddUint64(&rejected, 1)
				wg.Done()
			}
		}
		b.StartTimer()
		wg.Wait()
		b.StopTimer()
		cancel()
		reportFairnessIteration(b, rec, completed, failed, rejected)
	}
}

func BenchmarkFairnessKeylaneManyKeys(b *testing.B) {
	keys := generateManyKeys(benchFairnessJobCount, 3)
	runKeylaneFairness(b, benchConfig(), func(i int, _ *fairnessRecorder) keylane.Job {
		return keylane.Job{
			Key:  keys[i%len(keys)],
			Lane: "default",
			Run: func(ctx context.Context) error {
				time.Sleep(50 * time.Microsecond)
				return nil
			},
		}
	})
}

func BenchmarkFairnessKeylaneOneHotKey(b *testing.B) {
	hot := generateHotKey()
	runKeylaneFairness(b, benchConfig(), func(_ int, _ *fairnessRecorder) keylane.Job {
		return keylane.Job{
			Key:  hot,
			Lane: "default",
			Run: func(ctx context.Context) error {
				time.Sleep(50 * time.Microsecond)
				return nil
			},
		}
	})
}

func BenchmarkFairnessNoisyLaneVsSensitiveLane(b *testing.B) {
	cfg := benchConfig()
	for i := 0; i < b.N; i++ {
		rec := &fairnessRecorder{}
		q, cancel := startBenchmarkQueue(b, cfg)
		ctx := context.Background()
		var completed, failed, rejected uint64
		const noisyCount = 300
		const sensitiveCount = 100

		b.StopTimer()
		var wg sync.WaitGroup
		wg.Add(noisyCount + sensitiveCount)

		submit := func(lane keylane.Lane, dur time.Duration) {
			submitAt := time.Now()
			job := keylane.Job{
				Key:  "fair-" + string(lane),
				Lane: lane,
				Run: func(ctx context.Context) error {
					startAt := time.Now()
					time.Sleep(dur)
					doneAt := time.Now()
					rec.record(submitAt, startAt, doneAt)
					atomic.AddUint64(&completed, 1)
					wg.Done()
					return nil
				},
			}
			if err := q.Submit(ctx, job); err != nil {
				atomic.AddUint64(&rejected, 1)
				wg.Done()
			}
		}
		for j := 0; j < noisyCount; j++ {
			submit("noisy", 20*time.Microsecond)
		}
		for j := 0; j < sensitiveCount; j++ {
			submit("sensitive", 20*time.Microsecond)
		}

		b.StartTimer()
		wg.Wait()
		b.StopTimer()
		cancel()
		reportFairnessIteration(b, rec, completed, failed, rejected)
	}
}

func BenchmarkFairnessShortJobsBehindLongJobs(b *testing.B) {
	for i := 0; i < b.N; i++ {
		rec := &fairnessRecorder{}
		q, cancel := startBenchmarkQueue(b, benchConfigSingleLane())
		ctx := context.Background()
		var completed uint64

		b.StopTimer()
		var wg sync.WaitGroup
		wg.Add(3)

		longKey := "long-job"
		submitAtLong := time.Now()
		_ = q.Submit(ctx, keylane.Job{
			Key:  longKey,
			Lane: "default",
			Run: func(ctx context.Context) error {
				startAt := time.Now()
				time.Sleep(2 * time.Millisecond)
				rec.record(submitAtLong, startAt, time.Now())
				wg.Done()
				return nil
			},
		})

		for j := 0; j < 2; j++ {
			submitAt := time.Now()
			_ = q.Submit(ctx, keylane.Job{
				Key:  longKey,
				Lane: "default",
				Run: func(ctx context.Context) error {
					startAt := time.Now()
					time.Sleep(10 * time.Microsecond)
					doneAt := time.Now()
					rec.record(submitAt, startAt, doneAt)
					atomic.AddUint64(&completed, 1)
					wg.Done()
					return nil
				},
			})
		}

		b.StartTimer()
		wg.Wait()
		b.StopTimer()
		cancel()
		reportFairnessIteration(b, rec, completed+1, 0, 0)
	}
}

func BenchmarkFairnessBurstThenDrain(b *testing.B) {
	keys := generateManyKeys(benchFairnessJobCount, 4)
	for i := 0; i < b.N; i++ {
		rec := &fairnessRecorder{}
		q, cancel := startBenchmarkQueue(b, benchConfig())
		ctx := context.Background()
		var completed uint64

		b.StopTimer()
		var wg sync.WaitGroup
		wg.Add(benchFairnessJobCount)
		submitAt := time.Now()
		for j := 0; j < benchFairnessJobCount; j++ {
			key := keys[j%len(keys)]
			job := keylane.Job{
				Key:  key,
				Lane: "default",
				Run: func(ctx context.Context) error {
					startAt := time.Now()
					time.Sleep(30 * time.Microsecond)
					rec.record(submitAt, startAt, time.Now())
					atomic.AddUint64(&completed, 1)
					wg.Done()
					return nil
				},
			}
			_ = q.Submit(ctx, job)
		}
		b.StartTimer()
		wg.Wait()
		b.StopTimer()
		cancel()
		reportFairnessIteration(b, rec, completed, 0, 0)
	}
}
