// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"runtime"
	"sort"
	"testing"
	"time"
)

const benchContStormSize = 64

func benchContinuationQueue(obs ObservabilityConfig) (*Queue, context.CancelFunc) {
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 4096,
		LaneQuotas:       map[Lane]int{"default": 1},
		Continuation:     ContinuationConfig{Enabled: true, MaxPending: 4096},
		Observability:    obs,
	}
	q, err := New(cfg)
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := q.Start(ctx); err != nil {
		cancel()
		panic(err)
	}
	return q, cancel
}

func benchContinuationHooks() ObservabilityConfig {
	obs := DefaultObservabilityConfig()
	noop := func(ContinuationObservation) {}
	obs.Hooks.Request.Continuation = ContinuationHooks{
		OnContinuationYielded:   noop,
		OnContinuationResumed:   noop,
		OnContinuationCompleted: noop,
		OnContinuationFailed:    noop,
		OnContinuationCancelled: noop,
		OnContinuationLate:      noop,
	}
	return obs
}

func benchContinuationObsWithResumeRecorder(onResume func(ContinuationObservation)) ObservabilityConfig {
	obs := DefaultObservabilityConfig()
	obs.Hooks.Request.Continuation.OnContinuationResumed = onResume
	return obs
}

func benchPercentile(samples []time.Duration, p float64) time.Duration {
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

func reportDurationMetric(b *testing.B, name string, d time.Duration) {
	b.Helper()
	b.ReportMetric(float64(d.Nanoseconds()), name)
}

func reportDurationSamples(b *testing.B, prefix string, samples []time.Duration) {
	b.Helper()
	if len(samples) == 0 {
		return
	}
	reportDurationMetric(b, prefix+"_p50_ns", benchPercentile(samples, 0.50))
}

func benchYieldResumePipeline(ctx context.Context, q *Queue, ready chan<- ContinuationCompleter[pState]) (Future[pOutput], error) {
	return SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "bench", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](context.Background())
					ready <- c
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
}

func benchContinuationYieldCompleteAwait(b *testing.B, q *Queue, ctx context.Context) {
	b.Helper()
	ready := make(chan ContinuationCompleter[pState], 1)
	future, err := benchYieldResumePipeline(ctx, q, ready)
	if err != nil {
		b.Fatal(err)
	}
	if !(<-ready).Complete(pState{Val: 1}) {
		b.Fatal("complete failed")
	}
	if _, err := future.Await(ctx); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkPipelineContinuationYieldResume(b *testing.B) {
	var resumeWaits []time.Duration
	obs := benchContinuationObsWithResumeRecorder(func(o ContinuationObservation) {
		resumeWaits = append(resumeWaits, o.ResumeQueueWait)
	})
	q, cancel := benchContinuationQueue(obs)
	defer cancel()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchContinuationYieldCompleteAwait(b, q, ctx)
	}
	reportDurationSamples(b, "resume_queue_wait", resumeWaits)
}

func BenchmarkPipelineContinuationPendingMany(b *testing.B) {
	q, cancel := benchContinuationQueue(DefaultObservabilityConfig())
	defer cancel()
	ctx := context.Background()

	var totalHeapBytes uint64
	var totalPendingCount int

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		completers := make([]ContinuationCompleter[pState], benchContStormSize)
		futures := make([]Future[pOutput], benchContStormSize)

		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		for j := 0; j < benchContStormSize; j++ {
			ready := make(chan ContinuationCompleter[pState], 1)
			future, err := benchYieldResumePipeline(ctx, q, ready)
			if err != nil {
				b.Fatal(err)
			}
			completers[j] = <-ready
			futures[j] = future
		}
		var after runtime.MemStats
		runtime.ReadMemStats(&after)

		b.StopTimer()
		snap := q.DebugSnapshot().Continuation
		if snap.Pending > 0 {
			totalPendingCount += snap.Pending
			if after.Alloc >= before.Alloc {
				totalHeapBytes += after.Alloc - before.Alloc
			}
		}
		for _, c := range completers {
			c.Complete(pState{})
		}
		for _, f := range futures {
			if _, err := f.Await(ctx); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
	}

	if b.N > 0 && totalPendingCount > 0 {
		avgHeap := totalHeapBytes / uint64(b.N)
		b.ReportMetric(float64(avgHeap), "pending_heap_bytes")
		b.ReportMetric(float64(totalPendingCount)/float64(b.N), "pending_count")
		if avgHeap > 0 {
			b.ReportMetric(float64(avgHeap)/float64(benchContStormSize), "pending_bytes_per_continuation")
		}
	}
}

func BenchmarkPipelineContinuationCompletionStorm(b *testing.B) {
	q, cancel := benchContinuationQueue(DefaultObservabilityConfig())
	defer cancel()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		completers := make([]ContinuationCompleter[pState], benchContStormSize)
		futures := make([]Future[pOutput], benchContStormSize)
		b.StopTimer()
		for j := 0; j < benchContStormSize; j++ {
			ready := make(chan ContinuationCompleter[pState], 1)
			future, err := benchYieldResumePipeline(ctx, q, ready)
			if err != nil {
				b.Fatal(err)
			}
			completers[j] = <-ready
			futures[j] = future
		}
		b.StartTimer()
		for _, c := range completers {
			c.Complete(pState{Val: 1})
		}
		b.StopTimer()
		for _, f := range futures {
			if _, err := f.Await(ctx); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
	}
}

func BenchmarkPipelineContinuationCancellationStorm(b *testing.B) {
	q, cancel := benchContinuationQueue(DefaultObservabilityConfig())
	defer cancel()
	base := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		futures := make([]Future[pOutput], benchContStormSize)
		reqCancels := make([]context.CancelFunc, benchContStormSize)
		b.StopTimer()
		for j := 0; j < benchContStormSize; j++ {
			reqCtx, reqCancel := context.WithCancel(base)
			reqCancels[j] = reqCancel
			ready := make(chan ContinuationCompleter[pState], 1)
			future, err := benchYieldResumePipeline(reqCtx, q, ready)
			if err != nil {
				b.Fatal(err)
			}
			<-ready
			futures[j] = future
		}
		b.StartTimer()
		for _, c := range reqCancels {
			c()
		}
		b.StopTimer()
		for _, f := range futures {
			_, _ = f.Await(base)
		}
		b.StartTimer()
	}
}

func BenchmarkPipelineContinuationLateCompletion(b *testing.B) {
	q, cancel := benchContinuationQueue(DefaultObservabilityConfig())
	defer cancel()
	base := context.Background()

	var lateSamples []time.Duration

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		reqCtx, reqCancel := context.WithCancel(base)
		ready := make(chan ContinuationCompleter[pState], 1)
		future, err := benchYieldResumePipeline(reqCtx, q, ready)
		if err != nil {
			b.Fatal(err)
		}
		completer := <-ready
		reqCancel()
		_, _ = future.Await(base)
		b.StartTimer()
		start := time.Now()
		if completer.Complete(pState{Val: 1}) {
			b.Fatal("expected late Complete to return false")
		}
		lateSamples = append(lateSamples, time.Since(start))
		b.StopTimer()
		_, _ = future.Await(base)
		b.StartTimer()
	}

	if len(lateSamples) > 0 {
		var total int64
		for _, d := range lateSamples {
			total += d.Nanoseconds()
		}
		reportDurationMetric(b, "late_complete_ns/op", time.Duration(total/int64(len(lateSamples))))
		reportDurationSamples(b, "late_complete", lateSamples)
	}
}

func BenchmarkPipelineContinuationObservabilityOverhead(b *testing.B) {
	b.Run("hooks", func(b *testing.B) {
		q, cancel := benchContinuationQueue(benchContinuationHooks())
		defer cancel()
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchContinuationYieldCompleteAwait(b, q, ctx)
		}
	})
	b.Run("no_hooks", func(b *testing.B) {
		q, cancel := benchContinuationQueue(DefaultObservabilityConfig())
		defer cancel()
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchContinuationYieldCompleteAwait(b, q, ctx)
		}
	})
}

func BenchmarkPipelineContinuationVsSynchronousStage(b *testing.B) {
	benchmarkPipelineContinuationVsSynchronousStage(b)
}

func BenchmarkPipelineContinuationVsSynchronous(b *testing.B) {
	benchmarkPipelineContinuationVsSynchronousStage(b)
}

func benchmarkPipelineContinuationVsSynchronousStage(b *testing.B) {
	var resumeWaits []time.Duration
	obs := benchContinuationObsWithResumeRecorder(func(o ContinuationObservation) {
		resumeWaits = append(resumeWaits, o.ResumeQueueWait)
	})
	q, cancel := benchContinuationQueue(obs)
	defer cancel()
	ctx := context.Background()

	b.Run("continuation", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchContinuationYieldCompleteAwait(b, q, ctx)
		}
		reportDurationSamples(b, "resume_queue_wait", resumeWaits)
	})
	b.Run("synchronous", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
				Meta: RequestMeta{Key: "bench", Lane: "default"},
				Stages: []PipelineStage[pState]{
					{
						Meta: StageMeta{Name: StageValidate},
						Run: func(_ context.Context, st pState) (pState, error) {
							return pState{Val: 1}, nil
						},
					},
				},
				Complete: validPipelineComplete(),
			})
			if err != nil {
				b.Fatal(err)
			}
			if _, err := future.Await(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
}
