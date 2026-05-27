// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func benchPipelineQueue(b *testing.B, obs ObservabilityConfig, backend BackendResourceConfig) (*Queue, context.Context) {
	b.Helper()
	cfg := newTestConfig()
	cfg.Observability = obs
	cfg.BackendResources = backend
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	if err := q.Start(ctx); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = q.Stop(context.Background()) })
	return q, ctx
}

func benchSubmitSingleStage(b *testing.B, q *Queue, ctx context.Context) {
	b.Helper()
	meta := RequestMeta{Key: "bench", Lane: "default"}
	pipeline := Pipeline[pState, pOutput]{
		Meta: meta,
		Stages: []PipelineStage[pState]{
			validPipelineStage(StageValidate, func(s *pState) { s.Val = 1 }),
		},
		Complete: validPipelineComplete(),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		future, err := SubmitPipeline(ctx, q, pipeline)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := future.Await(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPipelineSingleStage(b *testing.B) {
	q, ctx := benchPipelineQueue(b, DefaultObservabilityConfig(), BackendResourceConfig{})
	benchSubmitSingleStage(b, q, ctx)
}

func BenchmarkPipelineMultiStage(b *testing.B) {
	q, ctx := benchPipelineQueue(b, DefaultObservabilityConfig(), BackendResourceConfig{})
	meta := RequestMeta{Key: "bench", Lane: "default"}
	pipeline := Pipeline[pState, pOutput]{
		Meta: meta,
		Stages: []PipelineStage[pState]{
			validPipelineStage(StageValidate, func(s *pState) {}),
			validPipelineStage(StageDBRead, func(s *pState) { s.Val = 1 }),
			validPipelineStage(StageBusiness, func(s *pState) {}),
		},
		Complete: validPipelineComplete(),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		future, err := SubmitPipeline(ctx, q, pipeline)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := future.Await(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPipelineStageHooksDisabled(b *testing.B) {
	obs := DefaultObservabilityConfig()
	obs.EnableHooks = false
	q, ctx := benchPipelineQueue(b, obs, BackendResourceConfig{})
	benchSubmitSingleStage(b, q, ctx)
}

func BenchmarkPipelineStageHooksEnabled(b *testing.B) {
	obs := DefaultObservabilityConfig()
	noop := func(StageObservation) {}
	obs.Hooks.Request.OnStageStarted = noop
	obs.Hooks.Request.OnStageCompleted = noop
	obs.Hooks.Request.OnStageFailed = noop
	q, ctx := benchPipelineQueue(b, obs, BackendResourceConfig{})
	benchSubmitSingleStage(b, q, ctx)
}

func BenchmarkPipelineFullStack(b *testing.B) {
	obs := DefaultObservabilityConfig()
	br := testBackendResourceConfig()
	br.PressureProviders = []BackendPressureProvider{
		staticPressureProvider{snap: BackendPressureSnapshot{
			Resource: "primary-db", Lane: BackendLaneDBRead, InUse: 1, Capacity: 4,
		}},
	}
	q, ctx := benchPipelineQueue(b, obs, br)
	meta := RequestMeta{Key: "bench", Lane: "default"}
	pipeline := Pipeline[pState, pOutput]{
		Meta: meta,
		Stages: []PipelineStage[pState]{
			validPipelineStage(StageValidate, func(s *pState) {}),
			{
				Meta: StageMeta{Name: StageDBRead},
				Run: func(ctx context.Context, st pState) (pState, error) {
					op := BackendOperationFromStage(ctx, "primary-db", BackendLaneDBRead)
					return WithBackend(ctx, q, op, func(ctx context.Context) (pState, error) {
						st.Val = 2
						return st, nil
					})
				},
			},
		},
		Complete: validPipelineComplete(),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		future, err := SubmitPipeline(ctx, q, pipeline)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := future.Await(ctx); err != nil {
			b.Fatal(err)
		}
		_ = q.BackendPressure(ctx)
	}
}

func BenchmarkPipelineBackendAcquireRelease(b *testing.B) {
	br := testBackendResourceConfig()
	q, ctx := benchPipelineQueue(b, DefaultObservabilityConfig(), br)
	meta := RequestMeta{Key: "bench", Lane: "default"}
	pipeline := Pipeline[pState, pOutput]{
		Meta: meta,
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageDBRead},
				Run: func(ctx context.Context, st pState) (pState, error) {
					op := BackendOperationFromStage(ctx, "primary-db", BackendLaneDBRead)
					return WithBackend(ctx, q, op, func(ctx context.Context) (pState, error) {
						return st, nil
					})
				},
			},
		},
		Complete: validPipelineComplete(),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		future, err := SubmitPipeline(ctx, q, pipeline)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := future.Await(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPipelineBackendSaturatedReject(b *testing.B) {
	br := BackendResourceConfig{
		Enabled: true,
		Resources: map[BackendResourceName]BackendResourcePolicy{
			"primary-db": {
				Lanes: map[BackendLane]BackendLanePolicy{
					BackendLaneDBWrite: {MaxInFlight: 1, Admission: BackendAdmissionReject},
				},
			},
		},
	}
	q, ctx := benchPipelineQueue(b, DefaultObservabilityConfig(), br)
	meta := RequestMeta{Key: "bench", Lane: "default"}
	pipeline := Pipeline[pState, pOutput]{
		Meta: meta,
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageDBRead},
				Run: func(ctx context.Context, st pState) (pState, error) {
					l1, err := AcquireBackend(ctx, q, BackendOperation{
						Resource: "primary-db", Lane: BackendLaneDBWrite,
					})
					if err != nil {
						return st, err
					}
					defer l1.Release()
					_, err = AcquireBackend(ctx, q, BackendOperation{
						Resource: "primary-db", Lane: BackendLaneDBWrite,
					})
					return st, err
				},
			},
		},
		Complete: validPipelineComplete(),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		future, err := SubmitPipeline(ctx, q, pipeline)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := future.Await(ctx); err == nil {
			b.Fatal("expected saturated reject error")
		}
	}
}

func BenchmarkPipelineSQLPressureAdapter(b *testing.B) {
	BenchmarkSQLDBPressureAdapter(b)
}

func BenchmarkPipelineAPIPressureAdapter(b *testing.B) {
	BenchmarkAPIClientPressureAdapter(b)
}

func BenchmarkPipelineRetryDisabled(b *testing.B) {
	q, ctx := benchPipelineQueue(b, DefaultObservabilityConfig(), BackendResourceConfig{})
	benchSubmitSingleStage(b, q, ctx)
}

func BenchmarkPipelineRetryEnabled(b *testing.B) {
	cfg := newTestConfig()
	cfg.Retry = RetryPolicy{Enabled: true, MaxAttempts: 2, InitialBackoff: 0, Jitter: false, MinRemainingBudget: 0}
	cfg.Observability = DefaultObservabilityConfig()
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	if err := q.Start(ctx); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = q.Stop(context.Background()) })
	var attempts atomic.Int32
	meta := RequestMeta{Key: "bench", Lane: "default"}
	pipeline := Pipeline[pState, pOutput]{
		Meta:        meta,
		Retry:       RetryPolicy{Enabled: true},
		Idempotency: retrySafeIdempotency(),
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				Run: func(_ context.Context, st pState) (pState, error) {
					if attempts.Add(1) == 1 {
						return st, RetryableFailure(errors.New("bench retry"))
					}
					return st, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		attempts.Store(0)
		future, err := SubmitPipeline(ctx, q, pipeline)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := future.Await(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPipelineBackendPressureCollection(b *testing.B) {
	br := BackendResourceConfig{}
	br.PressureProviders = []BackendPressureProvider{
		staticPressureProvider{snap: BackendPressureSnapshot{
			Resource: "primary-db", Lane: BackendLaneDBRead, InUse: 1, Capacity: 4,
		}},
	}
	q, ctx := benchPipelineQueue(b, DefaultObservabilityConfig(), br)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = q.BackendPressure(ctx)
	}
}
