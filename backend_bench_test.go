// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"runtime"
	"testing"
)

func benchmarkBackendQueue(b *testing.B, cfg Config) *Queue {
	b.Helper()
	ctx := context.Background()
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = q.Stop(context.Background()) })
	return q
}

func BenchmarkBackendAcquireReleaseDisabled(b *testing.B) {
	cfg := newTestConfig()
	q := benchmarkBackendQueue(b, cfg)
	op := BackendOperation{Resource: "primary-db", Lane: BackendLaneDBRead}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lease, err := AcquireBackend(ctx, q, op)
		if err != nil {
			b.Fatal(err)
		}
		lease.Release()
	}
}

func BenchmarkBackendAcquireReleaseEnabledUncontended(b *testing.B) {
	cfg := newTestConfig()
	cfg.BackendResources = testBackendResourceConfig()
	q := benchmarkBackendQueue(b, cfg)
	op := BackendOperation{Resource: "primary-db", Lane: BackendLaneDBRead}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lease, err := AcquireBackend(ctx, q, op)
		if err != nil {
			b.Fatal(err)
		}
		lease.Release()
	}
}

func BenchmarkBackendAcquireReleaseContended(b *testing.B) {
	cfg := newTestConfig()
	cfg.BackendResources = BackendResourceConfig{
		Enabled: true,
		Resources: map[BackendResourceName]BackendResourcePolicy{
			"primary-db": {
				Lanes: map[BackendLane]BackendLanePolicy{
					BackendLaneDBRead: {MaxInFlight: 1, Admission: BackendAdmissionReject},
				},
			},
		},
	}
	q := benchmarkBackendQueue(b, cfg)
	op := BackendOperation{Resource: "primary-db", Lane: BackendLaneDBRead}
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lease, err := AcquireBackend(ctx, q, op)
			if err != nil {
				continue
			}
			runtime.Gosched()
			lease.Release()
		}
	})
}

func BenchmarkBackendMultiResourceIndependent(b *testing.B) {
	cfg := newTestConfig()
	cfg.BackendResources = BackendResourceConfig{
		Enabled: true,
		Resources: map[BackendResourceName]BackendResourcePolicy{
			"primary-db": {
				Lanes: map[BackendLane]BackendLanePolicy{
					BackendLaneDBRead: {MaxInFlight: 8, Admission: BackendAdmissionReject},
				},
			},
			"wallet-api": {
				Lanes: map[BackendLane]BackendLanePolicy{
					BackendLaneExternalAPI: {MaxInFlight: 8, Admission: BackendAdmissionReject},
				},
			},
		},
	}
	q := benchmarkBackendQueue(b, cfg)
	dbOp := BackendOperation{Resource: "primary-db", Lane: BackendLaneDBRead}
	apiOp := BackendOperation{Resource: "wallet-api", Lane: BackendLaneExternalAPI}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dbLease, err := AcquireBackend(ctx, q, dbOp)
		if err != nil {
			b.Fatal(err)
		}
		apiLease, err := AcquireBackend(ctx, q, apiOp)
		if err != nil {
			dbLease.Release()
			b.Fatal(err)
		}
		dbLease.Release()
		apiLease.Release()
	}
}

func BenchmarkBackendHooksDisabled(b *testing.B) {
	BenchmarkBackendAcquireReleaseEnabledUncontended(b)
}

func BenchmarkBackendHooksEnabled(b *testing.B) {
	cfg := newTestConfig()
	cfg.BackendResources = testBackendResourceConfig()
	cfg.Observability.EnableHooks = true
	cfg.Observability.Hooks.Backend.OnBackendAdmission = func(BackendAdmissionDecision) {}
	cfg.Observability.Hooks.Backend.OnBackendReleased = func(BackendReleaseEvent) {}
	q := benchmarkBackendQueue(b, cfg)
	op := BackendOperation{Resource: "primary-db", Lane: BackendLaneDBRead}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lease, err := AcquireBackend(ctx, q, op)
		if err != nil {
			b.Fatal(err)
		}
		lease.Release()
	}
}
