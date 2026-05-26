// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"database/sql"
	"testing"
)

func BenchmarkSQLDBPressureAdapter(b *testing.B) {
	adapter := SQLDBPressureAdapter{
		Resource: "primary-db",
		Lane:     BackendLaneDBRead,
		DB: fakeSQLDBStats{stats: sql.DBStats{
			InUse:              4,
			Idle:               2,
			MaxOpenConnections: 8,
		}},
	}
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = adapter.BackendPressure(ctx)
	}
}

func BenchmarkAPIClientPressureAdapter(b *testing.B) {
	adapter := APIClientPressureAdapter{
		Resource: "wallet-api",
		Lane:     BackendLaneExternalAPI,
		Reader:   fakeResourcePressureReader{inUse: 3, capacity: 10},
	}
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = adapter.BackendPressure(ctx)
	}
}

func BenchmarkBackendPressureSnapshotCollection(b *testing.B) {
	cfg := newTestConfig()
	cfg.BackendResources.PressureProviders = []BackendPressureProvider{
		SQLDBPressureAdapter{Resource: "primary-db", Lane: BackendLaneDBRead, DB: fakeSQLDBStats{stats: sql.DBStats{MaxOpenConnections: 8}}},
		APIClientPressureAdapter{Resource: "wallet-api", Lane: BackendLaneExternalAPI, Reader: fakeResourcePressureReader{inUse: 1, capacity: 4}},
	}
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = q.BackendPressure(ctx)
	}
}

func BenchmarkBackendPressureHookDisabled(b *testing.B) {
	cfg := newTestConfig()
	cfg.Observability.EnableHooks = false
	cfg.BackendResources.PressureProviders = []BackendPressureProvider{
		staticPressureProvider{snap: BackendPressureSnapshot{Resource: "primary-db", Lane: BackendLaneDBRead, InUse: 1, Capacity: 4}},
	}
	q, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = q.BackendPressure(ctx)
	}
}
