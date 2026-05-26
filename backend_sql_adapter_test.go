// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

type fakeSQLDBStats struct {
	stats sql.DBStats
}

func (f fakeSQLDBStats) Stats() sql.DBStats { return f.stats }

func TestSQLDBPressureAdapterMapsStats(t *testing.T) {
	adapter := SQLDBPressureAdapter{
		Resource: "primary-db",
		Lane:     BackendLaneDBRead,
		DB: fakeSQLDBStats{stats: sql.DBStats{
			InUse:              4,
			Idle:               2,
			MaxOpenConnections: 8,
			WaitCount:          3,
			WaitDuration:       50 * time.Millisecond,
		}},
	}
	s := adapter.BackendPressure(context.Background())
	if s.InUse != 4 || s.Idle != 2 || s.Capacity != 8 {
		t.Fatalf("snapshot = %+v", s)
	}
	if s.WaitCount != 3 || s.WaitTime != 50*time.Millisecond {
		t.Fatalf("wait = %+v", s)
	}
	if s.Saturated || s.Pressure != 0.5 {
		t.Fatalf("saturated=%v pressure=%v", s.Saturated, s.Pressure)
	}
}

func TestSQLDBPressureAdapterUnboundedMaxOpen(t *testing.T) {
	adapter := SQLDBPressureAdapter{
		Resource: "primary-db",
		Lane:     BackendLaneDBRead,
		DB: fakeSQLDBStats{stats: sql.DBStats{
			InUse:              10,
			MaxOpenConnections: 0,
		}},
	}
	s := adapter.BackendPressure(context.Background())
	if s.Capacity != 0 || s.Pressure != 0 || s.Saturated {
		t.Fatalf("snapshot = %+v", s)
	}
}

func TestSQLDBPressureAdapterSaturationAtCap(t *testing.T) {
	adapter := SQLDBPressureAdapter{
		Resource: "primary-db",
		Lane:     BackendLaneDBWrite,
		DB: fakeSQLDBStats{stats: sql.DBStats{
			InUse:              5,
			MaxOpenConnections: 5,
		}},
	}
	s := adapter.BackendPressure(context.Background())
	if !s.Saturated || s.Pressure != 1 {
		t.Fatalf("snapshot = %+v", s)
	}
}

func TestSQLDBPressureAdapterNilDB(t *testing.T) {
	adapter := SQLDBPressureAdapter{Resource: "primary-db", Lane: BackendLaneDBRead}
	s := adapter.BackendPressure(context.Background())
	if s.Resource != "primary-db" || s.InUse != 0 {
		t.Fatalf("snapshot = %+v", s)
	}
}
