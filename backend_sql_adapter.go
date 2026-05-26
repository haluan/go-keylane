// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"database/sql"
)

// SQLDBStatsReader is implemented by *sql.DB and test fakes.
type SQLDBStatsReader interface {
	Stats() sql.DBStats
}

// SQLDBPressureAdapter maps database/sql pool stats into backend pressure snapshots.
type SQLDBPressureAdapter struct {
	Resource BackendResourceName
	Lane     BackendLane
	DB       SQLDBStatsReader
}

func (a SQLDBPressureAdapter) BackendPressure(ctx context.Context) BackendPressureSnapshot {
	_ = ctx
	snap := BackendPressureSnapshot{
		Resource: a.Resource,
		Lane:     a.Lane,
	}
	if a.DB == nil {
		return normalizeBackendPressureSnapshot(snap)
	}
	st := a.DB.Stats()
	snap.InUse = st.InUse
	snap.Idle = st.Idle
	snap.WaitCount = uint64(st.WaitCount)
	snap.WaitTime = st.WaitDuration
	if st.MaxOpenConnections > 0 {
		snap.Capacity = st.MaxOpenConnections
		snap.Saturated = st.InUse >= st.MaxOpenConnections
		snap.Pressure = float64(st.InUse) / float64(st.MaxOpenConnections)
	}
	return normalizeBackendPressureSnapshot(snap)
}
