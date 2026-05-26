// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"time"
)

// ResourcePressureReader exposes in-use/capacity metrics for a user-managed pool or semaphore.
type ResourcePressureReader interface {
	InUse() int
	Capacity() int
	WaitCount() uint64
	WaitTime() time.Duration
	Saturated() bool
}

// APIClientPressureAdapter maps custom API/HTTP pool metrics into backend pressure snapshots.
type APIClientPressureAdapter struct {
	Resource BackendResourceName
	Lane     BackendLane
	Reader   ResourcePressureReader
}

func (a APIClientPressureAdapter) BackendPressure(ctx context.Context) BackendPressureSnapshot {
	_ = ctx
	snap := BackendPressureSnapshot{
		Resource: a.Resource,
		Lane:     a.Lane,
	}
	if a.Reader == nil {
		return normalizeBackendPressureSnapshot(snap)
	}
	snap.InUse = a.Reader.InUse()
	snap.Capacity = a.Reader.Capacity()
	snap.WaitCount = a.Reader.WaitCount()
	snap.WaitTime = a.Reader.WaitTime()
	snap.Saturated = a.Reader.Saturated()
	if snap.Capacity > 0 {
		snap.Pressure = float64(snap.InUse) / float64(snap.Capacity)
	}
	return normalizeBackendPressureSnapshot(snap)
}
