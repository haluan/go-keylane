// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"testing"
	"time"
)

type fakeResourcePressureReader struct {
	inUse, capacity int
	waitCount       uint64
	waitTime        time.Duration
	saturated       bool
}

func (f fakeResourcePressureReader) InUse() int              { return f.inUse }
func (f fakeResourcePressureReader) Capacity() int           { return f.capacity }
func (f fakeResourcePressureReader) WaitCount() uint64       { return f.waitCount }
func (f fakeResourcePressureReader) WaitTime() time.Duration { return f.waitTime }
func (f fakeResourcePressureReader) Saturated() bool         { return f.saturated }

func TestAPIClientPressureAdapterMapsReader(t *testing.T) {
	adapter := APIClientPressureAdapter{
		Resource: "wallet-api",
		Lane:     BackendLaneExternalAPI,
		Reader: fakeResourcePressureReader{
			inUse: 7, capacity: 10, waitCount: 2,
			waitTime: 25 * time.Millisecond, saturated: true,
		},
	}
	s := adapter.BackendPressure(context.Background())
	if s.InUse != 7 || s.Capacity != 10 || s.WaitCount != 2 {
		t.Fatalf("snapshot = %+v", s)
	}
	if !s.Saturated || s.Pressure != 0.7 {
		t.Fatalf("saturated=%v pressure=%v", s.Saturated, s.Pressure)
	}
}

func TestAPIClientPressureAdapterNegativeWaitTimeClamped(t *testing.T) {
	adapter := APIClientPressureAdapter{
		Resource: "wallet-api",
		Lane:     BackendLaneExternalAPI,
		Reader: fakeResourcePressureReader{
			inUse: 1, capacity: 4, waitTime: -time.Second,
		},
	}
	s := adapter.BackendPressure(context.Background())
	if s.WaitTime != 0 {
		t.Fatalf("WaitTime = %v", s.WaitTime)
	}
}

func TestAPIClientPressureAdapterNilReader(t *testing.T) {
	adapter := APIClientPressureAdapter{
		Resource: "wallet-api",
		Lane:     BackendLaneExternalAPI,
	}
	s := adapter.BackendPressure(context.Background())
	if s.Resource != "wallet-api" || s.Lane != BackendLaneExternalAPI {
		t.Fatalf("snapshot = %+v", s)
	}
}
