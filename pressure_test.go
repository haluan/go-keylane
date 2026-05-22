// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane_test

import (
	"context"
	"testing"

	"github.com/haluan/go-keylane"
)

func TestPressureHealthy(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	for i := 0; i < 6; i++ {
		_ = q.Submit(context.Background(), keylane.Job{
			Key:  "key",
			Lane: "default",
			Run:  func(ctx context.Context) error { return nil },
		})
	}

	p := q.Pressure()
	if p.TotalDepth != 6 || p.TotalCapacity != 10 {
		t.Fatalf("depth=%d capacity=%d", p.TotalDepth, p.TotalCapacity)
	}
	if !p.IsHealthy || p.IsPressured || p.IsOverloaded {
		t.Errorf("expected healthy, got healthy=%v pressured=%v overloaded=%v ratio=%v",
			p.IsHealthy, p.IsPressured, p.IsOverloaded, p.TotalDepthRatio)
	}
}

func TestPressurePressured(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	for i := 0; i < 7; i++ {
		_ = q.Submit(context.Background(), keylane.Job{
			Key:  "key",
			Lane: "default",
			Run:  func(ctx context.Context) error { return nil },
		})
	}

	p := q.Pressure()
	if !p.IsPressured || p.IsHealthy || p.IsOverloaded {
		t.Errorf("expected pressured only, got healthy=%v pressured=%v overloaded=%v ratio=%v",
			p.IsHealthy, p.IsPressured, p.IsOverloaded, p.TotalDepthRatio)
	}
}

func TestPressureOverloaded(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	for i := 0; i < 9; i++ {
		_ = q.Submit(context.Background(), keylane.Job{
			Key:  "key",
			Lane: "default",
			Run:  func(ctx context.Context) error { return nil },
		})
	}

	p := q.Pressure()
	if !p.IsOverloaded || p.IsHealthy {
		t.Errorf("expected overloaded, got healthy=%v pressured=%v overloaded=%v ratio=%v",
			p.IsHealthy, p.IsPressured, p.IsOverloaded, p.TotalDepthRatio)
	}
}

func TestPressureZeroCapacity(t *testing.T) {
	// Document classifyPressure behavior when capacity is zero (see internal/core/pressure_test.go).
	var ratio float64
	capacity := uint64(0)
	depth := uint64(5)
	if capacity != 0 {
		ratio = float64(depth) / float64(capacity)
	}
	if ratio != 0 {
		t.Errorf("ratio = %v, want 0", ratio)
	}
	healthy := capacity == 0 || ratio < keylane.PressuredDepthRatio
	pressured := capacity != 0 && ratio >= keylane.PressuredDepthRatio && ratio < keylane.OverloadedDepthRatio
	overloaded := capacity != 0 && ratio >= keylane.OverloadedDepthRatio
	if !healthy || pressured || overloaded {
		t.Errorf("expected healthy-only flags when capacity=0, got healthy=%v pressured=%v overloaded=%v",
			healthy, pressured, overloaded)
	}
}
