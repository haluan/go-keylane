// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "testing"

func TestPressureZeroCapacity(t *testing.T) {
	p := classifyPressure(5, 0, 0)
	if p.TotalDepthRatio != 0 {
		t.Errorf("ratio = %v, want 0", p.TotalDepthRatio)
	}
	if p.IsHealthy || p.IsPressured || p.IsOverloaded {
		t.Errorf("expected no pressure flags when capacity is zero")
	}
}

func TestSafeRatio(t *testing.T) {
	if safeRatio(5, 10) != 0.5 {
		t.Errorf("got %v", safeRatio(5, 10))
	}
	if safeRatio(5, 0) != 0 {
		t.Errorf("got %v", safeRatio(5, 0))
	}
}
