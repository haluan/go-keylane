// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"sync/atomic"
	"testing"
)

func TestResolveObservabilityConfigPreservesOnQuotaChangeHook(t *testing.T) {
	var called atomic.Int32
	in := ObservabilityConfig{
		Hooks: Hooks{
			OnQuotaChange: func(QuotaChangeEvent) { called.Add(1) },
		},
	}
	got := ResolveObservabilityConfig(in)
	if got.Hooks.OnQuotaChange == nil {
		t.Fatal("OnQuotaChange hook discarded")
	}
	if !got.EnableHooks {
		t.Error("EnableHooks = false, want true when v0.4 hook is set")
	}
	got.Hooks.OnQuotaChange(QuotaChangeEvent{})
	if called.Load() != 1 {
		t.Error("resolved hook was not the user hook")
	}
}

func TestResolveObservabilityConfigPreservesAdaptiveTracingFlag(t *testing.T) {
	got := ResolveObservabilityConfig(ObservabilityConfig{
		EnableAdaptiveDecisionTracing: true,
	})
	if !got.EnableAdaptiveDecisionTracing {
		t.Error("EnableAdaptiveDecisionTracing reset to false")
	}
	if !got.EnableHooks {
		t.Error("EnableHooks = false, want true when tracing flag is set")
	}
}

func TestNewPreservesV04HookWithoutLegacyFields(t *testing.T) {
	var events atomic.Int32
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 2},
		Observability: ObservabilityConfig{
			Hooks: Hooks{
				OnQuotaChange: func(QuotaChangeEvent) { events.Add(1) },
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.UpdateLaneQuota("default", 3); err != nil {
		t.Fatal(err)
	}
	if events.Load() != 1 {
		t.Fatalf("events = %d, want 1", events.Load())
	}
}
