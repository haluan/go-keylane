// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/haluan/go-keylane/internal/core"
)

func v05HooksObservability() ObservabilityConfig {
	obs := DefaultObservabilityConfig()
	obs.EnableHooks = true
	return obs
}

func TestV05HooksHotKeyCandidateNoRawKey(t *testing.T) {
	cfg := v05EnabledConfig()
	cfg.ShardCount = 1
	cfg.PerKeyAdmission.Enabled = false
	cfg.Observability = v05HooksObservability()
	var hotKeyEvents int32
	cfg.Observability.Hooks.OnHotKeyCandidate = func(e HotKeyCandidateEvent) {
		atomic.AddInt32(&hotKeyEvents, 1)
		if e.Candidate.Key != "" {
			t.Error("hook must not expose raw key by default")
		}
		if e.Candidate.KeyHash == 0 {
			t.Error("expected key hash in hook event")
		}
	}
	q, block := newV05ScenarioQueue(t, cfg)
	for i := 0; i < 35; i++ {
		_ = q.Submit(context.Background(), Job{
			Key: "hook-hot-key", Lane: "default",
			Run: blockedRun(block),
		})
	}
	_ = q.DebugSnapshot()
	if atomic.LoadInt32(&hotKeyEvents) == 0 {
		t.Fatal("expected OnHotKeyCandidate hook to fire")
	}
}

func TestV05HooksScaleSignalAndShardPressure(t *testing.T) {
	cfg := v05EnabledConfig()
	cfg.Observability = v05HooksObservability()
	var scaleEvents, pressureEvents int32
	cfg.Observability.Hooks.OnScaleSignal = func(e ScaleSignalEvent) {
		atomic.AddInt32(&scaleEvents, 1)
		if !e.Signal.DiagnosticsEnabled {
			t.Error("expected diagnostics enabled in scale hook")
		}
	}
	cfg.Observability.Hooks.OnShardPressureSummary = func(e ShardPressureSummaryEvent) {
		atomic.AddInt32(&pressureEvents, 1)
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = q.ScaleSignal()
	_ = q.PressureSummary()
	if atomic.LoadInt32(&scaleEvents) == 0 {
		t.Fatal("expected OnScaleSignal hook")
	}
	if atomic.LoadInt32(&pressureEvents) == 0 {
		t.Fatal("expected OnShardPressureSummary hook")
	}
}

func TestV05HooksDisabledNoFire(t *testing.T) {
	cfg := v05EnabledConfig()
	cfg.Observability = DefaultObservabilityConfig()
	cfg.Observability.EnableHooks = false
	var fired int32
	cfg.Observability.Hooks.OnScaleSignal = func(ScaleSignalEvent) {
		atomic.AddInt32(&fired, 1)
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = q.ScaleSignal()
	if atomic.LoadInt32(&fired) != 0 {
		t.Fatal("hooks disabled should not fire")
	}
}

func TestV05HooksPerKeyAdmissionDecisionNoRawKey(t *testing.T) {
	cfg := v05EnabledConfig()
	cfg.ShardCount = 1
	cfg.Observability = v05HooksObservability()
	var decisions int32
	cfg.Observability.Hooks.OnPerKeyAdmissionDecision = func(e PerKeyAdmissionDecisionEvent) {
		atomic.AddInt32(&decisions, 1)
		if e.Decision.KeyHash == 0 {
			t.Error("expected key hash in per-key admission hook")
		}
	}
	q, _, block := startV05ScenarioQueue(t, cfg)
	for i := 0; i < 40; i++ {
		_ = q.Submit(context.Background(), Job{
			Key: "perkey-hook-hot", Lane: "default",
			Run: blockedRun(block),
		})
	}
	if atomic.LoadInt32(&decisions) == 0 {
		t.Fatal("expected OnPerKeyAdmissionDecision hook to fire under throttle")
	}
}

func TestV05HooksHotKeyHashMatchesCore(t *testing.T) {
	cfg := v05EnabledConfig()
	cfg.ShardCount = 1
	cfg.PerKeyAdmission.Enabled = false
	cfg.Observability = v05HooksObservability()
	wantHash := core.HashKey("tracked-key")
	var saw bool
	cfg.Observability.Hooks.OnHotKeyCandidate = func(e HotKeyCandidateEvent) {
		if e.Candidate.KeyHash == wantHash {
			saw = true
		}
	}
	q, block := newV05ScenarioQueue(t, cfg)
	for i := 0; i < 30; i++ {
		_ = q.Submit(context.Background(), Job{
			Key: "tracked-key", Lane: "default", Run: blockedRun(block),
		})
	}
	_ = q.DebugSnapshot()
	if !saw {
		t.Fatal("expected hook for tracked key hash")
	}
}

func v05PanickingHook() {
	panic("observer panic")
}

func TestV05HooksPanicDoesNotBreakDebugSnapshot(t *testing.T) {
	cfg := v05EnabledConfig()
	cfg.ShardCount = 1
	cfg.PerKeyAdmission.Enabled = false
	cfg.Observability = v05HooksObservability()
	cfg.Observability.Hooks.OnHotKeyCandidate = func(HotKeyCandidateEvent) { v05PanickingHook() }
	q, block := newV05ScenarioQueue(t, cfg)
	for i := 0; i < 35; i++ {
		_ = q.Submit(context.Background(), Job{
			Key: "panic-hot-key", Lane: "default",
			Run: blockedRun(block),
		})
	}
	snap := q.DebugSnapshot()
	if snap.ShardCount == 0 {
		t.Fatal("expected non-empty debug snapshot after hook panic")
	}
}

func TestV05HooksPanicDoesNotBreakScaleSignal(t *testing.T) {
	cfg := v05EnabledConfig()
	cfg.Observability = v05HooksObservability()
	cfg.Observability.Hooks.OnScaleSignal = func(ScaleSignalEvent) { v05PanickingHook() }
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = q.ScaleSignal()
}

func TestV05HooksPanicDoesNotBreakPressureSummary(t *testing.T) {
	cfg := v05EnabledConfig()
	cfg.Observability = v05HooksObservability()
	cfg.Observability.Hooks.OnShardPressureSummary = func(ShardPressureSummaryEvent) { v05PanickingHook() }
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = q.PressureSummary()
}

func TestV05HooksPanicDoesNotBreakPerKeyAdmission(t *testing.T) {
	cfg := v05EnabledConfig()
	cfg.ShardCount = 1
	cfg.Observability = v05HooksObservability()
	cfg.Observability.Hooks.OnPerKeyAdmissionDecision = func(PerKeyAdmissionDecisionEvent) { v05PanickingHook() }
	q, _, block := startV05ScenarioQueue(t, cfg)
	var gotPerKeyErr bool
	for i := 0; i < 40; i++ {
		err := q.Submit(context.Background(), Job{
			Key: "perkey-panic-hot", Lane: "default",
			Run: blockedRun(block),
		})
		var perr PerKeyAdmissionError
		if errors.As(err, &perr) {
			gotPerKeyErr = true
		}
	}
	if !gotPerKeyErr {
		t.Fatal("expected per-key admission error from Submit after hook panic")
	}
}
