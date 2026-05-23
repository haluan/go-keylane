// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestValidateAdaptiveQuotaRejectsInvalidInterval(t *testing.T) {
	err := ValidateAdaptiveQuotaPolicy(AdaptiveQuotaPolicy{
		Config: AdaptiveQuotaConfig{Enabled: true, EvaluationInterval: 0},
	}, map[Lane]int{"default": 1})
	if !errors.Is(err, ErrInvalidAdaptiveQuotaConfig) {
		t.Fatalf("err = %v, want ErrInvalidAdaptiveQuotaConfig", err)
	}
}

func TestValidateAdaptiveQuotaRejectsInvalidMinMaxBounds(t *testing.T) {
	err := ValidateAdaptiveQuotaPolicy(AdaptiveQuotaPolicy{
		Config: AdaptiveQuotaConfig{
			Enabled: true, EvaluationInterval: time.Second, MaxAdjustmentsPerTick: 1,
		},
		Lanes: []LaneAdaptivePolicy{
			{Lane: "default", MinQuota: 0, MaxQuota: 2},
		},
	}, map[Lane]int{"default": 1})
	if !errors.Is(err, ErrInvalidAdaptiveQuotaConfig) {
		t.Fatalf("MinQuota=0: err = %v", err)
	}
	err = ValidateAdaptiveQuotaPolicy(AdaptiveQuotaPolicy{
		Config: AdaptiveQuotaConfig{
			Enabled: true, EvaluationInterval: time.Second, MaxAdjustmentsPerTick: 1,
		},
		Lanes: []LaneAdaptivePolicy{
			{Lane: "default", MinQuota: 3, MaxQuota: 2},
		},
	}, map[Lane]int{"default": 1})
	if !errors.Is(err, ErrInvalidAdaptiveQuotaConfig) {
		t.Fatalf("Max<Min: err = %v", err)
	}
}

func TestValidateAdaptiveQuotaRejectsInvalidPressure(t *testing.T) {
	err := ValidateAdaptiveQuotaPolicy(AdaptiveQuotaPolicy{
		Config: AdaptiveQuotaConfig{
			Enabled: true, EvaluationInterval: time.Second,
			PressureLow: 0.90, PressureHigh: 0.70, MaxAdjustmentsPerTick: 1,
		},
	}, map[Lane]int{"default": 1})
	if !errors.Is(err, ErrInvalidAdaptiveQuotaConfig) {
		t.Fatalf("err = %v", err)
	}
}

func TestValidateAdaptiveQuotaRejectsNegativeConfigWhenEnabled(t *testing.T) {
	base := AdaptiveQuotaConfig{
		Enabled: true, EvaluationInterval: time.Second,
		MaxAdjustmentsPerTick: 1,
	}
	cases := []struct {
		name string
		cfg  AdaptiveQuotaConfig
	}{
		{"PressureLow", func() AdaptiveQuotaConfig { c := base; c.PressureLow = -0.1; return c }()},
		{"PressureHigh", func() AdaptiveQuotaConfig { c := base; c.PressureHigh = -0.1; return c }()},
		{"IncreaseStep", func() AdaptiveQuotaConfig { c := base; c.IncreaseStep = -1; return c }()},
		{"DecreaseStep", func() AdaptiveQuotaConfig { c := base; c.DecreaseStep = -1; return c }()},
		{"MaxAdjustmentsPerTick zero", func() AdaptiveQuotaConfig { c := base; c.MaxAdjustmentsPerTick = 0; return c }()},
		{"MaxAdjustmentsPerTick negative", func() AdaptiveQuotaConfig { c := base; c.MaxAdjustmentsPerTick = -1; return c }()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateAdaptiveQuotaPolicy(AdaptiveQuotaPolicy{Config: tc.cfg}, map[Lane]int{"default": 1})
			if !errors.Is(err, ErrInvalidAdaptiveQuotaConfig) {
				t.Fatalf("err = %v, want ErrInvalidAdaptiveQuotaConfig", err)
			}
		})
	}
}

func TestValidateAdaptiveQuotaRejectsUnknownLane(t *testing.T) {
	err := ValidateAdaptiveQuotaPolicy(AdaptiveQuotaPolicy{
		Config: AdaptiveQuotaConfig{
			Enabled: true, EvaluationInterval: time.Second, MaxAdjustmentsPerTick: 1,
		},
		Lanes: []LaneAdaptivePolicy{{Lane: "unknown", MinQuota: 1, MaxQuota: 2}},
	}, map[Lane]int{"default": 1})
	if !errors.Is(err, ErrInvalidAdaptiveQuotaConfig) {
		t.Fatalf("err = %v", err)
	}
}

func TestUpdateLaneQuotaUpdatesSingleLane(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 2, "fast": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	v, err := q.UpdateLaneQuota("default", 5)
	if err != nil {
		t.Fatal(err)
	}
	snap := q.CurrentQuotaPolicy()
	if snap.LaneQuotas["default"] != 5 {
		t.Errorf("default quota = %d, want 5", snap.LaneQuotas["default"])
	}
	if snap.Version != v {
		t.Errorf("version = %d, want %d", snap.Version, v)
	}
}

func TestAdaptiveQuotaSnapshotDisabled(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	snap := q.AdaptiveQuotaSnapshot()
	if snap.Enabled || snap.Running {
		t.Fatalf("disabled snapshot = %+v", snap)
	}
	_ = q.CurrentQuotaPolicy()
}

func TestAdaptiveQuotaDisabledNoGoroutine(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	snap := q.AdaptiveQuotaSnapshot()
	if snap.Enabled {
		t.Error("want disabled")
	}
	if snap.Running {
		t.Error("want not running")
	}
}

func TestAdaptiveQuotaControllerStartStop(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas:    map[Lane]int{"default": 2, "background": 1},
		Observability: DefaultObservabilityConfig(),
		AdaptiveQuota: AdaptiveQuotaPolicy{
			Config: AdaptiveQuotaConfig{
				Enabled:               true,
				EvaluationInterval:    20 * time.Millisecond,
				WarmupDuration:        time.Nanosecond,
				CooldownDuration:      10 * time.Millisecond,
				PressureHigh:          0.85,
				PressureLow:           0.60,
				IncreaseStep:          1,
				DecreaseStep:          1,
				MaxAdjustmentsPerTick: 1,
			},
			Lanes: []LaneAdaptivePolicy{
				{Lane: "default", Class: LaneNormal, Enabled: true, MinQuota: 1, MaxQuota: 8, AllowIncrease: true, AllowDecrease: true},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	snap := q.AdaptiveQuotaSnapshot()
	if !snap.Enabled || !snap.Running {
		t.Fatalf("snapshot = %+v", snap)
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer stopCancel()
	if err := q.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	snap = q.AdaptiveQuotaSnapshot()
	if snap.Running {
		t.Error("want stopped")
	}
}

func TestAdaptiveQuotaConcurrentSubmitAndUpdate(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 2, QueueSizePerLane: 20,
		LaneQuotas:    map[Lane]int{"default": 2},
		Observability: DefaultObservabilityConfig(),
		AdaptiveQuota: AdaptiveQuotaPolicy{
			Config: AdaptiveQuotaConfig{
				Enabled:               true,
				EvaluationInterval:    5 * time.Millisecond,
				WarmupDuration:        time.Nanosecond,
				CooldownDuration:      1 * time.Millisecond,
				MaxAdjustmentsPerTick: 1,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	defer func() {
		stopCtx, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		_ = q.Stop(stopCtx)
	}()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = q.Submit(ctx, Job{Key: "k", Lane: "default", Run: func(context.Context) error { return nil }})
		}()
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = q.UpdateQuotaPolicy(QuotaPolicy{DefaultQuota: 2, LaneQuotas: map[Lane]uint32{"default": 2}})
			_, _ = q.UpdateLaneQuota("default", 2)
		}()
	}
	wg.Wait()
}

func adaptiveQuotaTestQueue(t *testing.T) *Queue {
	t.Helper()
	obs := DefaultObservabilityConfig()
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 2, QueueSizePerLane: 64,
		LaneQuotas:    map[Lane]int{"default": 2, "critical": 2, "best_effort": 1},
		Observability: obs,
		AdaptiveQuota: AdaptiveQuotaPolicy{
			Config: AdaptiveQuotaConfig{
				Enabled:               true,
				EvaluationInterval:    10 * time.Millisecond,
				WarmupDuration:        time.Nanosecond,
				CooldownDuration:      5 * time.Millisecond,
				PressureHigh:          0.85,
				PressureLow:           0.60,
				IncreaseStep:          1,
				DecreaseStep:          1,
				MaxAdjustmentsPerTick: 1,
			},
			Lanes: []LaneAdaptivePolicy{
				{Lane: "critical", Class: LaneCritical, Enabled: true, MinQuota: 1, MaxQuota: 8, AllowIncrease: true, AllowDecrease: false},
				{Lane: "best_effort", Class: LaneBestEffort, Enabled: true, MinQuota: 1, MaxQuota: 4, AllowIncrease: false, AllowDecrease: true},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func TestAdaptiveQuotaStopIdempotent(t *testing.T) {
	// Queue-level smoke: scheduler Stop tolerates double call. Adaptive controller
	// idempotency is covered by core.TestAdaptiveQuotaControllerStopIdempotent.
	q := adaptiveQuotaTestQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer stopCancel()
	if err := q.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	if err := q.Stop(stopCtx); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}

func TestAdaptiveQuotaSnapshotConcurrentRead(t *testing.T) {
	q := adaptiveQuotaTestQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	defer func() {
		stopCtx, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		_ = q.Stop(stopCtx)
	}()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = q.AdaptiveQuotaSnapshot()
			_ = q.CurrentQuotaPolicy()
		}()
	}
	wg.Wait()
}

func TestAdaptiveQuotaSnapshotPolicyVersion(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 2},
		AdaptiveQuota: AdaptiveQuotaPolicy{
			Config: AdaptiveQuotaConfig{
				Enabled: true, EvaluationInterval: time.Second, MaxAdjustmentsPerTick: 1,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	snap := q.AdaptiveQuotaSnapshot()
	if !snap.Enabled {
		t.Fatal("want enabled")
	}
	if snap.PolicyVersion == 0 {
		t.Error("PolicyVersion should be non-zero when controller is active")
	}
}

func TestUpdateLaneQuotaIfVersionRejectsStaleVersion(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = q.UpdateQuotaPolicy(QuotaPolicy{DefaultQuota: 2, LaneQuotas: map[Lane]uint32{"default": 3}})
	_, err = q.UpdateLaneQuotaIfVersion("default", 4, 0)
	if !errors.Is(err, ErrQuotaPolicyVersionConflict) {
		t.Fatalf("err = %v, want ErrQuotaPolicyVersionConflict", err)
	}
}

func TestAdaptiveQuotaEmitsDecisionHook(t *testing.T) {
	var events atomic.Int32
	obs := DefaultObservabilityConfig()
	obs.Hooks.OnAdaptiveQuotaDecision = func(e AdaptiveQuotaEvent) {
		if e.Action != QuotaAdjustmentHold {
			events.Add(1)
		}
	}
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas:      map[Lane]int{"default": 2, "critical": 2, "best_effort": 3},
		Observability:   obs,
		OverloadEnabled: true,
		AdaptiveQuota: AdaptiveQuotaPolicy{
			Config: AdaptiveQuotaConfig{
				Enabled: true, EvaluationInterval: 5 * time.Millisecond,
				WarmupDuration: time.Nanosecond, CooldownDuration: 5 * time.Millisecond,
				PressureHigh: 0.85, PressureLow: 0.60, DecreaseStep: 1,
				MaxAdjustmentsPerTick: 1,
			},
			Lanes: []LaneAdaptivePolicy{
				{Lane: "best_effort", Class: LaneBestEffort, Enabled: true, MinQuota: 1, MaxQuota: 4, AllowDecrease: true},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{Class: LaneNormal, RejectAboveRatio: 0.90, MaxQueueDepth: 100},
		Lanes: []LaneOverloadPolicy{
			{Lane: "best_effort", Class: LaneBestEffort, RejectAboveRatio: 0.75, ShedAboveRatio: 0.50, MaxQueueDepth: 100},
		},
	})
	for _, lane := range []Lane{"default", "critical", "best_effort"} {
		for i := 0; i < 8; i++ {
			_ = q.Submit(context.Background(), Job{Key: "k", Lane: lane, Run: func(context.Context) error { return nil }})
		}
	}
	if err := CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "best_effort"}); !errors.Is(err, ErrOverloadShed) {
		t.Fatalf("CheckOverload = %v, want ErrOverloadShed", err)
	}
	if q.adaptive == nil {
		t.Fatal("adaptive controller nil")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	time.Sleep(q.config.AdaptiveQuota.Config.WarmupDuration + time.Nanosecond)
	q.adaptive.RunTick()
	if events.Load() == 0 {
		t.Fatal("expected adaptive quota decision hook after evaluation tick")
	}
	snap := q.AdaptiveQuotaSnapshot()
	if snap.PolicyVersion == 0 {
		t.Error("snapshot PolicyVersion should be set")
	}
	if q.CurrentQuotaPolicy().LaneQuotas["best_effort"] != 2 {
		t.Fatalf("quota after tick = %v", q.CurrentQuotaPolicy().LaneQuotas)
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer stopCancel()
	_ = q.Stop(stopCtx)
}

func TestAdaptiveQuotaConfigNormalizedOnInit(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 1},
		AdaptiveQuota: AdaptiveQuotaPolicy{
			Config: AdaptiveQuotaConfig{
				Enabled: true, EvaluationInterval: time.Second, MaxAdjustmentsPerTick: 1,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !q.config.AdaptiveQuota.Config.EnableIncrease || !q.config.AdaptiveQuota.Config.EnableDecrease {
		t.Fatalf("enable flags not normalized: %+v", q.config.AdaptiveQuota.Config)
	}
	def := DefaultAdaptiveQuotaConfig()
	if q.config.AdaptiveQuota.Config.WarmupDuration != def.WarmupDuration {
		t.Errorf("WarmupDuration = %v, want %v", q.config.AdaptiveQuota.Config.WarmupDuration, def.WarmupDuration)
	}
	if q.config.AdaptiveQuota.Config.CooldownDuration != def.CooldownDuration {
		t.Errorf("CooldownDuration = %v, want %v", q.config.AdaptiveQuota.Config.CooldownDuration, def.CooldownDuration)
	}
}

func TestAdaptiveQuotaWithAdmissionPolicy(t *testing.T) {
	q := adaptiveQuotaTestQueue(t)
	_, _ = q.UpdateAdmissionPolicy(AdmissionPolicy{
		DefaultClass: LaneNormal, DefaultRejectAboveRatio: 0.99, DefaultMaxQueueDepth: 100,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	_ = q.Submit(ctx, Job{Key: "k", Lane: "default", Run: func(context.Context) error { return nil }})
	stopCtx, c := context.WithTimeout(context.Background(), 3*time.Second)
	defer c()
	_ = q.Stop(stopCtx)
}

func TestAdaptiveQuotaSubmitNotBlocked(t *testing.T) {
	q := adaptiveQuotaTestQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	defer func() {
		stopCtx, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		_ = q.Stop(stopCtx)
	}()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			if err := q.Submit(ctx, Job{Key: "k", Lane: "default", Run: func(context.Context) error { return nil }}); err != nil {
				t.Errorf("Submit: %v", err)
				return
			}
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("submits blocked too long during adaptive ticks")
	}
}

func TestAdaptiveQuotaConcurrentSnapshotAndUpdate(t *testing.T) {
	q := adaptiveQuotaTestQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	defer func() {
		stopCtx, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		_ = q.Stop(stopCtx)
	}()

	var wg sync.WaitGroup
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = q.CurrentQuotaPolicy()
			_ = q.AdaptiveQuotaSnapshot()
		}()
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = q.UpdateLaneQuota("default", 2)
		}()
	}
	wg.Wait()
}

func TestAdaptiveQuotaConcurrentStopAndTick(t *testing.T) {
	q := adaptiveQuotaTestQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = q.Submit(ctx, Job{Key: "k", Lane: "default", Run: func(context.Context) error { return nil }})
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		stopCtx, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		_ = q.Stop(stopCtx)
	}()
	wg.Wait()
}

func TestAdaptiveQuotaConcurrentHookAndStop(t *testing.T) {
	var hookCalls atomic.Int32
	obs := DefaultObservabilityConfig()
	obs.Hooks.OnAdaptiveQuotaDecision = func(AdaptiveQuotaEvent) { hookCalls.Add(1) }
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas:    map[Lane]int{"default": 2},
		Observability: obs,
		AdaptiveQuota: AdaptiveQuotaPolicy{
			Config: AdaptiveQuotaConfig{
				Enabled: true, EvaluationInterval: 5 * time.Millisecond,
				WarmupDuration: time.Nanosecond, MaxAdjustmentsPerTick: 1,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	time.Sleep(30 * time.Millisecond)
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer stopCancel()
	_ = q.Stop(stopCtx)
	_ = hookCalls.Load()
}

func TestAdaptiveQuotaRepeatedStartStopNoLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	q := adaptiveQuotaTestQueue(t)
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		_ = q.Start(ctx)
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = q.Stop(stopCtx)
		stopCancel()
		cancel()
	}
	eventuallyNoGoroutineGrowth(t, before, 4)
}

func TestAdaptiveQuotaCanceledContextStops(t *testing.T) {
	q := adaptiveQuotaTestQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	_ = q.Start(ctx)
	cancel()
	time.Sleep(50 * time.Millisecond)
	snap := q.AdaptiveQuotaSnapshot()
	if snap.Running {
		t.Error("want controller stopped after context cancel")
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer stopCancel()
	_ = q.Stop(stopCtx)
}

func TestAdaptiveQuotaWorksWithOverloadCounters(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas:      map[Lane]int{"default": 2, "best_effort": 1},
		Observability:   DefaultObservabilityConfig(),
		OverloadEnabled: true,
		AdaptiveQuota: AdaptiveQuotaPolicy{
			Config: AdaptiveQuotaConfig{
				Enabled: true, EvaluationInterval: 10 * time.Millisecond,
				WarmupDuration: time.Nanosecond, CooldownDuration: 10 * time.Millisecond,
				PressureHigh: 0.85, PressureLow: 0.60, DecreaseStep: 1,
				MaxAdjustmentsPerTick: 1,
			},
			Lanes: []LaneAdaptivePolicy{
				{Lane: "best_effort", Class: LaneBestEffort, Enabled: true, MinQuota: 1, MaxQuota: 4, AllowDecrease: true},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{Class: LaneNormal, RejectAboveRatio: 0.90, MaxQueueDepth: 100},
		Lanes: []LaneOverloadPolicy{
			{Lane: "best_effort", Class: LaneBestEffort, RejectAboveRatio: 0.75, ShedAboveRatio: 0.50, MaxQueueDepth: 100},
		},
	})
	for _, lane := range []Lane{"default", "best_effort"} {
		for i := 0; i < 8; i++ {
			_ = q.Submit(context.Background(), Job{Key: "k", Lane: lane, Run: func(context.Context) error { return nil }})
		}
	}
	_ = CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "best_effort"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	stopCtx, c := context.WithTimeout(context.Background(), 3*time.Second)
	defer c()
	_ = q.Stop(stopCtx)
}
