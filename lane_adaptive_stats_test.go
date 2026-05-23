// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAdaptiveDebugSnapshotLaneStatsCopyOut(t *testing.T) {
	q := adaptiveQuotaTestQueue(t)
	snap1 := q.AdaptiveDebugSnapshot()
	if len(snap1.Lanes) == 0 {
		t.Fatal("expected lane stats")
	}
	snap1.Lanes[0].AdaptiveIncreaseTotal = 999
	snap2 := q.AdaptiveDebugSnapshot()
	if snap2.Lanes[0].AdaptiveIncreaseTotal == 999 {
		t.Error("snapshot leaked mutable lane stats slice elements")
	}
}

func TestLaneAdaptiveStatsIncrementOnAdaptiveDecrease(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas:      map[Lane]int{"best_effort": 3},
		OverloadEnabled: true,
		AdaptiveQuota: AdaptiveQuotaPolicy{
			Config: AdaptiveQuotaConfig{
				Enabled: true, EvaluationInterval: time.Second,
				WarmupDuration: time.Nanosecond, CooldownDuration: time.Millisecond,
				MaxAdjustmentsPerTick: 1, DecreaseStep: 1,
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
	_ = CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "best_effort"})
	if q.adaptive == nil {
		t.Fatal("adaptive nil")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	time.Sleep(2 * time.Nanosecond)
	q.adaptive.RunTick()
	snap := q.AdaptiveDebugSnapshot()
	var be LaneAdaptiveStats
	for _, l := range snap.Lanes {
		if l.Lane == "best_effort" {
			be = l
		}
	}
	if be.AdaptiveDecreaseTotal == 0 {
		t.Error("expected adaptive decrease counter increment")
	}
	if be.QuotaChangeTotal == 0 {
		t.Error("expected quota change counter increment")
	}
}

func TestAdaptiveHoldTracingEmitsDecisionHook(t *testing.T) {
	var holds atomic.Int32
	obs := DefaultObservabilityConfig()
	obs.EnableAdaptiveDecisionTracing = true
	obs.Hooks.OnAdaptiveQuotaDecision = func(e AdaptiveQuotaEvent) {
		if e.Action == QuotaAdjustmentHold {
			holds.Add(1)
		}
	}
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas:    map[Lane]int{"default": 2},
		Observability: obs,
		AdaptiveQuota: AdaptiveQuotaPolicy{
			Config: AdaptiveQuotaConfig{
				Enabled: true, EvaluationInterval: time.Second,
				WarmupDuration: time.Nanosecond, CooldownDuration: time.Second,
				PressureLow: 0.99, PressureHigh: 0.999,
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
	time.Sleep(2 * time.Nanosecond)
	if q.adaptive != nil {
		q.adaptive.RunTick()
	}
	if holds.Load() == 0 {
		t.Fatal("expected hold decision hook with tracing enabled")
	}
	stat := laneAdaptiveStat(t, q, "default")
	if stat.LastDecision != QuotaReasonInsufficientSamples {
		t.Errorf("LastDecision = %q, want %q", stat.LastDecision, QuotaReasonInsufficientSamples)
	}
}

func TestLaneStatsConcurrentRead(t *testing.T) {
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
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = q.AdaptiveDebugSnapshot()
		}()
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = q.Submit(ctx, Job{Key: "k", Lane: "default", Run: func(context.Context) error { return nil }})
		}()
	}
	wg.Wait()
}

func laneAdaptiveStat(t *testing.T, q *Queue, lane Lane) LaneAdaptiveStats {
	t.Helper()
	for _, l := range q.AdaptiveDebugSnapshot().Lanes {
		if l.Lane == lane {
			return l
		}
	}
	t.Fatalf("lane %q not found in adaptive stats", lane)
	return LaneAdaptiveStats{}
}

func TestAdaptiveHoldUpdatesLastDecisionWithoutTracing(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 2},
		AdaptiveQuota: AdaptiveQuotaPolicy{
			Config: AdaptiveQuotaConfig{
				Enabled: true, EvaluationInterval: time.Second,
				WarmupDuration: time.Nanosecond, CooldownDuration: time.Second,
				PressureLow: 0.99, PressureHigh: 0.999,
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
	time.Sleep(2 * time.Nanosecond)
	if q.adaptive != nil {
		q.adaptive.RunTick()
	}
	stat := laneAdaptiveStat(t, q, "default")
	if stat.LastDecision != QuotaReasonInsufficientSamples {
		t.Errorf("LastDecision = %q, want %q", stat.LastDecision, QuotaReasonInsufficientSamples)
	}
	if stat.AdaptiveHoldTotal != 0 {
		t.Errorf("AdaptiveHoldTotal = %d, want 0 without tracing", stat.AdaptiveHoldTotal)
	}
}

func TestAdmissionCounterKeepIncrements(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	before := laneAdaptiveStat(t, q, "default").KeepTotal
	if err := CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "default"}); err != nil {
		t.Fatal(err)
	}
	_ = q.Submit(context.Background(), Job{Key: "k", Lane: "default", Run: func(context.Context) error { return nil }})
	after := laneAdaptiveStat(t, q, "default").KeepTotal
	if after != before+1 {
		t.Errorf("KeepTotal = %d, want %d", after, before+1)
	}
}

func TestAdmissionCounterRejectIncrements(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{Class: LaneNormal, RejectAboveRatio: 0.50, MaxQueueDepth: 100},
	})
	for i := 0; i < 20; i++ {
		_ = q.Submit(context.Background(), Job{Key: "k", Lane: "default", Run: func(context.Context) error { return nil }})
	}
	before := laneAdaptiveStat(t, q, "default").RejectTotal
	_ = CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "default"})
	after := laneAdaptiveStat(t, q, "default").RejectTotal
	if after <= before {
		t.Errorf("RejectTotal = %d, want > %d", after, before)
	}
}

func TestAdmissionCounterShedIncrements(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 2, "best_effort": 1},
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
	before := laneAdaptiveStat(t, q, "best_effort").ShedTotal
	_ = CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "best_effort"})
	after := laneAdaptiveStat(t, q, "best_effort").ShedTotal
	if after != before+1 {
		t.Errorf("ShedTotal = %d, want %d", after, before+1)
	}
}

func TestAdmissionCounterDegradeIncrements(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"deg": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{Class: LaneNormal, RejectAboveRatio: 0.90, MaxQueueDepth: 100},
		Lanes: []LaneOverloadPolicy{
			{Lane: "deg", Class: LaneNormal, RejectAboveRatio: 0.95, ShedAboveRatio: 1.0,
				DegradeAboveRatio: 0.01, MaxQueueDepth: 100},
		},
	})
	_ = q.Submit(context.Background(), Job{Key: "fill", Lane: "deg", Run: func(context.Context) error { return nil }})
	before := laneAdaptiveStat(t, q, "deg").DegradeTotal
	_ = CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "deg"})
	after := laneAdaptiveStat(t, q, "deg").DegradeTotal
	if after != before+1 {
		t.Errorf("DegradeTotal = %d, want %d", after, before+1)
	}
}

func TestAdmissionCountersDefaultLanePolicy(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 2, "payment": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{Class: LaneNormal, RejectAboveRatio: 0.50, MaxQueueDepth: 100},
	})
	for i := 0; i < 20; i++ {
		_ = q.Submit(context.Background(), Job{Key: "k", Lane: "payment", Run: func(context.Context) error { return nil }})
	}
	before := laneAdaptiveStat(t, q, "payment").RejectTotal
	_ = CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "payment"})
	after := laneAdaptiveStat(t, q, "payment").RejectTotal
	if after <= before {
		t.Errorf("payment RejectTotal = %d, want > %d", after, before)
	}
}

func TestAdaptiveQuotaIncreaseEmitsDecisionEvent(t *testing.T) {
	var last AdaptiveQuotaEvent
	var got atomic.Bool
	obs := DefaultObservabilityConfig()
	obs.Hooks.OnAdaptiveQuotaDecision = func(e AdaptiveQuotaEvent) {
		if e.Action == QuotaAdjustmentIncrease {
			last = e
			got.Store(true)
		}
	}
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 32,
		LaneQuotas:    map[Lane]int{"critical": 1},
		Observability: obs,
		AdaptiveQuota: AdaptiveQuotaPolicy{
			Config: AdaptiveQuotaConfig{
				Enabled: true, EvaluationInterval: time.Second,
				WarmupDuration: time.Nanosecond, CooldownDuration: time.Millisecond,
				PressureLow: 0.99, PressureHigh: 0.999,
				QueueWaitHigh: 5 * time.Millisecond, MaxAdjustmentsPerTick: 1, IncreaseStep: 1,
			},
			Lanes: []LaneAdaptivePolicy{
				{Lane: "critical", Class: LaneCritical, Enabled: true, MinQuota: 1, MaxQuota: 8,
					AllowIncrease: true, TargetQueueWait: time.Millisecond},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	for i := 0; i < 8; i++ {
		_ = q.Submit(ctx, Job{Key: "k", Lane: "critical", Run: func(context.Context) error {
			time.Sleep(80 * time.Millisecond)
			return nil
		}})
	}
	time.Sleep(200 * time.Millisecond)
	if q.adaptive != nil {
		q.adaptive.RunTick()
	}
	if !got.Load() {
		t.Fatal("expected increase decision event")
	}
	if last.Reason != QuotaReasonCriticalQueueWaitHigh {
		t.Errorf("reason = %q, want %q", last.Reason, QuotaReasonCriticalQueueWaitHigh)
	}
}

func TestAdaptiveQuotaEventPayloadFields(t *testing.T) {
	var last AdaptiveQuotaEvent
	obs := DefaultObservabilityConfig()
	obs.Hooks.OnAdaptiveQuotaDecision = func(e AdaptiveQuotaEvent) {
		if e.Action == QuotaAdjustmentIncrease {
			last = e
		}
	}
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 32,
		LaneQuotas:    map[Lane]int{"critical": 1},
		Observability: obs,
		AdaptiveQuota: AdaptiveQuotaPolicy{
			Config: AdaptiveQuotaConfig{
				Enabled: true, EvaluationInterval: time.Second,
				WarmupDuration: time.Nanosecond, CooldownDuration: time.Millisecond,
				PressureLow: 0.99, PressureHigh: 0.999,
				QueueWaitHigh: 5 * time.Millisecond, MaxAdjustmentsPerTick: 1, IncreaseStep: 1,
			},
			Lanes: []LaneAdaptivePolicy{
				{Lane: "critical", Class: LaneCritical, Enabled: true, MinQuota: 1, MaxQuota: 8,
					AllowIncrease: true, TargetQueueWait: time.Millisecond},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	for i := 0; i < 8; i++ {
		_ = q.Submit(ctx, Job{Key: "k", Lane: "critical", Run: func(context.Context) error {
			time.Sleep(80 * time.Millisecond)
			return nil
		}})
	}
	time.Sleep(200 * time.Millisecond)
	if q.adaptive != nil {
		q.adaptive.RunTick()
	}
	if last.Lane != "critical" || last.Class != LaneCritical {
		t.Errorf("lane/class = %q/%q", last.Lane, last.Class)
	}
	if last.OldQuota == 0 || last.NewQuota <= last.OldQuota {
		t.Errorf("quota change = %d -> %d", last.OldQuota, last.NewQuota)
	}
	if last.GlobalPressure <= 0 {
		t.Error("GlobalPressure should be set")
	}
	if last.QueueDepth == 0 {
		t.Error("QueueDepth should be set")
	}
	if last.PolicyVersion == 0 || last.QuotaVersion == 0 {
		t.Errorf("versions: policy=%d quota=%d", last.PolicyVersion, last.QuotaVersion)
	}
}

func TestAdmissionCountersInLaneStats(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 2,
		LaneQuotas: map[Lane]int{"default": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_ = q.Start(ctx)
	_ = q.Submit(ctx, Job{Key: "k", Lane: "default", Run: func(context.Context) error { return nil }})
	_ = q.Submit(ctx, Job{Key: "k2", Lane: "default", Run: func(context.Context) error { return nil }})
	err = q.Submit(ctx, Job{Key: "k3", Lane: "default", Run: func(context.Context) error { return nil }})
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("err = %v, want queue full", err)
	}
	snap := q.AdaptiveDebugSnapshot()
	var def LaneAdaptiveStats
	for _, l := range snap.Lanes {
		if l.Lane == "default" {
			def = l
		}
	}
	if def.QueueFullTotal == 0 {
		t.Error("expected queue full counter in lane stats")
	}
}
