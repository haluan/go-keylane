// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestQuotaChangeEventManualUpdate(t *testing.T) {
	var events atomic.Int32
	var last QuotaChangeEvent
	obs := DefaultObservabilityConfig()
	obs.Hooks.OnQuotaChange = func(e QuotaChangeEvent) {
		events.Add(1)
		last = e
	}
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas:    map[Lane]int{"default": 2},
		Observability: obs,
	})
	if err != nil {
		t.Fatal(err)
	}
	ver, err := q.UpdateLaneQuota("default", 4)
	if err != nil {
		t.Fatal(err)
	}
	if events.Load() != 1 {
		t.Fatalf("events = %d, want 1", events.Load())
	}
	if last.Source != QuotaChangeManual {
		t.Errorf("source = %q, want manual", last.Source)
	}
	if last.OldQuota != 2 || last.NewQuota != 4 {
		t.Errorf("quota change = %d -> %d", last.OldQuota, last.NewQuota)
	}
	if last.QuotaVersion != ver {
		t.Errorf("QuotaVersion = %d, want %d", last.QuotaVersion, ver)
	}
}

func TestQuotaChangeEventNotEmittedOnConflict(t *testing.T) {
	var events atomic.Int32
	obs := DefaultObservabilityConfig()
	obs.Hooks.OnQuotaChange = func(QuotaChangeEvent) { events.Add(1) }
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas:    map[Lane]int{"default": 2},
		Observability: obs,
	})
	if err != nil {
		t.Fatal(err)
	}
	ver, _ := q.UpdateLaneQuota("default", 2)
	_, err = q.UpdateLaneQuotaIfVersion("default", 3, ver-1)
	if !errors.Is(err, ErrQuotaPolicyVersionConflict) {
		t.Fatalf("err = %v, want ErrQuotaPolicyVersionConflict", err)
	}
	if events.Load() != 0 {
		t.Errorf("events = %d, want 0 on failed update", events.Load())
	}
}

func TestQuotaVersionUnchangedOnFailedUpdate(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	ver0 := q.CurrentQuotaPolicy().Version
	quota0 := q.CurrentQuotaPolicy().LaneQuotas["default"]
	_, err = q.UpdateLaneQuotaIfVersion("default", 5, ver0-1)
	if !errors.Is(err, ErrQuotaPolicyVersionConflict) {
		t.Fatalf("err = %v, want ErrQuotaPolicyVersionConflict", err)
	}
	if got := q.CurrentQuotaPolicy().Version; got != ver0 {
		t.Errorf("version = %d, want unchanged %d", got, ver0)
	}
	if got := q.CurrentQuotaPolicy().LaneQuotas["default"]; got != quota0 {
		t.Errorf("quota = %d, want unchanged %d", got, quota0)
	}
}

func TestManualUpdateIncrementsQuotaChangeTotal(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	before := laneAdaptiveStat(t, q, "default").QuotaChangeTotal
	if _, err := q.UpdateLaneQuota("default", 4); err != nil {
		t.Fatal(err)
	}
	after := laneAdaptiveStat(t, q, "default").QuotaChangeTotal
	if after != before+1 {
		t.Errorf("QuotaChangeTotal = %d, want %d", after, before+1)
	}
}

func TestQuotaChangeEventManualPolicyVersionUnset(t *testing.T) {
	var last QuotaChangeEvent
	obs := DefaultObservabilityConfig()
	obs.Hooks.OnQuotaChange = func(e QuotaChangeEvent) { last = e }
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas:    map[Lane]int{"default": 2},
		Observability: obs,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{Class: LaneNormal, RejectAboveRatio: 0.90, MaxQueueDepth: 100},
	})
	if _, err := q.UpdateLaneQuota("default", 3); err != nil {
		t.Fatal(err)
	}
	if last.PolicyVersion != 0 {
		t.Errorf("PolicyVersion = %d, want 0 for manual quota changes", last.PolicyVersion)
	}
	if last.QuotaVersion == 0 {
		t.Error("QuotaVersion should be set for manual quota changes")
	}
}

func TestQuotaChangeNilHookSafe(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.UpdateLaneQuota("default", 3); err != nil {
		t.Fatal(err)
	}
}

func TestAdaptiveQuotaChangeEventOnSuccessfulTick(t *testing.T) {
	var quotaEvents atomic.Int32
	obs := DefaultObservabilityConfig()
	obs.Hooks.OnQuotaChange = func(e QuotaChangeEvent) {
		if e.Source == QuotaChangeAdaptive {
			quotaEvents.Add(1)
		}
	}
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas:      map[Lane]int{"default": 2, "best_effort": 3},
		Observability:   obs,
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	time.Sleep(2 * time.Nanosecond)
	if q.adaptive != nil {
		q.adaptive.RunTick()
	}
	if quotaEvents.Load() == 0 {
		t.Fatal("expected adaptive QuotaChangeEvent")
	}
}
