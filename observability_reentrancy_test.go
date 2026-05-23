// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

func TestHooksReentrantWithoutDeadlock(t *testing.T) {
	var q *Queue
	obs := DefaultObservabilityConfig()
	obs.Hooks.OnQuotaChange = func(QuotaChangeEvent) {
		_, _ = q.UpdateLaneQuota("default", 2)
		_ = q.AdaptiveDebugSnapshot()
		_ = q.DebugSnapshot()
	}
	obs.Hooks.OnAdaptiveQuotaDecision = func(AdaptiveQuotaEvent) {
		_ = q.CurrentQuotaPolicy()
		_ = q.AdaptiveDebugSnapshot()
	}
	obs.Hooks.OnOverloadPolicyDecision = func(OverloadPolicyEvent) {
		_ = CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "default"})
		_ = q.DebugSnapshot()
	}
	var err error
	q, err = New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas:      map[Lane]int{"default": 2, "best_effort": 1},
		OverloadEnabled: true,
		Observability:   obs,
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

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := q.UpdateLaneQuota("default", 3); err != nil {
			t.Errorf("UpdateLaneQuota: %v", err)
		}
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
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: hooks blocked reentrant queue calls")
	}
}

func TestRejectedSubmitNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{Class: LaneNormal, RejectAboveRatio: 0.01, MaxQueueDepth: 100},
	})
	for i := 0; i < 20; i++ {
		_ = q.Submit(context.Background(), Job{
			Key: "k", Lane: "default", Run: func(context.Context) error { return nil },
		})
	}
	for i := 0; i < 50; i++ {
		err := CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "default"})
		if !errors.Is(err, ErrOverloadRejected) {
			t.Fatalf("check %d: err = %v, want overload rejected", i, err)
		}
	}
	eventuallyNoGoroutineGrowth(t, before, 6)
}
