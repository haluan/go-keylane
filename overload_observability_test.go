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

func TestOverloadPolicyEventOnShed(t *testing.T) {
	var events atomic.Int32
	var last OverloadPolicyEvent
	obs := DefaultObservabilityConfig()
	obs.Hooks.OnOverloadPolicyDecision = func(e OverloadPolicyEvent) {
		events.Add(1)
		last = e
	}
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas:    map[Lane]int{"default": 2, "critical": 2, "best_effort": 1},
		Observability: obs,
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
	err = CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "best_effort"})
	if !errors.Is(err, ErrOverloadShed) {
		t.Fatalf("err = %v, want ErrOverloadShed", err)
	}
	if events.Load() != 1 {
		t.Fatalf("events = %d, want 1", events.Load())
	}
	if last.Action != OverloadShed {
		t.Errorf("action = %q, want shed", last.Action)
	}
	if last.Lane != "best_effort" {
		t.Errorf("lane = %q", last.Lane)
	}
}

func TestOverloadPolicyEventNotEmittedOnKeep(t *testing.T) {
	var events atomic.Int32
	obs := DefaultObservabilityConfig()
	obs.Hooks.OnOverloadPolicyDecision = func(OverloadPolicyEvent) { events.Add(1) }
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas:    map[Lane]int{"default": 2},
		Observability: obs,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "default"}); err != nil {
		t.Fatalf("unexpected overload: %v", err)
	}
	if events.Load() != 0 {
		t.Errorf("events = %d, want 0 for keep", events.Load())
	}
}

func TestOverloadPolicyEventOnReject(t *testing.T) {
	var events atomic.Int32
	obs := DefaultObservabilityConfig()
	obs.Hooks.OnOverloadPolicyDecision = func(OverloadPolicyEvent) { events.Add(1) }
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas:    map[Lane]int{"default": 2},
		Observability: obs,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{Class: LaneNormal, RejectAboveRatio: 0.50, MaxQueueDepth: 100},
	})
	for i := 0; i < 20; i++ {
		_ = q.Submit(context.Background(), Job{
			Key: "k", Lane: "default", Run: func(context.Context) error { return nil },
		})
	}
	err = CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "default"})
	if !errors.Is(err, ErrOverloadRejected) {
		t.Fatalf("err = %v, want ErrOverloadRejected", err)
	}
	if events.Load() != 1 {
		t.Fatalf("events = %d, want 1", events.Load())
	}
}

func TestOverloadPolicyEventOnDegrade(t *testing.T) {
	var events atomic.Int32
	obs := DefaultObservabilityConfig()
	obs.Hooks.OnOverloadPolicyDecision = func(OverloadPolicyEvent) { events.Add(1) }
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas:    map[Lane]int{"deg": 1},
		Observability: obs,
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
	_ = q.Submit(context.Background(), Job{
		Key: "fill", Lane: "deg", Run: func(context.Context) error { return nil },
	})
	err = CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "deg"})
	if !errors.Is(err, ErrOverloadDegraded) {
		t.Fatalf("err = %v, want ErrOverloadDegraded", err)
	}
	if events.Load() != 1 {
		t.Fatalf("events = %d, want 1", events.Load())
	}
}

func TestOverloadPolicyEventPayloadFields(t *testing.T) {
	cases := []struct {
		name       string
		setup      func(*Queue)
		lane       Lane
		wantAction OverloadAction
		wantErr    error
	}{
		{
			name:       "keep",
			setup:      func(*Queue) {},
			lane:       "default",
			wantAction: OverloadKeep,
		},
		{
			name: "reject",
			setup: func(q *Queue) {
				_, _ = q.UpdateOverloadPolicy(OverloadPolicy{
					Default: LaneOverloadPolicy{Class: LaneNormal, RejectAboveRatio: 0.50, MaxQueueDepth: 100},
				})
				for i := 0; i < 20; i++ {
					_ = q.Submit(context.Background(), Job{Key: "k", Lane: "default", Run: func(context.Context) error { return nil }})
				}
			},
			lane:       "default",
			wantAction: OverloadReject,
			wantErr:    ErrOverloadRejected,
		},
		{
			name: "shed",
			setup: func(q *Queue) {
				for _, lane := range []Lane{"default", "critical", "best_effort"} {
					for i := 0; i < 8; i++ {
						_ = q.Submit(context.Background(), Job{Key: "k", Lane: lane, Run: func(context.Context) error { return nil }})
					}
				}
			},
			lane:       "best_effort",
			wantAction: OverloadShed,
			wantErr:    ErrOverloadShed,
		},
		{
			name: "degrade",
			setup: func(q *Queue) {
				_ = q.Submit(context.Background(), Job{Key: "fill", Lane: "deg", Run: func(context.Context) error { return nil }})
			},
			lane:       "deg",
			wantAction: OverloadDegrade,
			wantErr:    ErrOverloadDegraded,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var last OverloadPolicyEvent
			obs := DefaultObservabilityConfig()
			obs.Hooks.OnOverloadPolicyDecision = func(e OverloadPolicyEvent) { last = e }
			q, err := New(Config{
				ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
				LaneQuotas:    map[Lane]int{"default": 2, "critical": 2, "best_effort": 1, "deg": 1},
				Observability: obs,
			})
			if err != nil {
				t.Fatal(err)
			}
			_, _ = q.UpdateOverloadPolicy(OverloadPolicy{
				Default: LaneOverloadPolicy{Class: LaneNormal, RejectAboveRatio: 0.90, MaxQueueDepth: 100},
				Lanes: []LaneOverloadPolicy{
					{Lane: "best_effort", Class: LaneBestEffort, RejectAboveRatio: 0.75, ShedAboveRatio: 0.50,
						DegradeAboveRatio: 0.40, MaxQueueDepth: 50, RetryAfter: 100 * time.Millisecond},
					{Lane: "deg", Class: LaneNormal, RejectAboveRatio: 0.95, ShedAboveRatio: 1.0,
						DegradeAboveRatio: 0.01, MaxQueueDepth: 100, RetryAfter: 50 * time.Millisecond},
				},
			})
			tc.setup(q)
			policyVer := q.DebugSnapshot().OverloadPolicyVersion
			checkErr := CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: tc.lane})
			if tc.wantErr != nil {
				if !errors.Is(checkErr, tc.wantErr) {
					t.Fatalf("CheckOverload = %v, want %v", checkErr, tc.wantErr)
				}
			} else if checkErr != nil {
				t.Fatalf("CheckOverload = %v, want nil", checkErr)
			}
			if tc.wantAction == OverloadKeep {
				return
			}
			if last.Action != tc.wantAction {
				t.Errorf("action = %q, want %q", last.Action, tc.wantAction)
			}
			if last.Lane != tc.lane {
				t.Errorf("lane = %q, want %q", last.Lane, tc.lane)
			}
			if last.Class == "" {
				t.Error("Class empty")
			}
			if last.Reason == "" {
				t.Error("Reason empty")
			}
			if last.PolicyVersion != policyVer {
				t.Errorf("PolicyVersion = %d, want %d", last.PolicyVersion, policyVer)
			}
			if last.GlobalPressure <= 0 {
				t.Error("GlobalPressure should be set")
			}
			if last.QueueDepth == 0 && tc.wantAction != OverloadKeep {
				t.Error("QueueDepth should be non-zero when overloaded")
			}
			if last.MaxQueueDepth == 0 {
				t.Error("MaxQueueDepth should be set")
			}
			if tc.wantAction == OverloadDegrade {
				if last.RetryAfter <= 0 {
					t.Error("RetryAfter should be set")
				}
			}
		})
	}
}

func TestOverloadPolicyNilHookSafe(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "default"})
}
