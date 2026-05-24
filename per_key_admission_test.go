// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"testing"
	"time"
)

func perKeyTestConfig() Config {
	return Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 64,
		LaneQuotas:       map[Lane]int{"default": 2},
		HotKey: HotKeyConfig{
			Enabled:                true,
			MaxTrackedKeysPerShard: 16,
			DetectionWindow:        time.Minute,
			HotKeyDepthRatio:       0.3,
			HotKeyWaitRatio:        0.9,
		},
		PerKeyAdmission: PerKeyAdmissionConfig{
			Enabled:                true,
			MinStatus:              HotKeyStatusCandidate,
			DefaultAction:          PerKeyMitigationReject,
			PressureRatioThreshold: 0.35,
			Cooldown:               time.Minute,
		},
	}
}

func TestPerKeyAdmissionDisabledPreservesSubmit(t *testing.T) {
	t.Parallel()
	cfg := perKeyTestConfig()
	cfg.PerKeyAdmission.Enabled = false
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)
	defer func() { _ = q.Stop(context.Background()) }()

	if err := q.Submit(ctx, Job{Key: "k", Lane: "default", Run: func(context.Context) error { return nil }}); err != nil {
		t.Fatal(err)
	}
}

func TestPerKeyAdmissionErrorsIs(t *testing.T) {
	t.Parallel()
	err := PerKeyAdmissionError{Decision: PerKeyAdmissionDecision{Action: PerKeyMitigationReject}}
	if !errors.Is(err, ErrPerKeyAdmissionRejected) {
		t.Fatal("want ErrPerKeyAdmissionRejected")
	}
	err2 := PerKeyAdmissionError{Decision: PerKeyAdmissionDecision{Action: PerKeyMitigationThrottle}}
	if !errors.Is(err2, ErrPerKeyAdmissionThrottled) {
		t.Fatal("want ErrPerKeyAdmissionThrottled")
	}
	err3 := PerKeyAdmissionError{Decision: PerKeyAdmissionDecision{Action: PerKeyMitigationShed}}
	if !errors.Is(err3, ErrPerKeyAdmissionShed) {
		t.Fatal("want ErrPerKeyAdmissionShed")
	}
}

func TestPerKeyAdmissionHotKeyRejectedNormalKeyAdmits(t *testing.T) {
	cfg := perKeyTestConfig()
	cfg.PerKeyAdmission.Enabled = false
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	defer close(block)
	run := func(ctx context.Context) error {
		select {
		case <-block:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	const hot = "hot-tenant"
	const normal = "normal-tenant"
	for i := 0; i < 30; i++ {
		if err := q.Submit(context.Background(), Job{Key: hot, Lane: "default", Run: run}); err != nil {
			t.Fatalf("hot submit %d: %v", i, err)
		}
	}
	activePK := perKeyTestConfig().PerKeyAdmission
	err = CheckPerKeyAdmission(q, activePK, RequestMeta{Key: hot, Lane: "default"})
	if !errors.Is(err, ErrPerKeyAdmissionRejected) {
		t.Fatalf("hot CheckAdmission = %v, want rejected", err)
	}
	if err := q.Submit(context.Background(), Job{Key: normal, Lane: "default", Run: run}); err != nil {
		t.Fatalf("normal submit: %v", err)
	}
}

func TestValidatePerKeyAdmissionRequiresHotKey(t *testing.T) {
	t.Parallel()
	err := ValidatePerKeyAdmissionConfig(PerKeyAdmissionConfig{Enabled: true}, HotKeyConfig{})
	if !errors.Is(err, ErrInvalidPerKeyAdmissionConfig) {
		t.Fatalf("err = %v", err)
	}
}

func TestPerKeyRejectIncrementsRejectedApprox(t *testing.T) {
	cfg := perKeyTestConfig()
	cfg.PerKeyAdmission.Enabled = false
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	defer close(block)
	run := func(ctx context.Context) error {
		select {
		case <-block:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	const hot = "reject-track"
	for i := 0; i < 25; i++ {
		_ = q.Submit(context.Background(), Job{Key: hot, Lane: "default", Run: run})
	}
	activePK := perKeyTestConfig().PerKeyAdmission
	_ = CheckPerKeyAdmission(q, activePK, RequestMeta{Key: hot, Lane: "default"})
	snap := q.DebugSnapshot()
	var rejected uint64
	for _, sh := range snap.Shards {
		if sh.HotKeyCandidate != nil {
			rejected = sh.HotKeyCandidate.RejectedApprox
		}
	}
	if rejected == 0 {
		t.Fatal("expected RejectedApprox > 0 after per-key reject")
	}
}

func TestPerKeyThrottleIncrementsRejectedApprox(t *testing.T) {
	cfg := perKeyTestConfig()
	cfg.PerKeyAdmission.DefaultAction = PerKeyMitigationThrottle
	cfg.PerKeyAdmission.Enabled = false
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 25; i++ {
		_ = q.Submit(context.Background(), Job{
			Key: "throttle-track", Lane: "default",
			Run: func(context.Context) error { return nil },
		})
	}
	activePK := perKeyTestConfig().PerKeyAdmission
	activePK.DefaultAction = PerKeyMitigationThrottle
	err = CheckPerKeyAdmission(q, activePK, RequestMeta{Key: "throttle-track", Lane: "default"})
	if !errors.Is(err, ErrPerKeyAdmissionThrottled) {
		t.Fatalf("err = %v, want throttled", err)
	}
	snap := q.DebugSnapshot()
	var rejected uint64
	for _, sh := range snap.Shards {
		if sh.HotKeyCandidate != nil {
			rejected = sh.HotKeyCandidate.RejectedApprox
		}
	}
	if rejected == 0 {
		t.Fatal("expected RejectedApprox > 0 after per-key throttle")
	}
}

func TestPerKeyHotKeyDifferentLaneAllows(t *testing.T) {
	cfg := perKeyTestConfig()
	cfg.LaneQuotas = map[Lane]int{"hotlane": 2, "coollane": 2}
	cfg.PerKeyAdmission.Enabled = false
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	defer close(block)
	run := func(ctx context.Context) error {
		select {
		case <-block:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for i := 0; i < 30; i++ {
		_ = q.Submit(context.Background(), Job{Key: "hot", Lane: "hotlane", Run: run})
	}
	activePK := perKeyTestConfig().PerKeyAdmission
	if err := CheckPerKeyAdmission(q, activePK, RequestMeta{Key: "hot", Lane: "hotlane"}); !errors.Is(err, ErrPerKeyAdmissionRejected) {
		t.Fatalf("hot lane reject = %v", err)
	}
	if err := q.Submit(context.Background(), Job{Key: "cool", Lane: "coollane", Run: run}); err != nil {
		t.Fatalf("cool lane submit: %v", err)
	}
}

func TestPerKeyDistributedBacklogNoMitigation(t *testing.T) {
	cfg := perKeyTestConfig()
	cfg.PerKeyAdmission.Enabled = false
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		key := "key-" + string(rune('a'+i%10))
		_ = q.Submit(context.Background(), Job{
			Key: key, Lane: "default",
			Run: func(context.Context) error { return nil },
		})
	}
	activePK := perKeyTestConfig().PerKeyAdmission
	for i := 0; i < 5; i++ {
		key := "key-" + string(rune('a'+i%10))
		if err := CheckPerKeyAdmission(q, activePK, RequestMeta{Key: key, Lane: "default"}); err != nil {
			t.Fatalf("distributed key %q rejected: %v", key, err)
		}
	}
}

func TestPerKeyWithLaneAdmissionPrecedence(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas:      map[Lane]int{"default": 2, "critical": 2, "best_effort": 2},
		HotKey:          HotKeyConfig{Enabled: true, MaxTrackedKeysPerShard: 16, DetectionWindow: time.Minute, HotKeyDepthRatio: 0.3},
		PerKeyAdmission: PerKeyAdmissionConfig{Enabled: false},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = q.UpdateAdmissionPolicy(AdmissionPolicy{
		DefaultClass: LaneNormal, DefaultRejectAboveRatio: 0.90, DefaultMaxQueueDepth: 100,
		Lanes: []LanePolicy{
			{Lane: "critical", Class: LaneCritical, RejectAboveRatio: 0.98, MaxQueueDepth: 100},
			{Lane: "best_effort", Class: LaneBestEffort, RejectAboveRatio: 0.50, MaxQueueDepth: 100},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, lane := range []Lane{"default", "critical", "best_effort"} {
		for i := 0; i < 7; i++ {
			if err := q.Submit(context.Background(), Job{
				Key: "fill", Lane: lane,
				Run: func(context.Context) error { return nil },
			}); err != nil {
				t.Fatalf("Submit lane %s: %v", lane, err)
			}
		}
	}
	err = CheckAdmission(q, AdmissionConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "best_effort"})
	if !errors.Is(err, ErrAdmissionRejected) {
		t.Fatalf("lane admission = %v, want rejected", err)
	}
	pk := perKeyTestConfig().PerKeyAdmission
	if err := CheckPerKeyAdmission(q, pk, RequestMeta{Key: "vip", Lane: "critical"}); err != nil {
		t.Fatalf("per-key blocked critical lane: %v", err)
	}
}

func TestOverloadPrecedenceOverPerKeyAllow(t *testing.T) {
	cfg := perKeyTestConfig()
	cfg.QueueSizePerLane = 10
	cfg.OverloadEnabled = false
	cfg.PerKeyAdmission.Enabled = false
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{Class: LaneNormal, RejectAboveRatio: 0.50, MaxQueueDepth: 100},
	})
	if err != nil {
		t.Fatal(err)
	}
	fillQueueDepth(t, q, 9)
	err = CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "hot-key", Lane: "default"})
	if !errors.Is(err, ErrOverloadRejected) {
		t.Fatalf("CheckOverload = %v, want ErrOverloadRejected before per-key", err)
	}
	pk := perKeyTestConfig().PerKeyAdmission
	if err := CheckPerKeyAdmission(q, pk, RequestMeta{Key: "cold-key", Lane: "default"}); err != nil {
		t.Fatalf("per-key should allow cold key under overload pressure: %v", err)
	}
}

func TestPerKeyAdmissionHookFires(t *testing.T) {
	cfg := perKeyTestConfig()
	cfg.Observability.EnableHooks = true
	var fired bool
	cfg.Observability.Hooks.OnPerKeyAdmissionDecision = func(PerKeyAdmissionDecisionEvent) {
		fired = true
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	defer close(block)
	run := func(ctx context.Context) error {
		select {
		case <-block:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for i := 0; i < 25; i++ {
		_ = q.Submit(context.Background(), Job{Key: "hook-key", Lane: "default", Run: run})
	}
	_ = CheckPerKeyAdmission(q, cfg.PerKeyAdmission, RequestMeta{Key: "hook-key", Lane: "default"})
	if !fired {
		t.Fatal("expected OnPerKeyAdmissionDecision hook")
	}
}
