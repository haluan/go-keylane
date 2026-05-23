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

func admissionTestQueue(t *testing.T) *Queue {
	t.Helper()
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[Lane]int{"default": 1},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return q
}

func fillQueueDepth(t *testing.T, q *Queue, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		err := q.Submit(context.Background(), Job{
			Key:  "key",
			Lane: "default",
			Run:  func(context.Context) error { return nil },
		})
		if err != nil {
			t.Fatalf("Submit %d: %v", i, err)
		}
	}
}

func laneCounters(t *testing.T, q *Queue) (accepted, rejected, admissionRejected uint64) {
	t.Helper()
	snap := q.StatsGCPressure()
	if len(snap.Lanes) == 0 {
		return 0, 0, 0
	}
	c := snap.Lanes[0].Counters
	return c.Accepted, c.Rejected, c.AdmissionRejected
}

func TestCheckAdmissionDisabledHighPressure(t *testing.T) {
	q := admissionTestQueue(t)
	fillQueueDepth(t, q, 9)

	err := CheckAdmission(q, AdmissionConfig{Enabled: false}, RequestMeta{Key: "k", Lane: "default"})
	if err != nil {
		t.Fatalf("CheckAdmission = %v, want nil when disabled", err)
	}
}

func TestCheckAdmissionBelowThreshold(t *testing.T) {
	q := admissionTestQueue(t)
	fillQueueDepth(t, q, 8)

	cfg := AdmissionConfig{Enabled: true, RejectAboveRatio: 0.90}
	err := CheckAdmission(q, cfg, RequestMeta{Key: "k", Lane: "default"})
	if err != nil {
		t.Fatalf("CheckAdmission = %v, want admit below threshold", err)
	}
}

func TestCheckAdmissionAtThreshold(t *testing.T) {
	q := admissionTestQueue(t)
	fillQueueDepth(t, q, 9)

	cfg := AdmissionConfig{Enabled: true, RejectAboveRatio: 0.90}
	err := CheckAdmission(q, cfg, RequestMeta{Key: "k", Lane: "default"})
	if !errors.Is(err, ErrAdmissionRejected) {
		t.Fatalf("CheckAdmission = %v, want ErrAdmissionRejected", err)
	}
}

func TestCheckAdmissionAboveThreshold(t *testing.T) {
	q := admissionTestQueue(t)
	fillQueueDepth(t, q, 10)

	cfg := AdmissionConfig{Enabled: true, RejectAboveRatio: 0.85}
	err := CheckAdmission(q, cfg, RequestMeta{Key: "k", Lane: "default"})
	if !errors.Is(err, ErrAdmissionRejected) {
		t.Fatalf("CheckAdmission = %v, want ErrAdmissionRejected", err)
	}
}

func submitRequestAdmissionAdmit(t *testing.T, q *Queue, admission AdmissionConfig) {
	t.Helper()
	ctx := testTimeout(t)
	if err := q.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var ran atomic.Bool
	acceptedBefore, _, _ := laneCounters(t, q)

	future, err := SubmitRequest(ctx, q, Request[struct{}, struct{}]{
		Meta:      RequestMeta{Key: "k", Lane: "default"},
		Admission: admission,
		Input:     struct{}{},
		Handle: func(context.Context, struct{}) (struct{}, error) {
			ran.Store(true)
			return struct{}{}, nil
		},
	})
	if err != nil {
		t.Fatalf("SubmitRequest: %v", err)
	}
	if _, err := future.Await(ctx); err != nil {
		t.Fatalf("Await: %v", err)
	}
	if !ran.Load() {
		t.Fatal("handler did not run")
	}

	acceptedAfter, _, _ := laneCounters(t, q)
	if acceptedAfter != acceptedBefore+1 {
		t.Errorf("Accepted = %d, want %d (request must be enqueued)", acceptedAfter, acceptedBefore+1)
	}
}

func TestCheckAdmissionNegativeRatioReturnsInvalidConfig(t *testing.T) {
	q := admissionTestQueue(t)
	fillQueueDepth(t, q, 9)

	err := CheckAdmission(q, AdmissionConfig{Enabled: true, RejectAboveRatio: -0.1},
		RequestMeta{Key: "k", Lane: "default"})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("CheckAdmission = %v, want ErrInvalidConfig", err)
	}
	if errors.Is(err, ErrAdmissionRejected) {
		t.Fatal("negative ratio must not be treated as pressure rejection")
	}
}

func TestSubmitRequestInvalidAdmissionConfig(t *testing.T) {
	q := admissionTestQueue(t)
	fillQueueDepth(t, q, 9)

	var ran atomic.Bool
	_, err := SubmitRequest(context.Background(), q, Request[struct{}, struct{}]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Admission: AdmissionConfig{
			Enabled:          true,
			RejectAboveRatio: -0.1,
		},
		Input: struct{}{},
		Handle: func(context.Context, struct{}) (struct{}, error) {
			ran.Store(true)
			return struct{}{}, nil
		},
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("SubmitRequest err = %v, want ErrInvalidConfig", err)
	}
	if ran.Load() {
		t.Error("handler ran with invalid admission config")
	}
}

func TestSubmitRequestAdmissionDisabledAdmitsUnderHighPressure(t *testing.T) {
	q := admissionTestQueue(t)
	fillQueueDepth(t, q, 9)

	submitRequestAdmissionAdmit(t, q, AdmissionConfig{Enabled: false})
}

func TestSubmitRequestAdmissionBelowThresholdAdmits(t *testing.T) {
	q := admissionTestQueue(t)
	fillQueueDepth(t, q, 8)

	submitRequestAdmissionAdmit(t, q, AdmissionConfig{
		Enabled:          true,
		RejectAboveRatio: 0.90,
	})
}

func TestSubmitRequestAdmissionRejectsBeforeEnqueue(t *testing.T) {
	q := admissionTestQueue(t)
	fillQueueDepth(t, q, 9)

	var ran atomic.Bool
	_, err := SubmitRequest(context.Background(), q, Request[struct{}, struct{}]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Admission: AdmissionConfig{
			Enabled:          true,
			RejectAboveRatio: 0.90,
		},
		Input: struct{}{},
		Handle: func(context.Context, struct{}) (struct{}, error) {
			ran.Store(true)
			return struct{}{}, nil
		},
	})
	if !errors.Is(err, ErrAdmissionRejected) {
		t.Fatalf("SubmitRequest err = %v, want ErrAdmissionRejected", err)
	}
	if ran.Load() {
		t.Error("handler ran after admission rejection")
	}
}

func TestAdmissionRejectionDoesNotIncreaseQueueDepth(t *testing.T) {
	q := admissionTestQueue(t)
	fillQueueDepth(t, q, 9)

	before := q.StatsGCPressure().TotalQueued
	_ = CheckAdmission(q, AdmissionConfig{Enabled: true, RejectAboveRatio: 0.90},
		RequestMeta{Key: "k", Lane: "default"})
	after := q.StatsGCPressure().TotalQueued
	if after != before {
		t.Errorf("TotalQueued after reject = %d, want %d", after, before)
	}
}

func TestAdmissionRejectionIncrementsRejectedCounter(t *testing.T) {
	q := admissionTestQueue(t)
	fillQueueDepth(t, q, 9)

	acceptedBefore, rejectedBefore, admissionBefore := laneCounters(t, q)
	_ = CheckAdmission(q, AdmissionConfig{Enabled: true, RejectAboveRatio: 0.90},
		RequestMeta{Key: "k", Lane: "default"})
	acceptedAfter, rejectedAfter, admissionAfter := laneCounters(t, q)

	if rejectedAfter != rejectedBefore+1 {
		t.Errorf("Rejected = %d, want %d", rejectedAfter, rejectedBefore+1)
	}
	if admissionAfter != admissionBefore+1 {
		t.Errorf("AdmissionRejected = %d, want %d", admissionAfter, admissionBefore+1)
	}
	if acceptedAfter != acceptedBefore {
		t.Errorf("Accepted = %d, want %d (enqueue counter must not increment on pressure reject)",
			acceptedAfter, acceptedBefore)
	}
}

func TestNormalizeAdmissionConfigDefaultRatio(t *testing.T) {
	cfg := AdmissionConfig{Enabled: true}
	NormalizeAdmissionConfig(&cfg)
	if cfg.RejectAboveRatio != 0.90 {
		t.Errorf("RejectAboveRatio = %v, want 0.90", cfg.RejectAboveRatio)
	}
}

func TestValidateAdmissionConfigNegativeRatio(t *testing.T) {
	err := ValidateAdmissionConfig(AdmissionConfig{Enabled: true, RejectAboveRatio: -0.1})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("ValidateAdmissionConfig = %v, want ErrInvalidConfig", err)
	}
}

func TestValidateAdmissionConfigZeroRatioDefaults(t *testing.T) {
	err := ValidateAdmissionConfig(AdmissionConfig{Enabled: true, RejectAboveRatio: 0})
	if err != nil {
		t.Fatalf("ValidateAdmissionConfig = %v, want nil (zero defaults to 0.90)", err)
	}
}

func TestSubmitRequestAdmissionEnabledZeroRatioUsesDefault(t *testing.T) {
	q := admissionTestQueue(t)
	fillQueueDepth(t, q, 8)

	submitRequestAdmissionAdmit(t, q, AdmissionConfig{
		Enabled:          true,
		RejectAboveRatio: 0,
	})
}

func TestCheckAdmissionRecordsHotKeyRejectForTrackedKey(t *testing.T) {
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[Lane]int{"default": 1},
		HotKey: HotKeyConfig{
			Enabled:                true,
			MaxTrackedKeysPerShard: 16,
			DetectionWindow:        time.Minute,
			HotKeyDepthRatio:       0.3,
		},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Lane depth rejection (not global pressure) so the test stays deterministic
	// without starting workers — running workers would drain the queue before CheckAdmission.
	_, err = q.UpdateAdmissionPolicy(AdmissionPolicy{
		DefaultClass:            LaneNormal,
		DefaultRejectAboveRatio: 0.99,
		DefaultMaxQueueDepth:    2,
	})
	if err != nil {
		t.Fatal(err)
	}

	const key = "tracked-tenant"
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
	for i := 0; i < 2; i++ {
		if err := q.Submit(context.Background(), Job{Key: key, Lane: "default", Run: run}); err != nil {
			t.Fatalf("Submit %d: %v", i, err)
		}
	}

	err = CheckAdmission(q, AdmissionConfig{Enabled: true},
		RequestMeta{Key: key, Lane: "default"})
	if !errors.Is(err, ErrAdmissionRejected) {
		t.Fatalf("CheckAdmission = %v, want ErrAdmissionRejected", err)
	}
	var rej AdmissionRejectedError
	if !errors.As(err, &rej) {
		t.Fatal("want AdmissionRejectedError")
	}
	if rej.Reason != AdmissionReasonLaneQueueDepthExceeded {
		t.Fatalf("reason = %q, want %s", rej.Reason, AdmissionReasonLaneQueueDepthExceeded)
	}

	snap := q.DebugSnapshot()
	var rejected uint64
	for _, sh := range snap.Shards {
		if sh.HotKeyCandidate != nil && sh.HotKeyCandidate.RejectedApprox > 0 {
			rejected = sh.HotKeyCandidate.RejectedApprox
		}
	}
	if rejected == 0 {
		t.Fatal("expected RejectedApprox > 0 on hot key candidate after admission reject")
	}
}

func TestAdmissionRejectedErrorUnwrap(t *testing.T) {
	err := AdmissionRejectedError{Pressure: 0.95, Threshold: 0.90}
	if !errors.Is(err, ErrAdmissionRejected) {
		t.Error("AdmissionRejectedError should unwrap to ErrAdmissionRejected")
	}
}
