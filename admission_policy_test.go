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

func admissionPolicyTestQueue(t *testing.T) *Queue {
	t.Helper()
	q, err := New(Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{
			"default":     2,
			"critical":    2,
			"background":  1,
			"best_effort": 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func TestLaneClassValidate(t *testing.T) {
	if err := LaneClass("bad").Validate(); !errors.Is(err, ErrInvalidLaneClass) {
		t.Errorf("err = %v, want ErrInvalidLaneClass", err)
	}
}

func TestUpdateAdmissionPolicyRejectsInvalidRatio(t *testing.T) {
	q := admissionPolicyTestQueue(t)
	_, err := q.UpdateAdmissionPolicy(AdmissionPolicy{
		DefaultClass:            LaneNormal,
		DefaultRejectAboveRatio: 1.5,
		DefaultMaxQueueDepth:    10,
	})
	if !errors.Is(err, ErrInvalidAdmissionPolicy) {
		t.Errorf("err = %v, want ErrInvalidAdmissionPolicy", err)
	}
}

func TestAdmissionPolicyMissingLaneUsesDefaults(t *testing.T) {
	// admissionPolicyTestQueue registers "default", "critical", "background", "best_effort".
	// UpdateAdmissionPolicy sets an explicit override only for "critical".
	// "default" and "best_effort" receive no override and must reflect the default fields.
	q := admissionPolicyTestQueue(t)
	_, err := q.UpdateAdmissionPolicy(AdmissionPolicy{
		DefaultClass:            LaneNormal,
		DefaultRejectAboveRatio: 0.90,
		DefaultMaxQueueDepth:    50,
		Lanes: []LanePolicy{
			{Lane: "critical", Class: LaneCritical, RejectAboveRatio: 0.98, MaxQueueDepth: 200},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	snap := q.CurrentAdmissionPolicy()

	// Find entries for "default" and "best_effort" — they must have default values.
	for _, lp := range snap.Lanes {
		if lp.Lane == "critical" {
			if lp.Class != LaneCritical {
				t.Errorf("critical Class = %v, want %v", lp.Class, LaneCritical)
			}
			if lp.RejectAboveRatio != 0.98 {
				t.Errorf("critical RejectAboveRatio = %.2f, want 0.98", lp.RejectAboveRatio)
			}
			if lp.MaxQueueDepth != 200 {
				t.Errorf("critical MaxQueueDepth = %d, want 200", lp.MaxQueueDepth)
			}
			continue
		}
		// All other lanes must fall back to the defaults.
		if lp.Class != LaneNormal {
			t.Errorf("lane %q Class = %v, want %v (default)", lp.Lane, lp.Class, LaneNormal)
		}
		if lp.RejectAboveRatio != 0.90 {
			t.Errorf("lane %q RejectAboveRatio = %.2f, want 0.90 (default)", lp.Lane, lp.RejectAboveRatio)
		}
		if lp.MaxQueueDepth != 50 {
			t.Errorf("lane %q MaxQueueDepth = %d, want 50 (default)", lp.Lane, lp.MaxQueueDepth)
		}
	}
}

func TestUpdateAdmissionPolicyRejectsNegativeRatio(t *testing.T) {
	q := admissionPolicyTestQueue(t)
	_, err := q.UpdateAdmissionPolicy(AdmissionPolicy{
		DefaultClass:            LaneNormal,
		DefaultRejectAboveRatio: -0.1,
		DefaultMaxQueueDepth:    10,
	})
	if !errors.Is(err, ErrInvalidAdmissionPolicy) {
		t.Errorf("err = %v, want ErrInvalidAdmissionPolicy", err)
	}
}

func TestUpdateAdmissionPolicyRejectsZeroMaxQueueDepth(t *testing.T) {
	q := admissionPolicyTestQueue(t)
	_, err := q.UpdateAdmissionPolicy(AdmissionPolicy{
		DefaultClass:            LaneNormal,
		DefaultRejectAboveRatio: 0.9,
		DefaultMaxQueueDepth:    0,
	})
	if !errors.Is(err, ErrInvalidAdmissionPolicy) {
		t.Errorf("err = %v, want ErrInvalidAdmissionPolicy", err)
	}
}

func TestUpdateAdmissionPolicyCurrentVersionMatches(t *testing.T) {
	q := admissionPolicyTestQueue(t)
	v, err := q.UpdateAdmissionPolicy(AdmissionPolicy{
		DefaultClass:            LaneNormal,
		DefaultRejectAboveRatio: 0.85,
		DefaultMaxQueueDepth:    20,
		Lanes: []LanePolicy{
			{Lane: "critical", Class: LaneCritical, RejectAboveRatio: 0.98, MaxQueueDepth: 30},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	snap := q.CurrentAdmissionPolicy()
	if snap.Version != v {
		t.Errorf("CurrentAdmissionPolicy().Version = %d, want %d", snap.Version, v)
	}
}

func TestAdmissionPolicyCallerInputMutationDoesNotAffectScheduler(t *testing.T) {
	q := admissionPolicyTestQueue(t)
	lanes := []LanePolicy{
		{Lane: "default", Class: LaneNormal, RejectAboveRatio: 0.9, MaxQueueDepth: 10},
	}
	policy := AdmissionPolicy{
		DefaultClass:            LaneNormal,
		DefaultRejectAboveRatio: 0.9,
		DefaultMaxQueueDepth:    10,
		Lanes:                   lanes,
	}
	if _, err := q.UpdateAdmissionPolicy(policy); err != nil {
		t.Fatal(err)
	}
	lanes[0].MaxQueueDepth = 1
	lanes[0].RejectAboveRatio = 0.1

	snap := q.CurrentAdmissionPolicy()
	if snap.Lanes[0].MaxQueueDepth != 10 {
		t.Errorf("MaxQueueDepth = %d, want 10 after mutating caller input", snap.Lanes[0].MaxQueueDepth)
	}
}

func TestAdmissionPolicySnapshotDefensiveCopy(t *testing.T) {
	q := admissionPolicyTestQueue(t)
	_, _ = q.UpdateAdmissionPolicy(AdmissionPolicy{
		DefaultClass:            LaneNormal,
		DefaultRejectAboveRatio: 0.9,
		DefaultMaxQueueDepth:    10,
		Lanes: []LanePolicy{
			{Lane: "default", Class: LaneNormal, RejectAboveRatio: 0.9, MaxQueueDepth: 10},
		},
	})
	snap := q.CurrentAdmissionPolicy()
	snap.Lanes[0].MaxQueueDepth = 1
	snap2 := q.CurrentAdmissionPolicy()
	if snap2.Lanes[0].MaxQueueDepth != 10 {
		t.Errorf("MaxQueueDepth = %d, want 10 after mutating returned snapshot", snap2.Lanes[0].MaxQueueDepth)
	}
}

func TestCheckAdmissionRejectsLaneDepthWithReason(t *testing.T) {
	q := admissionPolicyTestQueue(t)
	_, err := q.UpdateAdmissionPolicy(AdmissionPolicy{
		DefaultClass:            LaneNormal,
		DefaultRejectAboveRatio: 0.99,
		DefaultMaxQueueDepth:    2,
	})
	if err != nil {
		t.Fatal(err)
	}
	fillQueueDepth(t, q, 2)

	cfg := AdmissionConfig{Enabled: true}
	err = CheckAdmission(q, cfg, RequestMeta{Key: "k", Lane: "default"})
	if !errors.Is(err, ErrAdmissionRejected) {
		t.Fatalf("err = %v, want ErrAdmissionRejected", err)
	}
	var rej AdmissionRejectedError
	if !errors.As(err, &rej) {
		t.Fatal("want AdmissionRejectedError")
	}
	if rej.Reason != AdmissionReasonLaneQueueDepthExceeded {
		t.Errorf("reason = %q, want %s", rej.Reason, AdmissionReasonLaneQueueDepthExceeded)
	}
}

func TestCheckAdmissionBestEffortRejectsBeforeCritical(t *testing.T) {
	q := admissionPolicyTestQueue(t)
	_, err := q.UpdateAdmissionPolicy(AdmissionPolicy{
		DefaultClass:            LaneNormal,
		DefaultRejectAboveRatio: 0.90,
		DefaultMaxQueueDepth:    100,
		Lanes: []LanePolicy{
			{Lane: "critical", Class: LaneCritical, RejectAboveRatio: 0.98, MaxQueueDepth: 100},
			{Lane: "best_effort", Class: LaneBestEffort, RejectAboveRatio: 0.50, MaxQueueDepth: 100},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Fill 7 jobs per lane -> 21/30 total depth ratio ~ 0.7
	for _, lane := range []Lane{"default", "critical", "best_effort"} {
		for i := 0; i < 7; i++ {
			if err := q.Submit(context.Background(), Job{
				Key: "k", Lane: lane,
				Run: func(context.Context) error { return nil },
			}); err != nil {
				t.Fatalf("Submit lane %s: %v", lane, err)
			}
		}
	}

	cfg := AdmissionConfig{Enabled: true}
	err = CheckAdmission(q, cfg, RequestMeta{Key: "k", Lane: "best_effort"})
	if !errors.Is(err, ErrAdmissionRejected) {
		t.Fatalf("best_effort: err = %v, want reject", err)
	}
	err = CheckAdmission(q, cfg, RequestMeta{Key: "k", Lane: "critical"})
	if err != nil {
		t.Fatalf("critical: err = %v, want admit at same pressure", err)
	}
}

func TestCheckAdmissionBackgroundRejectsBeforeCritical(t *testing.T) {
	q := admissionPolicyTestQueue(t)
	_, err := q.UpdateAdmissionPolicy(AdmissionPolicy{
		DefaultClass:            LaneNormal,
		DefaultRejectAboveRatio: 0.90,
		DefaultMaxQueueDepth:    100,
		Lanes: []LanePolicy{
			{Lane: "critical", Class: LaneCritical, RejectAboveRatio: 0.98, MaxQueueDepth: 100},
			{Lane: "background", Class: LaneBackground, RejectAboveRatio: 0.70, MaxQueueDepth: 100},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Four lanes × 7 jobs → 28/40 total depth ratio = 0.7 (background rejects at >= 0.70).
	for _, lane := range []Lane{"default", "critical", "background", "best_effort"} {
		for i := 0; i < 7; i++ {
			if err := q.Submit(context.Background(), Job{
				Key: "k", Lane: lane,
				Run: func(context.Context) error { return nil },
			}); err != nil {
				t.Fatalf("Submit lane %s: %v", lane, err)
			}
		}
	}

	cfg := AdmissionConfig{Enabled: true}
	err = CheckAdmission(q, cfg, RequestMeta{Key: "k", Lane: "background"})
	if !errors.Is(err, ErrAdmissionRejected) {
		t.Fatalf("background: err = %v, want reject", err)
	}
	err = CheckAdmission(q, cfg, RequestMeta{Key: "k", Lane: "critical"})
	if err != nil {
		t.Fatalf("critical: err = %v, want admit at same pressure", err)
	}
}

func TestSubmitRequestAdmissionRejectedNotEnqueued(t *testing.T) {
	q, err := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = q.UpdateAdmissionPolicy(AdmissionPolicy{
		DefaultClass:            LaneBestEffort,
		DefaultRejectAboveRatio: 0.40,
		DefaultMaxQueueDepth:    100,
	})
	fillQueueDepth(t, q, 5) // 5/10 pressure >= 0.40

	ctx := context.Background()
	_, err = SubmitRequest(ctx, q, Request[struct{}, struct{}]{
		Meta:      RequestMeta{Key: "k", Lane: "default"},
		Admission: AdmissionConfig{Enabled: true},
		Input:     struct{}{},
		Handle: func(context.Context, struct{}) (struct{}, error) {
			return struct{}{}, nil
		},
	})
	if !errors.Is(err, ErrAdmissionRejected) {
		t.Fatalf("SubmitRequest err = %v, want ErrAdmissionRejected", err)
	}
	_, _, adm := laneCounters(t, q)
	if adm != 1 {
		t.Errorf("AdmissionRejected = %d, want 1", adm)
	}
}

func TestAdmissionPolicyQueuedJobsContinueAfterPolicyUpdate(t *testing.T) {
	q, err := New(Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 20,
		LaneQuotas:       map[Lane]int{"default": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}

	hold := make(chan struct{})
	blockerStarted := make(chan struct{})
	go func() {
		_ = q.Submit(ctx, Job{
			Key: "blocker", Lane: "default",
			Run: func(context.Context) error {
				close(blockerStarted)
				<-hold
				return nil
			},
		})
	}()
	<-blockerStarted

	const n = 5
	var executed int32
	for i := 0; i < n; i++ {
		if err := q.Submit(ctx, Job{
			Key: "k", Lane: "default",
			Run: func(context.Context) error {
				atomic.AddInt32(&executed, 1)
				return nil
			},
		}); err != nil {
			t.Fatalf("Submit %d: %v", i, err)
		}
	}

	// Tighten policy after jobs are already queued.
	if _, err := q.UpdateAdmissionPolicy(AdmissionPolicy{
		DefaultClass:            LaneBestEffort,
		DefaultRejectAboveRatio: 0.01,
		DefaultMaxQueueDepth:    1,
	}); err != nil {
		t.Fatal(err)
	}

	close(hold)

	deadline := time.After(3 * time.Second)
	for atomic.LoadInt32(&executed) < n {
		select {
		case <-deadline:
			t.Fatalf("executed = %d, want %d after policy update", atomic.LoadInt32(&executed), n)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestAdmissionPolicyDoesNotInterruptRunningJob(t *testing.T) {
	q := admissionPolicyTestQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})

	errCh := make(chan error, 1)
	go func() {
		errCh <- q.Submit(ctx, Job{
			Key:  "k",
			Lane: "default",
			Run: func(context.Context) error {
				close(started)
				<-release
				return nil
			},
		})
	}()

	<-started
	if _, err := q.UpdateAdmissionPolicy(AdmissionPolicy{
		DefaultClass:            LaneBestEffort,
		DefaultRejectAboveRatio: 0.01,
		DefaultMaxQueueDepth:    1,
	}); err != nil {
		t.Fatal(err)
	}
	close(release)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Submit: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for job to complete after policy update")
	}
}

func TestDebugSnapshotIncludesAdmissionPolicyVersion(t *testing.T) {
	q := admissionPolicyTestQueue(t)
	initial := q.DebugSnapshot().AdmissionPolicyVersion

	v, err := q.UpdateAdmissionPolicy(AdmissionPolicy{
		DefaultClass:            LaneNormal,
		DefaultRejectAboveRatio: 0.88,
		DefaultMaxQueueDepth:    50,
	})
	if err != nil {
		t.Fatal(err)
	}
	snap := q.DebugSnapshot()
	if snap.AdmissionPolicyVersion != v {
		t.Errorf("DebugSnapshot.AdmissionPolicyVersion = %d, want %d", snap.AdmissionPolicyVersion, v)
	}
	if v <= initial {
		t.Errorf("updated version = %d, want > initial %d", v, initial)
	}
}

func TestAdmissionPolicyConcurrentUpdateAndCheck(t *testing.T) {
	q := admissionPolicyTestQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

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
		go func(i int) {
			defer wg.Done()
			_, _ = q.UpdateAdmissionPolicy(AdmissionPolicy{
				DefaultClass:            LaneNormal,
				DefaultRejectAboveRatio: 0.85 + float64(i%10)*0.01,
				DefaultMaxQueueDepth:    50,
			})
			_ = q.CurrentAdmissionPolicy()
			_ = CheckAdmission(q, AdmissionConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "default"})
		}(i)
	}
	wg.Wait()
}

// fillQueueDepth and laneCounters reused from admission_test.go in same package
