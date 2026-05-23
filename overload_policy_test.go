// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func overloadPolicyTestQueue(t *testing.T) *Queue {
	t.Helper()
	q, err := New(Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{
			"default":     2,
			"critical":    2,
			"best_effort": 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func TestUpdateOverloadPolicyRejectsNegativeBackoff(t *testing.T) {
	q := overloadPolicyTestQueue(t)
	_, err := q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{
			Class: LaneNormal, RejectAboveRatio: 0.9, MaxQueueDepth: 10,
			RetryAfter: -1 * time.Millisecond,
		},
	})
	if !errors.Is(err, ErrInvalidOverloadPolicy) {
		t.Errorf("err = %v, want ErrInvalidOverloadPolicy", err)
	}
}

func TestCheckOverloadRejectsShedNotEnqueued(t *testing.T) {
	q := overloadPolicyTestQueue(t)
	_, err := q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{Class: LaneNormal, RejectAboveRatio: 0.90, MaxQueueDepth: 100},
		Lanes: []LaneOverloadPolicy{
			{Lane: "best_effort", Class: LaneBestEffort, RejectAboveRatio: 0.75, ShedAboveRatio: 0.50, MaxQueueDepth: 100},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Fill global pressure above best_effort shed threshold (0.50) across all lanes.
	for _, lane := range []Lane{"default", "critical", "best_effort"} {
		for i := 0; i < 8; i++ {
			_ = q.Submit(context.Background(), Job{
				Key: "k", Lane: lane,
				Run: func(context.Context) error { return nil },
			})
		}
	}
	err = CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "best_effort"})
	if !errors.Is(err, ErrOverloadShed) {
		t.Fatalf("err = %v, want ErrOverloadShed", err)
	}
}

func TestSubmitRequestOverloadRejectedNotEnqueued(t *testing.T) {
	q := overloadPolicyTestQueue(t)
	_, _ = q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{Class: LaneBestEffort, RejectAboveRatio: 0.01, ShedAboveRatio: 0.01, MaxQueueDepth: 100},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	future, err := SubmitRequest(ctx, q, Request[struct{}, struct{}]{
		Meta:     RequestMeta{Key: "k", Lane: "default"},
		Overload: OverloadConfig{Enabled: true},
		Handle:   func(context.Context, struct{}) (struct{}, error) { return struct{}{}, nil },
	})
	if err == nil {
		_, _ = future.Await(ctx)
	}
	if !errors.Is(err, ErrOverloadRejected) && !errors.Is(err, ErrOverloadShed) {
		if err == nil {
			if _, awaitErr := future.Await(ctx); awaitErr != nil {
				err = awaitErr
			}
		}
	}
	if err != nil && !errors.Is(err, ErrOverloadRejected) && !errors.Is(err, ErrOverloadShed) {
		t.Fatalf("err = %v", err)
	}
}

func TestAdmissionPolicyMissingLaneUsesDefaultsOverload(t *testing.T) {
	q := overloadPolicyTestQueue(t)
	_, err := q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{Class: LaneNormal, RejectAboveRatio: 0.90, MaxQueueDepth: 50},
		Lanes: []LaneOverloadPolicy{
			{Lane: "critical", Class: LaneCritical, RejectAboveRatio: 0.98, MaxQueueDepth: 200},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	snap := q.CurrentOverloadPolicy()
	for _, lp := range snap.Lanes {
		if lp.Lane == "critical" {
			if lp.RejectAboveRatio != 0.98 {
				t.Errorf("critical ratio = %.2f", lp.RejectAboveRatio)
			}
			continue
		}
		if lp.MaxQueueDepth != 50 {
			t.Errorf("lane %s MaxQueueDepth = %d, want 50", lp.Lane, lp.MaxQueueDepth)
		}
	}
}

func TestDebugSnapshotIncludesOverloadPolicyVersion(t *testing.T) {
	q := overloadPolicyTestQueue(t)
	initial := q.DebugSnapshot().OverloadPolicyVersion
	v, err := q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{Class: LaneNormal, RejectAboveRatio: 0.88, MaxQueueDepth: 50},
	})
	if err != nil {
		t.Fatal(err)
	}
	snap := q.DebugSnapshot()
	if snap.OverloadPolicyVersion != v {
		t.Errorf("OverloadPolicyVersion = %d, want %d", snap.OverloadPolicyVersion, v)
	}
	if v <= initial {
		t.Errorf("version = %d, want > %d", v, initial)
	}
}

func TestUpdateOverloadPolicyRejectsNegativeMinBackoff(t *testing.T) {
	q := overloadPolicyTestQueue(t)
	_, err := q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{
			Class: LaneNormal, RejectAboveRatio: 0.9, MaxQueueDepth: 10,
			MinBackoff: -1 * time.Millisecond,
		},
	})
	if !errors.Is(err, ErrInvalidOverloadPolicy) {
		t.Errorf("err = %v, want ErrInvalidOverloadPolicy", err)
	}
}

func TestUpdateOverloadPolicyRejectsNegativeMaxBackoff(t *testing.T) {
	q := overloadPolicyTestQueue(t)
	_, err := q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{
			Class: LaneNormal, RejectAboveRatio: 0.9, MaxQueueDepth: 10,
			MaxBackoff: -1 * time.Millisecond,
		},
	})
	if !errors.Is(err, ErrInvalidOverloadPolicy) {
		t.Errorf("err = %v, want ErrInvalidOverloadPolicy", err)
	}
}

func TestUpdateOverloadPolicyRejectsMaxBackoffLessThanMinBackoff(t *testing.T) {
	q := overloadPolicyTestQueue(t)
	_, err := q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{
			Class: LaneNormal, RejectAboveRatio: 0.9, MaxQueueDepth: 10,
			MinBackoff: 2 * time.Second, MaxBackoff: 1 * time.Second,
		},
	})
	if !errors.Is(err, ErrInvalidOverloadPolicy) {
		t.Errorf("err = %v, want ErrInvalidOverloadPolicy", err)
	}
}

func TestUpdateOverloadPolicyRejectsNegativeRatio(t *testing.T) {
	q := overloadPolicyTestQueue(t)
	_, err := q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{
			Class: LaneNormal, RejectAboveRatio: -0.1, MaxQueueDepth: 10,
		},
	})
	if !errors.Is(err, ErrInvalidOverloadPolicy) {
		t.Errorf("err = %v, want ErrInvalidOverloadPolicy", err)
	}
}

func TestUpdateOverloadPolicyRejectsRatioAboveOne(t *testing.T) {
	q := overloadPolicyTestQueue(t)
	_, err := q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{
			Class: LaneNormal, RejectAboveRatio: 1.5, MaxQueueDepth: 10,
		},
	})
	if !errors.Is(err, ErrInvalidOverloadPolicy) {
		t.Errorf("err = %v, want ErrInvalidOverloadPolicy", err)
	}
}

func TestUpdateOverloadPolicyRejectsInvalidShedRatio(t *testing.T) {
	q := overloadPolicyTestQueue(t)
	_, err := q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{
			Class: LaneNormal, RejectAboveRatio: 0.9, ShedAboveRatio: 1.5, MaxQueueDepth: 10,
		},
	})
	if !errors.Is(err, ErrInvalidOverloadPolicy) {
		t.Errorf("err = %v, want ErrInvalidOverloadPolicy", err)
	}
}

func TestUpdateOverloadPolicyRejectsInvalidDegradeRatio(t *testing.T) {
	q := overloadPolicyTestQueue(t)
	_, err := q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{
			Class: LaneNormal, RejectAboveRatio: 0.9, DegradeAboveRatio: -0.1, MaxQueueDepth: 10,
		},
	})
	if !errors.Is(err, ErrInvalidOverloadPolicy) {
		t.Errorf("err = %v, want ErrInvalidOverloadPolicy", err)
	}
}

func TestOverloadPolicyCallerInputMutationDoesNotAffectScheduler(t *testing.T) {
	q := overloadPolicyTestQueue(t)
	lanes := []LaneOverloadPolicy{
		{Lane: "default", Class: LaneNormal, RejectAboveRatio: 0.9, MaxQueueDepth: 10},
	}
	policy := OverloadPolicy{
		Default: LaneOverloadPolicy{Class: LaneNormal, RejectAboveRatio: 0.9, MaxQueueDepth: 10},
		Lanes:   lanes,
	}
	if _, err := q.UpdateOverloadPolicy(policy); err != nil {
		t.Fatal(err)
	}
	lanes[0].MaxQueueDepth = 1
	lanes[0].RejectAboveRatio = 0.1

	snap := q.CurrentOverloadPolicy()
	if snap.Lanes[0].MaxQueueDepth != 10 {
		t.Errorf("MaxQueueDepth = %d, want 10 after mutating caller input", snap.Lanes[0].MaxQueueDepth)
	}
}

func TestOverloadPolicySnapshotDefensiveCopy(t *testing.T) {
	q := overloadPolicyTestQueue(t)
	_, _ = q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{Class: LaneNormal, RejectAboveRatio: 0.9, MaxQueueDepth: 10},
		Lanes: []LaneOverloadPolicy{
			{Lane: "default", Class: LaneNormal, RejectAboveRatio: 0.9, MaxQueueDepth: 10},
		},
	})
	snap := q.CurrentOverloadPolicy()
	snap.Lanes[0].MaxQueueDepth = 1
	snap2 := q.CurrentOverloadPolicy()
	if snap2.Lanes[0].MaxQueueDepth != 10 {
		t.Errorf("MaxQueueDepth = %d, want 10 after mutating returned snapshot", snap2.Lanes[0].MaxQueueDepth)
	}
}

func TestOverloadPolicyDoesNotInterruptRunningJob(t *testing.T) {
	q := overloadPolicyTestQueue(t)
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
	if _, err := q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{
			Class: LaneBestEffort, RejectAboveRatio: 0.01, ShedAboveRatio: 0.01, MaxQueueDepth: 1,
		},
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

func TestOverloadPolicyConcurrentUpdateAndCheck(t *testing.T) {
	q := overloadPolicyTestQueue(t)
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
			_, _ = q.UpdateOverloadPolicy(OverloadPolicy{
				Default: LaneOverloadPolicy{
					Class: LaneNormal, RejectAboveRatio: 0.85 + float64(i%10)*0.01, MaxQueueDepth: 50,
				},
			})
			_ = q.CurrentOverloadPolicy()
			_ = CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "default"})
		}(i)
	}
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = SubmitRequest(ctx, q, Request[struct{}, struct{}]{
				Meta:     RequestMeta{Key: "k", Lane: "default"},
				Overload: OverloadConfig{Enabled: true},
				Handle:   func(context.Context, struct{}) (struct{}, error) { return struct{}{}, nil },
			})
		}()
	}
	wg.Wait()
}

func laneOverloadCounters(t *testing.T, q *Queue, lane Lane) (reject, shed, degrade uint64) {
	t.Helper()
	for _, ln := range q.StatsGCPressure().Lanes {
		if ln.Name == string(lane) {
			c := ln.Counters
			return c.OverloadRejected, c.OverloadShed, c.OverloadDegrade
		}
	}
	t.Fatalf("lane %q not found in StatsGCPressure", lane)
	return 0, 0, 0
}

func TestOverloadRejectionIncrementsOverloadCounters(t *testing.T) {
	t.Run("reject", func(t *testing.T) {
		q, err := New(Config{
			ShardCount:       1,
			WorkerCount:      1,
			QueueSizePerLane: 10,
			LaneQuotas:       map[Lane]int{"default": 1},
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = q.UpdateOverloadPolicy(OverloadPolicy{
			Default: LaneOverloadPolicy{
				Class: LaneNormal, RejectAboveRatio: 0.90, MaxQueueDepth: 100,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		fillQueueDepth(t, q, 9)

		rejBefore, shedBefore, degBefore := laneOverloadCounters(t, q, "default")
		rejectedBefore := q.StatsGCPressure().Lanes[0].Counters.Rejected

		err = CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "default"})
		if !errors.Is(err, ErrOverloadRejected) {
			t.Fatalf("CheckOverload = %v, want ErrOverloadRejected", err)
		}

		rejAfter, shedAfter, degAfter := laneOverloadCounters(t, q, "default")
		rejectedAfter := q.StatsGCPressure().Lanes[0].Counters.Rejected

		if rejAfter != rejBefore+1 {
			t.Errorf("OverloadRejected = %d, want %d", rejAfter, rejBefore+1)
		}
		if shedAfter != shedBefore {
			t.Errorf("OverloadShed = %d, want %d", shedAfter, shedBefore)
		}
		if degAfter != degBefore {
			t.Errorf("OverloadDegrade = %d, want %d", degAfter, degBefore)
		}
		if rejectedAfter != rejectedBefore+1 {
			t.Errorf("Rejected = %d, want %d", rejectedAfter, rejectedBefore+1)
		}
	})

	t.Run("shed", func(t *testing.T) {
		q := overloadPolicyTestQueue(t)
		_, err := q.UpdateOverloadPolicy(OverloadPolicy{
			Default: LaneOverloadPolicy{Class: LaneNormal, RejectAboveRatio: 0.90, MaxQueueDepth: 100},
			Lanes: []LaneOverloadPolicy{
				{Lane: "best_effort", Class: LaneBestEffort, RejectAboveRatio: 0.75, ShedAboveRatio: 0.50, MaxQueueDepth: 100},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, lane := range []Lane{"default", "critical", "best_effort"} {
			for i := 0; i < 8; i++ {
				_ = q.Submit(context.Background(), Job{
					Key: "k", Lane: lane, Run: func(context.Context) error { return nil },
				})
			}
		}

		rejBefore, shedBefore, degBefore := laneOverloadCounters(t, q, "best_effort")
		err = CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "best_effort"})
		if !errors.Is(err, ErrOverloadShed) {
			t.Fatalf("CheckOverload = %v, want ErrOverloadShed", err)
		}

		rejAfter, shedAfter, degAfter := laneOverloadCounters(t, q, "best_effort")
		if rejAfter != rejBefore {
			t.Errorf("OverloadRejected = %d, want %d", rejAfter, rejBefore)
		}
		if shedAfter != shedBefore+1 {
			t.Errorf("OverloadShed = %d, want %d", shedAfter, shedBefore+1)
		}
		if degAfter != degBefore {
			t.Errorf("OverloadDegrade = %d, want %d", degAfter, degBefore)
		}
	})

	t.Run("degrade", func(t *testing.T) {
		q, err := New(Config{
			ShardCount:       1,
			WorkerCount:      1,
			QueueSizePerLane: 10,
			LaneQuotas:       map[Lane]int{"deg": 1},
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = q.UpdateOverloadPolicy(OverloadPolicy{
			Default: LaneOverloadPolicy{Class: LaneNormal, RejectAboveRatio: 0.90, MaxQueueDepth: 100},
			Lanes: []LaneOverloadPolicy{
				{Lane: "deg", Class: LaneNormal, RejectAboveRatio: 0.95, ShedAboveRatio: 1.0,
					DegradeAboveRatio: 0.01, MaxQueueDepth: 100},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		_ = q.Submit(context.Background(), Job{
			Key: "fill", Lane: "deg", Run: func(context.Context) error { return nil },
		})

		rejBefore, shedBefore, degBefore := laneOverloadCounters(t, q, "deg")
		err = CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "deg"})
		if !errors.Is(err, ErrOverloadDegraded) {
			t.Fatalf("CheckOverload = %v, want ErrOverloadDegraded", err)
		}

		rejAfter, shedAfter, degAfter := laneOverloadCounters(t, q, "deg")
		if rejAfter != rejBefore {
			t.Errorf("OverloadRejected = %d, want %d", rejAfter, rejBefore)
		}
		if shedAfter != shedBefore {
			t.Errorf("OverloadShed = %d, want %d", shedAfter, shedBefore)
		}
		if degAfter != degBefore+1 {
			t.Errorf("OverloadDegrade = %d, want %d", degAfter, degBefore+1)
		}
	})
}
