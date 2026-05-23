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

func testQueueConfig() Config {
	return Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 100,
		LaneQuotas: map[Lane]int{
			"default": 2,
			"fast":    1,
		},
	}
}

func TestQuotaPolicyRejectsInvalidDefaultQuota(t *testing.T) {
	q, err := New(testQueueConfig())
	if err != nil {
		t.Fatal(err)
	}
	_, err = q.UpdateQuotaPolicy(QuotaPolicy{DefaultQuota: 0})
	if !errors.Is(err, ErrInvalidLaneQuota) {
		t.Errorf("err = %v, want ErrInvalidLaneQuota", err)
	}
}

func TestQuotaPolicyRejectsQuotaTooLarge(t *testing.T) {
	q, _ := New(testQueueConfig())
	_, err := q.UpdateQuotaPolicy(QuotaPolicy{
		DefaultQuota: 1,
		LaneQuotas:   map[Lane]uint32{"default": MaxLaneQuota + 1},
	})
	if !errors.Is(err, ErrQuotaTooLarge) {
		t.Errorf("err = %v, want ErrQuotaTooLarge", err)
	}
}

func TestQuotaPolicyRejectsLaneQuotaTooLow(t *testing.T) {
	q, _ := New(testQueueConfig())
	_, err := q.UpdateQuotaPolicy(QuotaPolicy{
		DefaultQuota: 1,
		LaneQuotas:   map[Lane]uint32{"default": 0},
	})
	if !errors.Is(err, ErrInvalidLaneQuota) {
		t.Errorf("err = %v, want ErrInvalidLaneQuota", err)
	}
}

func TestQuotaPolicyRejectsUnknownLane(t *testing.T) {
	q, _ := New(testQueueConfig())
	_, err := q.UpdateQuotaPolicy(QuotaPolicy{
		DefaultQuota: 1,
		LaneQuotas:   map[Lane]uint32{"unknown": 2},
	})
	if !errors.Is(err, ErrInvalidQuotaPolicy) {
		t.Errorf("err = %v, want ErrInvalidQuotaPolicy", err)
	}
}

func TestQuotaPolicyRejectsInvalidLaneName(t *testing.T) {
	q, _ := New(testQueueConfig())
	_, err := q.UpdateQuotaPolicy(QuotaPolicy{
		DefaultQuota: 1,
		LaneQuotas:   map[Lane]uint32{"": 2},
	})
	if !errors.Is(err, ErrInvalidLane) {
		t.Errorf("err = %v, want ErrInvalidLane", err)
	}
}

func TestQuotaPolicyAcceptsValidPolicy(t *testing.T) {
	q, _ := New(testQueueConfig())
	v, err := q.UpdateQuotaPolicy(QuotaPolicy{
		DefaultQuota: 1,
		LaneQuotas:   map[Lane]uint32{"default": 3, "fast": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if v != 1 {
		t.Errorf("version = %d, want 1", v)
	}
	snap := q.CurrentQuotaPolicy()
	if snap.Version != v {
		t.Errorf("CurrentQuotaPolicy().Version = %d, want %d from UpdateQuotaPolicy", snap.Version, v)
	}
	if snap.LaneQuotas[Lane("default")] != 3 {
		t.Errorf("default quota = %d, want 3", snap.LaneQuotas[Lane("default")])
	}
}

func TestQuotaPolicyCurrentQuotaPolicyReturnsLatestVersion(t *testing.T) {
	q, _ := New(testQueueConfig())

	initial := q.CurrentQuotaPolicy()
	if initial.Version != 0 {
		t.Errorf("initial Version = %d, want 0", initial.Version)
	}

	v1, err := q.UpdateQuotaPolicy(QuotaPolicy{DefaultQuota: 2})
	if err != nil {
		t.Fatal(err)
	}
	snap1 := q.CurrentQuotaPolicy()
	if snap1.Version != v1 {
		t.Errorf("after first update: CurrentQuotaPolicy().Version = %d, want %d", snap1.Version, v1)
	}

	v2, err := q.UpdateQuotaPolicy(QuotaPolicy{DefaultQuota: 4})
	if err != nil {
		t.Fatal(err)
	}
	snap2 := q.CurrentQuotaPolicy()
	if snap2.Version != v2 {
		t.Errorf("after second update: CurrentQuotaPolicy().Version = %d, want %d", snap2.Version, v2)
	}
	if snap2.Version <= snap1.Version {
		t.Errorf("versions %d then %d, want strictly increasing in CurrentQuotaPolicy", snap1.Version, snap2.Version)
	}
}

func TestQuotaPolicyVersionIncreasesMonotonically(t *testing.T) {
	q, _ := New(testQueueConfig())
	v1, err := q.UpdateQuotaPolicy(QuotaPolicy{DefaultQuota: 2})
	if err != nil {
		t.Fatal(err)
	}
	v2, err := q.UpdateQuotaPolicy(QuotaPolicy{DefaultQuota: 3})
	if err != nil {
		t.Fatal(err)
	}
	if v2 <= v1 {
		t.Errorf("versions %d then %d, want strictly increasing", v1, v2)
	}
	if q.CurrentQuotaPolicy().Version != v2 {
		t.Errorf("CurrentQuotaPolicy().Version = %d, want latest %d", q.CurrentQuotaPolicy().Version, v2)
	}
}

func TestQuotaPolicyCallerMapMutationDoesNotAffectScheduler(t *testing.T) {
	q, _ := New(testQueueConfig())
	policy := QuotaPolicy{
		DefaultQuota: 1,
		LaneQuotas:   map[Lane]uint32{"default": 4},
	}
	_, err := q.UpdateQuotaPolicy(policy)
	if err != nil {
		t.Fatal(err)
	}
	policy.LaneQuotas["default"] = 1

	snap := q.CurrentQuotaPolicy()
	if snap.LaneQuotas[Lane("default")] != 4 {
		t.Errorf("scheduler quota = %d, want 4 after caller mutated input", snap.LaneQuotas[Lane("default")])
	}
	snap.LaneQuotas[Lane("default")] = 99
	snap2 := q.CurrentQuotaPolicy()
	if snap2.LaneQuotas[Lane("default")] != 4 {
		t.Errorf("scheduler quota = %d, want 4 after mutating returned snapshot", snap2.LaneQuotas[Lane("default")])
	}
}

func TestQuotaPolicyDefaultAppliesToUnspecifiedLanes(t *testing.T) {
	q, _ := New(testQueueConfig())
	_, err := q.UpdateQuotaPolicy(QuotaPolicy{DefaultQuota: 5})
	if err != nil {
		t.Fatal(err)
	}
	snap := q.CurrentQuotaPolicy()
	if snap.LaneQuotas[Lane("fast")] != 5 {
		t.Errorf("fast quota = %d, want 5 from default", snap.LaneQuotas[Lane("fast")])
	}
}

func TestQuotaPolicyUpdateBeforeStart(t *testing.T) {
	q, _ := New(testQueueConfig())
	_, err := q.UpdateQuotaPolicy(QuotaPolicy{
		DefaultQuota: 1,
		LaneQuotas:   map[Lane]uint32{"default": 5},
	})
	if err != nil {
		t.Fatalf("update before start: %v", err)
	}
}

func TestQuotaPolicyUpdateRejectedAfterStop(t *testing.T) {
	q, _ := New(testQueueConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := q.Stop(stopCtx, WithDrain(true)); err != nil {
		t.Fatal(err)
	}
	_, err := q.UpdateQuotaPolicy(QuotaPolicy{DefaultQuota: 2})
	if !errors.Is(err, ErrStopped) {
		t.Errorf("err = %v, want ErrStopped", err)
	}
}

func TestQuotaPolicyStatsReflectUpdate(t *testing.T) {
	q, _ := New(testQueueConfig())
	_, _ = q.UpdateQuotaPolicy(QuotaPolicy{
		DefaultQuota: 1,
		LaneQuotas:   map[Lane]uint32{"default": 7},
	})
	stats := q.Stats()
	if stats.Shards[0].Lanes[0].Quota != 7 {
		t.Errorf("Stats quota = %d, want 7", stats.Shards[0].Lanes[0].Quota)
	}
}

func TestQuotaPolicyDoesNotInterruptRunningJob(t *testing.T) {
	q, _ := New(testQueueConfig())
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
	if _, err := q.UpdateQuotaPolicy(QuotaPolicy{DefaultQuota: 1}); err != nil {
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

func TestQuotaPolicyConcurrentUpdateAndSubmit(t *testing.T) {
	q, _ := New(testQueueConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = q.Submit(ctx, Job{
				Key:  "k",
				Lane: "default",
				Run:  func(context.Context) error { return nil },
			})
		}(i)
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = q.UpdateQuotaPolicy(QuotaPolicy{
				DefaultQuota: uint32(1 + (i % 3)),
				LaneQuotas:   map[Lane]uint32{"default": uint32(1 + (i % 5))},
			})
			_ = q.CurrentQuotaPolicy()
		}(i)
	}
	wg.Wait()
}
