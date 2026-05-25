// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"testing"
	"time"
)

func sumPerKeyDecisionTotals(totals []PerKeyAdmissionDecisionTotal) uint64 {
	var n uint64
	for _, d := range totals {
		n += d.Count
	}
	return n
}

func TestRetrySuppressionSnapshotDoesNotMutatePerKeyState(t *testing.T) {
	cfg := perKeyTestConfig()
	cfg.RetrySuppression = RetrySuppressionPolicy{Enabled: true}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop(context.Background()) }()

	hot := "hot-snapshot-key"
	for i := 0; i < 40; i++ {
		_ = q.Submit(ctx, Job{
			Key:  hot,
			Lane: "default",
			Run:  func(context.Context) error { return nil },
		})
	}

	before := sumPerKeyDecisionTotals(q.PerKeyAdmissionDecisionTotals())
	for i := 0; i < 10; i++ {
		_ = q.RetrySuppressionSnapshot(hot, "default", q.ShardIDForKey(hot))
	}
	after := sumPerKeyDecisionTotals(q.PerKeyAdmissionDecisionTotals())
	if after != before {
		t.Fatalf("decision totals changed: before=%d after=%d", before, after)
	}
}

func TestRetrySuppressionSnapshotHotKeyWithoutPerKeyAdmission(t *testing.T) {
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 64,
		LaneQuotas:       map[Lane]int{"default": 2},
		HotKey: HotKeyConfig{
			Enabled:                true,
			MaxTrackedKeysPerShard: 16,
			DetectionWindow:        time.Minute,
			HotKeyDepthRatio:       0.35,
			HotKeyWaitRatio:        0.9,
		},
		PerKeyAdmission: PerKeyAdmissionConfig{Enabled: false},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop(context.Background()) }()

	hot := "noisy-tenant"
	for i := 0; i < 30; i++ {
		if err := q.Submit(ctx, Job{
			Key:  hot,
			Lane: "default",
			Run:  func(context.Context) error { return nil },
		}); err != nil {
			t.Fatal(err)
		}
	}

	snap := q.RetrySuppressionSnapshot(hot, "default", q.ShardIDForKey(hot))
	if !snap.HotKeyCandidate {
		t.Fatal("expected HotKeyCandidate with hot-key tracking and per-key admission disabled")
	}
}

func TestRetrySuppressionSnapshotDoesNotExpireStaleTrackerEntries(t *testing.T) {
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 64,
		LaneQuotas:       map[Lane]int{"default": 8},
		HotKey: HotKeyConfig{
			Enabled:                true,
			MaxTrackedKeysPerShard: 16,
			DetectionWindow:        40 * time.Millisecond,
			HotKeyDepthRatio:       0.35,
			HotKeyWaitRatio:        0.9,
		},
		RetrySuppression: RetrySuppressionPolicy{Enabled: true},
		PerKeyAdmission:  PerKeyAdmissionConfig{Enabled: false},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop(context.Background()) }()

	keys := []string{"stale-tenant-a", "stale-tenant-b"}
	for _, key := range keys {
		for i := 0; i < 25; i++ {
			if err := q.Submit(ctx, Job{
				Key:  key,
				Lane: "default",
				Run:  func(context.Context) error { return nil },
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	time.Sleep(50 * time.Millisecond)

	trackerLenBefore := q.sched.HotKeyTrackerLen(0)
	if trackerLenBefore < 2 {
		t.Fatalf("tracker len = %d, want at least 2 indexed keys", trackerLenBefore)
	}

	for i := 0; i < 5; i++ {
		_ = q.RetrySuppressionSnapshot(keys[0], "default", q.ShardIDForKey(keys[0]))
	}
	if got := q.sched.HotKeyTrackerLen(0); got != trackerLenBefore {
		t.Fatalf("tracker len changed: before=%d after=%d", trackerLenBefore, got)
	}
}
