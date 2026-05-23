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

func lowAllocTestConfig() Config {
	cfg := newTestConfig()
	cfg.Observability = LowAllocationObservabilityConfig()
	return cfg
}

func startQueue(t *testing.T, cfg Config) (*Queue, context.Context, context.CancelFunc) {
	t.Helper()
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := q.Start(ctx); err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(cancel)
	return q, ctx, cancel
}

func runUntilDone(t *testing.T, q *Queue, ctx context.Context, n int) {
	t.Helper()
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_ = q.Submit(ctx, Job{
				Key:  "k",
				Lane: "default",
				Run:  func(ctx context.Context) error { return nil },
			})
		}()
	}
	wg.Wait()
	time.Sleep(50 * time.Millisecond)
}

func TestLowAllocationDisablesQueueWaitTiming(t *testing.T) {
	q, ctx, _ := startQueue(t, lowAllocTestConfig())
	runUntilDone(t, q, ctx, 8)
	snap := q.StatsGCPressure()
	if snap.QueueWait.Count != 0 {
		t.Errorf("QueueWait.Count = %d, want 0 when EnableQueueWaitTiming is false", snap.QueueWait.Count)
	}
}

func TestLowAllocationDisablesRunTiming(t *testing.T) {
	q, ctx, _ := startQueue(t, lowAllocTestConfig())
	runUntilDone(t, q, ctx, 8)
	snap := q.StatsGCPressure()
	if snap.Run.Count != 0 {
		t.Errorf("Run.Count = %d, want 0 when EnableRunTiming is false", snap.Run.Count)
	}
}

func TestLowAllocationDisablesHooks(t *testing.T) {
	timingDone := make(chan JobTimingEvent, 1)
	cfg := lowAllocTestConfig()
	cfg.Observability.EnableHooks = false
	cfg.Observability.Hooks = Hooks{
		OnJobTiming: func(ev JobTimingEvent) {
			timingDone <- ev
		},
	}
	q, _, _ := startQueue(t, cfg)

	_ = q.Submit(context.Background(), Job{
		Key:  "k",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	})
	time.Sleep(100 * time.Millisecond)
	select {
	case <-timingDone:
		t.Fatal("OnJobTiming should not run when EnableHooks is false")
	default:
	}
}

func TestLowAllocationCountersStillIncrement(t *testing.T) {
	q, ctx, _ := startQueue(t, lowAllocTestConfig())
	runUntilDone(t, q, ctx, 5)
	snap := q.StatsGCPressure()
	if len(snap.Lanes) == 0 {
		t.Fatal("expected lane stats")
	}
	var defaultLane *LaneStatsGCPressure
	for i := range snap.Lanes {
		if snap.Lanes[i].Name == "default" {
			defaultLane = &snap.Lanes[i]
			break
		}
	}
	if defaultLane == nil {
		t.Fatal("default lane not found")
	}
	if defaultLane.Counters.Submitted < 5 {
		t.Errorf("Submitted = %d, want at least 5", defaultLane.Counters.Submitted)
	}
	if defaultLane.Counters.Completed < 5 {
		t.Errorf("Completed = %d, want at least 5", defaultLane.Counters.Completed)
	}
}

func TestLowAllocationStatsGCPressureValid(t *testing.T) {
	q, ctx, _ := startQueue(t, lowAllocTestConfig())
	runUntilDone(t, q, ctx, 2)
	snap := q.StatsGCPressure()
	if snap.Version != StatsGCPressureVersion {
		t.Errorf("Version = %q, want %q", snap.Version, StatsGCPressureVersion)
	}
	if snap.ShardCount != 4 || snap.WorkerCount != 2 {
		t.Errorf("unexpected counts: shards=%d workers=%d", snap.ShardCount, snap.WorkerCount)
	}
}

func TestLowAllocationDebugSnapshotValid(t *testing.T) {
	q, ctx, _ := startQueue(t, lowAllocTestConfig())
	runUntilDone(t, q, ctx, 2)
	snap := q.DebugSnapshot()
	if snap.Version != DebugSnapshotVersion {
		t.Errorf("Version = %q, want %q", snap.Version, DebugSnapshotVersion)
	}
	if snap.ShardCount != 4 {
		t.Errorf("ShardCount = %d, want 4", snap.ShardCount)
	}
}

func TestLowAllocationSlowJobHookNotEmitted(t *testing.T) {
	slowDone := make(chan SlowJobEvent, 1)
	cfg := lowAllocTestConfig()
	cfg.Observability.EnableHooks = false
	cfg.Observability.SlowJobThreshold = time.Millisecond
	cfg.Observability.Hooks = Hooks{
		OnSlowJob: func(ev SlowJobEvent) {
			slowDone <- ev
		},
	}
	q, _, _ := startQueue(t, cfg)

	_ = q.Submit(context.Background(), Job{
		Key:  "slow",
		Lane: "default",
		Run: func(ctx context.Context) error {
			time.Sleep(5 * time.Millisecond)
			return nil
		},
	})
	time.Sleep(100 * time.Millisecond)
	select {
	case <-slowDone:
		t.Fatal("OnSlowJob should not run when EnableHooks is false")
	default:
	}
}

func TestResolveObservabilityLowAllocationPreset(t *testing.T) {
	got := ResolveObservabilityConfig(ObservabilityConfig{LowAllocationMode: true})
	want := LowAllocationObservabilityConfig()
	if got.EnableQueueWaitTiming || got.EnableRunTiming || got.EnableHooks {
		t.Errorf("preset should disable timing and hooks: %+v", got)
	}
	if !got.EnableStats || !got.EnableCounters || !got.EnableDebugSnapshot {
		t.Errorf("preset should keep stats/counters/debug: %+v", got)
	}
	if want.LowAllocationMode != got.LowAllocationMode {
		t.Error("LowAllocationMode mismatch")
	}
}

func TestEnableStatsFalseReturnsEmptySnapshot(t *testing.T) {
	cfg := newTestConfig()
	cfg.Observability = DefaultObservabilityConfig()
	cfg.Observability.EnableStats = false
	q, ctx, _ := startQueue(t, cfg)
	runUntilDone(t, q, ctx, 3)
	snap := q.StatsGCPressure()
	if len(snap.Shards) != 0 || len(snap.Lanes) != 0 {
		t.Errorf("expected empty shard/lane slices when EnableStats false, got shards=%d lanes=%d",
			len(snap.Shards), len(snap.Lanes))
	}
	if snap.TotalQueued != 0 {
		t.Errorf("TotalQueued = %d, want 0 when EnableStats false", snap.TotalQueued)
	}
}

func TestEnableDebugSnapshotFalseReturnsMinimalSnapshot(t *testing.T) {
	cfg := newTestConfig()
	cfg.Observability = DefaultObservabilityConfig()
	cfg.Observability.EnableDebugSnapshot = false
	q, ctx, _ := startQueue(t, cfg)
	runUntilDone(t, q, ctx, 2)
	snap := q.DebugSnapshot()
	if len(snap.Shards) != 0 || len(snap.HotShards) != 0 {
		t.Error("expected empty debug snapshot when EnableDebugSnapshot false")
	}
}

func TestLowAllocationModeWinsOverExplicitTiming(t *testing.T) {
	cfg := newTestConfig()
	cfg.Observability = ObservabilityConfig{
		LowAllocationMode:     true,
		EnableQueueWaitTiming: true,
		EnableRunTiming:       true,
		EnableHooks:           true,
	}
	got := ResolveObservabilityConfig(cfg.Observability)
	if got.EnableQueueWaitTiming || got.EnableRunTiming || got.EnableHooks {
		t.Error("LowAllocationMode preset should win over explicit enable flags")
	}
}

func TestLowAllocationSubmitStillSucceeds(t *testing.T) {
	q, ctx, _ := startQueue(t, lowAllocTestConfig())
	if err := q.Submit(ctx, Job{
		Key:  "k",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
}

func TestLowAllocationSubmitValueAwait(t *testing.T) {
	q, ctx, _ := startQueue(t, lowAllocTestConfig())
	f, err := SubmitValue(ctx, q, ValueJob[int]{
		Key:  "k",
		Lane: "default",
		Run:  func(ctx context.Context) (int, error) { return 42, nil },
	})
	if err != nil {
		t.Fatalf("SubmitValue: %v", err)
	}
	v, err := f.Await(ctx)
	if err != nil || v != 42 {
		t.Fatalf("Await: val=%d err=%v", v, err)
	}
}

// TestLowAllocationSubmitRequestNilRequestHooksAllocs lives in request_observability_test.go.

func TestLowAllocationFailedJobStillCountsWhenCountersOn(t *testing.T) {
	q, ctx, _ := startQueue(t, lowAllocTestConfig())
	_ = q.Submit(ctx, Job{
		Key:  "k",
		Lane: "default",
		Run:  func(ctx context.Context) error { return errors.New("fail") },
	})
	time.Sleep(100 * time.Millisecond)
	snap := q.StatsGCPressure()
	var failed uint64
	for _, ln := range snap.Lanes {
		if ln.Name == "default" {
			failed = ln.Counters.Failed
		}
	}
	if failed < 1 {
		t.Errorf("Failed = %d, want at least 1", failed)
	}
}
