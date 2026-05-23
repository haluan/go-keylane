// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestDebugSnapshotHotKeyCandidate(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 64, reg)
	s.ConfigureHotKey(HotKeyConfig{
		Enabled:                true,
		MaxTrackedKeysPerShard: 16,
		DetectionWindow:        time.Minute,
		HotKeyDepthRatio:       0.3,
		HotKeyWaitRatio:        0.9,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Stop(context.Background(), false) }()

	hotHash := HashKey("hot-tenant")
	run := func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	for i := 0; i < 40; i++ {
		_, _, err := s.Enqueue(InternalJob{KeyHash: hotHash, LaneID: 0, Run: run})
		if err != nil {
			t.Fatal(err)
		}
	}
	snap := s.DebugSnapshot()
	if len(snap.Shards) != 1 {
		t.Fatalf("shards = %d", len(snap.Shards))
	}
	if snap.Shards[0].HotKeyCandidate == nil {
		t.Fatal("expected HotKeyCandidate on shard snapshot")
	}
	if snap.Shards[0].HotKeyCandidate.KeyHash != hotHash {
		t.Fatalf("KeyHash = %d", snap.Shards[0].HotKeyCandidate.KeyHash)
	}
	if snap.Shards[0].HotKeyCandidate.Key != "" {
		t.Fatal("raw key should not be exposed without ExposeRawKey")
	}
}

func TestDebugSnapshotHotKeyCopyOut(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 32, reg)
	s.ConfigureHotKey(HotKeyConfig{
		Enabled: true, MaxTrackedKeysPerShard: 8,
		DetectionWindow: time.Minute, HotKeyDepthRatio: 0.2,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)
	defer func() { _ = s.Stop(context.Background(), false) }()

	h := HashKey("x")
	for i := 0; i < 20; i++ {
		_, _, _ = s.Enqueue(InternalJob{
			KeyHash: h, LaneID: 0,
			Run: func(ctx context.Context) error { time.Sleep(10 * time.Millisecond); return nil },
		})
	}
	snap1 := s.DebugSnapshot()
	if snap1.Shards[0].HotKeyCandidate == nil {
		t.Fatal("expected candidate")
	}
	snap1.Shards[0].HotKeyCandidate.KeyHash = 0
	snap2 := s.DebugSnapshot()
	if snap2.Shards[0].HotKeyCandidate == nil || snap2.Shards[0].HotKeyCandidate.KeyHash != h {
		t.Fatal("snapshot should be copy-out, not alias tracker memory")
	}
}

func TestRaceHotKeyTrackerSubmitDequeue(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(2, 4, 32, reg)
	s.ConfigureHotKey(HotKeyConfig{
		Enabled: true, MaxTrackedKeysPerShard: 32,
		DetectionWindow: time.Minute, HotKeyDepthRatio: 0.35,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)
	defer func() { _ = s.Stop(context.Background(), false) }()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				key := HashKey(string(rune('a' + (i % 26))))
				if id == 0 {
					key = HashKey("hot")
				}
				_, _, _ = s.Enqueue(InternalJob{
					KeyHash: key, LaneID: 0,
					Run: func(ctx context.Context) error { return nil },
				})
			}
		}(g)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = s.DebugSnapshot()
			}
		}()
	}
	wg.Wait()
}
