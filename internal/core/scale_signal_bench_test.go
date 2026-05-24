// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"sync"
	"testing"
	"time"
)

func BenchmarkScaleSignalHealthy(b *testing.B) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(4, 2, 32, reg)
	s.ConfigureAutoscalingSignal(testAutoscalingCfg())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.ScaleSignalSnapshot()
	}
}

func BenchmarkScaleSignalHighQueueDepth(b *testing.B) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 16, reg)
	s.ConfigureAutoscalingSignal(testAutoscalingCfg())
	s.ConfigureShardPressure(testShardPressureConfig())
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
	for i := 0; i < 14; i++ {
		_, _, _ = s.Enqueue(InternalJob{KeyHash: HashKey("k"), LaneID: 0, Run: run})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.ScaleSignalSnapshot()
	}
}

func BenchmarkScaleSignalWithHotShardDiagnostics(b *testing.B) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(4, 2, 32, reg)
	s.ConfigureAutoscalingSignal(testAutoscalingCfg())
	s.ConfigureShardPressure(testShardPressureConfig())
	s.ConfigureHotKey(HotKeyConfig{
		Enabled: true, MaxTrackedKeysPerShard: 16,
		DetectionWindow: time.Minute, HotKeyDepthRatio: 0.3,
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.ScaleSignalSnapshot()
	}
}

func BenchmarkScaleSignalConcurrentRead(b *testing.B) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(4, 2, 32, reg)
	s.ConfigureAutoscalingSignal(testAutoscalingCfg())
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = s.ScaleSignalSnapshot()
		}
	})
}

func BenchmarkScaleSignalDisabled(b *testing.B) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(4, 2, 32, reg)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.ScaleSignalSnapshot()
	}
}

func TestScaleSignalConcurrentReadRace(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(2, 2, 32, reg)
	s.ConfigureAutoscalingSignal(testAutoscalingCfg())
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = s.ScaleSignalSnapshot()
			}
		}()
	}
	wg.Wait()
}
