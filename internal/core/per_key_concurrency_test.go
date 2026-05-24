// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestRacePerKeyAdmissionSubmitEvaluate(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(2, 4, 32, reg)
	s.ConfigureHotKey(HotKeyConfig{
		Enabled: true, MaxTrackedKeysPerShard: 32,
		DetectionWindow: time.Minute, HotKeyDepthRatio: 0.35,
	})
	if err := s.ConfigurePerKeyAdmission(PerKeyAdmissionConfig{
		Enabled: true, DefaultAction: PerKeyMitigationThrottle,
		PressureRatioThreshold: 0.35,
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.Start(ctx)
	defer func() { _ = s.Stop(context.Background(), false) }()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				key := HashKey("k")
				if id == 0 {
					key = HashKey("hot")
				}
				_, _, _ = s.Enqueue(InternalJob{
					KeyHash: key, LaneID: 0,
					Run: func(ctx context.Context) error { return nil },
				})
				_ = s.EvaluatePerKeyAdmission(id%2, key, 0)
			}
		}(g)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				_ = s.DebugSnapshot()
			}
		}()
	}
	wg.Wait()
}
