// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"sync"
	"testing"
)

func TestV05RuntimeRaceMixedSubmitAndDiagnostics(t *testing.T) {
	cfg := v05EnabledConfig()
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

	var wg sync.WaitGroup
	stop := make(chan struct{})
	defer close(stop)

	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 40; j++ {
				select {
				case <-stop:
					return
				default:
				}
				key := "hot"
				if (id+j)%3 != 0 {
					key = "other-" + string(rune('a'+j%26))
				}
				_ = q.Submit(context.Background(), Job{
					Key: key, Lane: "default",
					Run: func(context.Context) error { return nil },
				})
			}
		}(i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				select {
				case <-stop:
					return
				default:
				}
				_ = q.DebugSnapshot()
				_ = q.ScaleSignal()
				_ = q.PressureSummary()
			}
		}()
	}
	wg.Wait()
}

func TestV05RuntimeRaceConfigureAutoscaling(t *testing.T) {
	cfg := v05EnabledConfig()
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = q.ScaleSignal()
			}
		}()
	}
	for i := 0; i < 30; i++ {
		c := cfg.AutoscalingSignal
		NormalizeAutoscalingSignalConfig(&c)
		c.ConsecutiveWindows = 1 + (i % 3)
		q.sched.ConfigureAutoscalingSignal(c)
	}
	wg.Wait()
}
