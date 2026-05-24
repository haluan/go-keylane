// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func TestV05GoroutineLeakAfterHotKeyBurstShutdown(t *testing.T) {
	cfg := v05EnabledConfig()
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	before := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		key := "hot-leak"
		if i%4 != 0 {
			key = "other-" + string(rune('a'+i%26))
		}
		_ = q.Submit(ctx, Job{
			Key: key, Lane: "default",
			Run: func(context.Context) error { return nil },
		})
	}
	_ = q.DebugSnapshot()
	_ = q.ScaleSignal()
	cancel()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := q.Stop(stopCtx, WithDrain(false)); err != nil {
		t.Fatal(err)
	}
	eventuallyNoGoroutineGrowth(t, before, 8)
}

func TestV05GoroutineLeakDisabledFeaturesShutdown(t *testing.T) {
	q := newV05DisabledQueue(t)
	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		_ = q.Submit(ctx, Job{
			Key: "k", Lane: "default",
			Run: func(context.Context) error { return nil },
		})
	}
	cancel()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := q.Stop(stopCtx, WithDrain(false)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	eventuallyNoGoroutineGrowth(t, before, 6)
}
