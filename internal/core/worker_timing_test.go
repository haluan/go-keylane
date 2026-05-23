// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"testing"
	"time"
)

func TestWorkerTimingContext(t *testing.T) {
	accepted := time.Now().Add(-10 * time.Millisecond)
	started := time.Now()
	wt := WorkerTiming{AcceptedAt: accepted, StartedAt: started}
	ctx := ContextWithWorkerTiming(context.Background(), wt)

	got, ok := WorkerTimingFromContext(ctx)
	if !ok {
		t.Fatal("WorkerTimingFromContext = false")
	}
	if got.QueueWaitDuration() <= 0 {
		t.Errorf("QueueWaitDuration = %v, want > 0", got.QueueWaitDuration())
	}
}

func TestWorkerTimingQueueWaitZeroWithoutAcceptedAt(t *testing.T) {
	wt := WorkerTiming{StartedAt: time.Now()}
	if got := wt.QueueWaitDuration(); got != 0 {
		t.Errorf("QueueWaitDuration = %v, want 0", got)
	}
}
