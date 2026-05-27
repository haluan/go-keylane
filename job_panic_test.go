// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func newJobPanicTestQueue(t *testing.T) (*Queue, context.Context, context.CancelFunc) {
	t.Helper()
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 32,
		LaneQuotas:       map[Lane]int{"default": 1},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := q.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = q.Stop(stopCtx, WithDrain(true))
		cancel()
	})
	return q, ctx, cancel
}

func TestJobPanicConvertedToFailure(t *testing.T) {
	q, ctx, _ := newJobPanicTestQueue(t)
	var panicDone sync.WaitGroup
	panicDone.Add(1)
	if err := q.Submit(ctx, Job{
		Key:  "panic-1",
		Lane: "default",
		Run: func(context.Context) error {
			panicDone.Done()
			panic("job panic")
		},
	}); err != nil {
		t.Fatal(err)
	}
	panicDone.Wait()
	time.Sleep(50 * time.Millisecond)

	var panicCount uint64
	for _, lane := range q.StatsGCPressure().Lanes {
		if lane.Name == "default" {
			panicCount = lane.Counters.Panicked
			break
		}
	}
	if panicCount != 1 {
		t.Fatalf("Panicked = %d, want 1", panicCount)
	}
}

func TestJobPanicDoesNotStopWorker(t *testing.T) {
	q, ctx, _ := newJobPanicTestQueue(t)
	done := make(chan struct{}, 1)
	if err := q.Submit(ctx, Job{
		Key:  "panic-first",
		Lane: "default",
		Run: func(context.Context) error {
			panic("first")
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.Submit(ctx, Job{
		Key:  "ok-second",
		Lane: "default",
		Run: func(context.Context) error {
			close(done)
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("second job did not complete; worker may have died")
	}
}

func TestSubmitValueJobPanicAwaitReturnsError(t *testing.T) {
	before := runtime.NumGoroutine()
	q, ctx, _ := newJobPanicTestQueue(t)

	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key:  "v-panic",
		Lane: "default",
		Run: func(context.Context) (int, error) {
			panic("value panic")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, awaitErr := future.Await(ctx)
	if !errors.Is(awaitErr, ErrJobPanicked) {
		t.Fatalf("await err = %v, want ErrJobPanicked", awaitErr)
	}
	f, ok := AsFailure(awaitErr)
	if !ok || f.Kind != FailurePanic {
		t.Fatalf("failure = %+v, want FailurePanic", f)
	}
	if !strings.Contains(awaitErr.Error(), "value panic") {
		t.Fatalf("error = %v", awaitErr)
	}
	eventuallyNoGoroutineGrowth(t, before, 10)
}
