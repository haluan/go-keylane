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

func TestFutureAwaitSuccess(t *testing.T) {
	f := newResultFuture[int]()
	go func() {
		f.completeValue(42, nil)
	}()

	val, err := f.Await(context.Background())
	if err != nil {
		t.Errorf("Await() failed: %v", err)
	}
	if val != 42 {
		t.Errorf("val = %d, want 42", val)
	}
}

func TestFutureAwaitError(t *testing.T) {
	f := newResultFuture[int]()
	errExpected := errors.New("test error")
	go func() {
		f.completeValue(0, errExpected)
	}()

	_, err := f.Await(context.Background())
	if !errors.Is(err, errExpected) {
		t.Errorf("got error %v, want %v", err, errExpected)
	}
}

func TestFutureAwaitContextCancelled(t *testing.T) {
	f := newResultFuture[int]()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := f.Await(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("got error %v, want %v", err, context.Canceled)
	}
}

func TestFutureDoneClosesOnComplete(t *testing.T) {
	f := newResultFuture[int]()
	done := f.Done()

	select {
	case <-done:
		t.Fatal("Done() closed before complete")
	default:
	}

	f.completeValue(42, nil)

	select {
	case <-done:
		// success
	default:
		t.Fatal("Done() not closed after complete")
	}
}

func TestFutureMultipleAwaitReturnsSameResult(t *testing.T) {
	f := newResultFuture[int]()
	f.completeValue(42, nil)

	for i := 0; i < 3; i++ {
		val, err := f.Await(context.Background())
		if err != nil || val != 42 {
			t.Errorf("iteration %d: val=%d, err=%v", i, val, err)
		}
	}
}

func TestFutureAwaitAfterCompletion(t *testing.T) {
	f := newResultFuture[int]()
	f.completeValue(42, nil)

	val, err := f.Await(context.Background())
	if err != nil || val != 42 {
		t.Errorf("Await after completion failed: val=%d, err=%v", val, err)
	}
}

func TestResultFutureCompleteOnlyOnce(t *testing.T) {
	f := newResultFuture[int]()
	f.completeValue(42, nil)
	f.completeValue(99, errors.New("ignored"))

	val, err := f.Await(context.Background())
	if err != nil || val != 42 {
		t.Errorf("first completion did not win: val=%d, err=%v", val, err)
	}
}

func TestResultFutureConcurrentAwait(t *testing.T) {
	f := newResultFuture[int]()
	const count = 10
	var wg sync.WaitGroup
	wg.Add(count)

	var startWg sync.WaitGroup
	startWg.Add(count)

	for i := 0; i < count; i++ {
		go func() {
			startWg.Done()
			defer wg.Done()
			val, err := f.Await(context.Background())
			if err != nil || val != 42 {
				t.Errorf("concurrent await got (%d, %v), want (42, nil)", val, err)
			}
		}()
	}

	startWg.Wait()
	f.completeValue(42, nil)
	wg.Wait()
}

func TestResultFutureConcurrentDoneSelect(t *testing.T) {
	f := newResultFuture[int]()
	const count = 10
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			select {
			case <-f.Done():
				val, _ := f.Await(context.Background())
				if val != 42 {
					t.Errorf("got %d, want 42", val)
				}
			case <-time.After(100 * time.Millisecond):
				t.Error("timed out waiting for Done")
			}
		}()
	}

	f.completeValue(42, nil)
	wg.Wait()
}

func TestResultFutureFirstCompletionWins(t *testing.T) {
	f := newResultFuture[int]()
	f.completeValue(1, nil)
	f.completeValue(2, errors.New("second"))

	val, err := f.Await(context.Background())
	if val != 1 || err != nil {
		t.Errorf("got (%d, %v), want (1, nil)", val, err)
	}
}

func TestResultFutureConcurrentComplete(t *testing.T) {
	f := newResultFuture[int]()
	const count = 10
	var wg sync.WaitGroup
	wg.Add(count)

	results := make([]int, count)
	for i := 0; i < count; i++ {
		val := i
		go func() {
			defer wg.Done()
			f.completeValue(val, nil)
			res, _ := f.Await(context.Background())
			results[val] = res
		}()
	}
	wg.Wait()

	// All should have the same result (the winner)
	winner := results[0]
	for i, res := range results {
		if res != winner {
			t.Errorf("result[%d] = %d, want %d", i, res, winner)
		}
	}
}

func TestResultFutureDoneReturnsSameChannel(t *testing.T) {
	f := newResultFuture[int]()
	d1 := f.Done()
	d2 := f.Done()
	if d1 != d2 {
		t.Error("Done() returned different channels")
	}
}

func TestAwaitTimeoutDoesNotCompleteFuture(t *testing.T) {
	f := newResultFuture[int]()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := f.Await(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want %v", err, context.DeadlineExceeded)
	}

	select {
	case <-f.Done():
		t.Error("Done() should NOT be closed after Await timeout")
	default:
		// success
	}
}

func TestAwaitContextTimeout(t *testing.T) {
	f := newResultFuture[int]()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := f.Await(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want %v", err, context.DeadlineExceeded)
	}
}

func TestAwaitCanSucceedAfterEarlierTimeout(t *testing.T) {
	f := newResultFuture[int]()
	ctx1, cancel1 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel1()

	_, err := f.Await(ctx1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}

	// Now complete the future
	f.completeValue(42, nil)

	// Await with fresh background context should succeed
	val, err := f.Await(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 42 {
		t.Errorf("got val %d, want 42", val)
	}
}

func TestAwaitTimeoutNoPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic caught: %v", r)
		}
	}()

	f := newResultFuture[int]()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	_, _ = f.Await(ctx)
}
