// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestRaceConcurrentSubmit(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	const count = 50
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%d", i)
		go func() {
			defer wg.Done()
			_ = q.Submit(ctx, Job{
				Key:  key,
				Lane: "default",
				Run:  func(ctx context.Context) error { return nil },
			})
		}()
	}

	wg.Wait()
}

func TestRaceConcurrentTrySubmit(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	const count = 50
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%d", i)
		go func() {
			defer wg.Done()
			_ = q.TrySubmit(Job{
				Key:  key,
				Lane: "default",
				Run:  func(ctx context.Context) error { return nil },
			})
		}()
	}

	wg.Wait()
}

func TestRaceConcurrentSubmitValue(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	const count = 50
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%d", i)
		go func() {
			defer wg.Done()
			_, _ = SubmitValue(ctx, q, ValueJob[int]{
				Key:  key,
				Lane: "default",
				Run:  func(ctx context.Context) (int, error) { return 42, nil },
			})
		}()
	}

	wg.Wait()
}

func TestRaceConcurrentAwait(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	future, _ := SubmitValue(ctx, q, ValueJob[int]{
		Key:  "k",
		Lane: "default",
		Run:  func(ctx context.Context) (int, error) { return 100, nil },
	})

	const count = 50
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			val, err := future.Await(ctx)
			if err != nil || val != 100 {
				t.Errorf("Await got (%d, %v), want (100, nil)", val, err)
			}
		}()
	}

	wg.Wait()
}

func TestRaceConcurrentStop(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	const count = 10
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			_ = q.Stop(ctx)
		}()
	}

	wg.Wait()
}

func TestRaceConcurrentStats(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	const count = 50
	var wg sync.WaitGroup
	wg.Add(count * 2)

	// One set of goroutines enqueuing, another reading Stats
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%d", i)
		go func() {
			defer wg.Done()
			_ = q.Submit(ctx, Job{
				Key:  key,
				Lane: "default",
				Run:  func(ctx context.Context) error { return nil },
			})
		}()

		go func() {
			defer wg.Done()
			_ = q.Stats()
		}()
	}

	wg.Wait()
}

func TestRaceConcurrentStatsGCPressure(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	const count = 50
	var wg sync.WaitGroup
	wg.Add(count * 2)

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%d", i)
		go func() {
			defer wg.Done()
			_ = q.Submit(ctx, Job{
				Key:  key,
				Lane: "default",
				Run:  func(ctx context.Context) error { return nil },
			})
		}()

		go func() {
			defer wg.Done()
			_ = q.StatsGCPressure()
		}()
	}

	wg.Wait()
}
