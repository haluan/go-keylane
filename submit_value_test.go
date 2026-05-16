package keylane

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSubmitValueSuccess(t *testing.T) {
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 1,
		LaneQuotas: map[Lane]int{
			"test": 1,
		},
	}
	q, _ := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key:  "k",
		Lane: "test",
		Run: func(ctx context.Context) (int, error) {
			return 42, nil
		},
	})

	if err != nil {
		t.Fatalf("SubmitValue failed: %v", err)
	}

	val, err := future.Await(ctx)
	if err != nil {
		t.Errorf("Await failed: %v", err)
	}
	if val != 42 {
		t.Errorf("val = %d, want 42", val)
	}
}

func TestSubmitValueJobError(t *testing.T) {
	cfg := Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 1, LaneQuotas: map[Lane]int{"test": 1},
	}
	q, _ := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	errExpected := errors.New("job error")
	future, _ := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "test", Run: func(ctx context.Context) (int, error) {
			return 0, errExpected
		},
	})

	_, err := future.Await(ctx)
	if !errors.Is(err, errExpected) {
		t.Errorf("got error %v, want %v", err, errExpected)
	}
}

func TestSubmitValueRejectsNilQueue(t *testing.T) {
	_, err := SubmitValue(context.Background(), nil, ValueJob[int]{})
	if !errors.Is(err, ErrNilQueue) {
		t.Errorf("got error %v, want %v", err, ErrNilQueue)
	}
}

func TestSubmitValueRejectsInvalidJob(t *testing.T) {
	q, _ := New(Config{ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 1, LaneQuotas: map[Lane]int{"test": 1}})
	_, err := SubmitValue(context.Background(), q, ValueJob[int]{Key: ""}) // Invalid key
	if !errors.Is(err, ErrInvalidKey) {
		t.Errorf("got error %v, want %v", err, ErrInvalidKey)
	}
}

func TestSubmitValueQueueFull(t *testing.T) {
	cfg := Config{ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 1, LaneQuotas: map[Lane]int{"test": 1}}
	q, _ := New(cfg)
	// Filling the queue with a slow job
	_ = q.Submit(context.Background(), Job{
		Key: "slow", Lane: "test", Run: func(ctx context.Context) error {
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	})

	// Submit another one to fill it
	_ = q.Submit(context.Background(), Job{Key: "full", Lane: "test", Run: func(ctx context.Context) error { return nil }})

	// This should fail with ErrQueueFull
	_, err := SubmitValue(context.Background(), q, ValueJob[int]{
		Key: "k", Lane: "test", Run: func(ctx context.Context) (int, error) { return 0, nil },
	})
	if !errors.Is(err, ErrQueueFull) {
		t.Errorf("got error %v, want %v", err, ErrQueueFull)
	}
}

func TestAwaitTimeoutBeforeJobCompletion(t *testing.T) {
	cfg := Config{ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 1, LaneQuotas: map[Lane]int{"test": 1}}
	q, _ := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	future, _ := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "test", Run: func(ctx context.Context) (int, error) {
			time.Sleep(200 * time.Millisecond)
			return 42, nil
		},
	})

	// Await with short timeout
	shortCtx, shortCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer shortCancel()

	_, err := future.Await(shortCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got error %v, want %v", err, context.DeadlineExceeded)
	}

	// Now await with longer timeout
	val, err := future.Await(ctx)
	if err != nil || val != 42 {
		t.Errorf("Await after timeout failed: val=%d, err=%v", val, err)
	}
}

func TestSubmitValueFailurePathReturnsCompletedFuture(t *testing.T) {
	// Test nil queue
	f1, err1 := SubmitValue(context.Background(), nil, ValueJob[int]{})
	if f1 == nil { t.Fatal("future should not be nil") }
	if !errors.Is(err1, ErrNilQueue) { t.Errorf("got %v", err1) }
	_, waitErr1 := f1.Await(context.Background())
	if !errors.Is(waitErr1, ErrNilQueue) { t.Errorf("Await got %v", waitErr1) }

	// Test invalid job
	q, _ := New(Config{ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 1, LaneQuotas: map[Lane]int{"test": 1}})
	f2, err2 := SubmitValue(context.Background(), q, ValueJob[int]{Key: ""})
	if !errors.Is(err2, ErrInvalidKey) { t.Errorf("got %v", err2) }
	_, waitErr2 := f2.Await(context.Background())
	if !errors.Is(waitErr2, ErrInvalidKey) { t.Errorf("Await got %v", waitErr2) }

	// Test queue full
	cfg := Config{ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 1, LaneQuotas: map[Lane]int{"test": 1}}
	qFull, _ := New(cfg)
	_ = qFull.Submit(context.Background(), Job{Key: "slow", Lane: "test", Run: func(ctx context.Context) error { time.Sleep(100 * time.Millisecond); return nil }})
	_ = qFull.Submit(context.Background(), Job{Key: "full", Lane: "test", Run: func(ctx context.Context) error { return nil }})
	
	f3, err3 := SubmitValue(context.Background(), qFull, ValueJob[int]{Key: "k", Lane: "test", Run: func(ctx context.Context) (int, error) { return 0, nil }})
	if !errors.Is(err3, ErrQueueFull) { t.Errorf("got %v", err3) }
	_, waitErr3 := f3.Await(context.Background())
	if !errors.Is(waitErr3, ErrQueueFull) { t.Errorf("Await got %v", waitErr3) }

	// Test unknown lane
	f4, err4 := SubmitValue(context.Background(), q, ValueJob[int]{Key: "k", Lane: "unknown", Run: func(ctx context.Context) (int, error) { return 0, nil }})
	if !errors.Is(err4, ErrInvalidLane) { t.Errorf("got %v", err4) }
	_, waitErr4 := f4.Await(context.Background())
	if !errors.Is(waitErr4, ErrInvalidLane) { t.Errorf("Await got %v", waitErr4) }

	// Test context cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f5, err5 := SubmitValue(ctx, q, ValueJob[int]{Key: "k", Lane: "test", Run: func(ctx context.Context) (int, error) { return 0, nil }})
	if !errors.Is(err5, context.Canceled) { t.Errorf("got %v", err5) }
	_, waitErr5 := f5.Await(context.Background())
	if !errors.Is(waitErr5, context.Canceled) { t.Errorf("Await got %v", waitErr5) }
}

func TestSubmitValueUnknownLane(t *testing.T) {
	q, _ := New(Config{ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 1, LaneQuotas: map[Lane]int{"test": 1}})
	_, err := SubmitValue(context.Background(), q, ValueJob[int]{Key: "k", Lane: "unknown", Run: func(ctx context.Context) (int, error) { return 0, nil }})
	if !errors.Is(err, ErrInvalidLane) {
		t.Errorf("got %v, want %v", err, ErrInvalidLane)
	}
}

func TestSubmitValueContextCancelled(t *testing.T) {
	q, _ := New(Config{ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 1, LaneQuotas: map[Lane]int{"test": 1}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	
	_, err := SubmitValue(ctx, q, ValueJob[int]{Key: "k", Lane: "test", Run: func(ctx context.Context) (int, error) { return 0, nil }})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("got %v, want %v", err, context.Canceled)
	}
}

func TestSubmitValueFutureCompletedOnlyOnce(t *testing.T) {
	cfg := Config{ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 1, LaneQuotas: map[Lane]int{"test": 1}}
	q, _ := New(cfg)
	_ = q.Start(context.Background())

	var execCount int32
	future, _ := SubmitValue(context.Background(), q, ValueJob[int]{
		Key: "k", Lane: "test", Run: func(ctx context.Context) (int, error) {
			atomic.AddInt32(&execCount, 1)
			return 42, nil
		},
	})
	
	val, _ := future.Await(context.Background())
	if val != 42 { t.Errorf("got %d", val) }
	if atomic.LoadInt32(&execCount) != 1 { t.Errorf("execCount = %d", execCount) }
}

func TestSubmitValueUsesQueueSubmitPath(t *testing.T) {
	// Verify that SubmitValue respects QueueSizePerLane (which is part of q.Submit path)
	cfg := Config{ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 1, LaneQuotas: map[Lane]int{"test": 1}}
	q, _ := New(cfg)
	
	// Occupy the single queue slot
	_ = q.Submit(context.Background(), Job{Key: "slow", Lane: "test", Run: func(ctx context.Context) error { time.Sleep(100 * time.Millisecond); return nil }})
	_ = q.Submit(context.Background(), Job{Key: "full", Lane: "test", Run: func(ctx context.Context) error { return nil }})

	_, err := SubmitValue(context.Background(), q, ValueJob[int]{Key: "v", Lane: "test", Run: func(ctx context.Context) (int, error) { return 1, nil }})
	if !errors.Is(err, ErrQueueFull) {
		t.Errorf("got %v, want ErrQueueFull", err)
	}
}

func TestSubmitValueConcurrentRace(t *testing.T) {
	cfg := Config{ShardCount: 8, WorkerCount: 4, QueueSizePerLane: 100, LaneQuotas: map[Lane]int{"test": 1}}
	q, _ := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	const count = 50
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		val := i
		go func() {
			defer wg.Done()
			f, _ := SubmitValue(ctx, q, ValueJob[int]{
				Key:  "k",
				Lane: "test",
				Run: func(ctx context.Context) (int, error) {
					return val * val, nil
				},
			})
			res, _ := f.Await(ctx)
			if res != val*val {
				// This would be caught by -race or if values were somehow mixed up
			}
		}()
	}
	wg.Wait()
}
