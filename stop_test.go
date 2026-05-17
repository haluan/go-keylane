package keylane_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/haluan/go-keylane"
)

func TestStopIdleQueue(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = q.Start(ctx)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stopCancel()

	if err := q.Stop(stopCtx); err != nil {
		t.Errorf("expected no error stopping idle queue, got %v", err)
	}
}

func TestStopPreventsNewSubmit(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = q.Start(ctx)
	_ = q.Stop(context.Background())

	job := keylane.Job{
		Key:  "key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	}

	err := q.Submit(context.Background(), job)
	if !errors.Is(err, keylane.ErrStopped) {
		t.Errorf("expected ErrStopped, got %v", err)
	}
}

func TestStopPreventsNewTrySubmit(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = q.Start(ctx)
	_ = q.Stop(context.Background())

	job := keylane.Job{
		Key:  "key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	}

	if ok := q.TrySubmit(job); ok {
		t.Errorf("expected TrySubmit to return false after Stop")
	}
}

func TestStopIdempotent(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = q.Start(ctx)

	if err := q.Stop(context.Background()); err != nil {
		t.Errorf("first stop failed: %v", err)
	}

	if err := q.Stop(context.Background()); err != nil {
		t.Errorf("second stop failed: %v", err)
	}
}

func TestStopWhileJobRunning(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = q.Start(ctx)

	jobStarted := make(chan struct{})
	jobBlock := make(chan struct{})
	jobFinished := make(chan struct{})

	job := keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			close(jobStarted)
			<-jobBlock
			close(jobFinished)
			return nil
		},
	}

	_ = q.Submit(context.Background(), job)

	<-jobStarted

	// Call Stop concurrently while job is running. It should block waiting for the job to complete if drain is true.
	stopDone := make(chan struct{})
	go func() {
		_ = q.Stop(context.Background(), keylane.WithDrain(true))
		close(stopDone)
	}()

	// Ensure Stop does not return immediately while job is still blocked
	select {
	case <-stopDone:
		t.Fatal("Stop returned while job was still running")
	case <-time.After(50 * time.Millisecond):
		// OK
	}

	// Unblock job
	close(jobBlock)

	// Now stop should complete
	select {
	case <-stopDone:
		// success
	case <-time.After(time.Second):
		t.Fatal("Stop timed out waiting for job completion")
	}

	<-jobFinished
}

func TestStopWithoutDrainDoesNotWaitForQueuedJobs(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = q.Start(ctx)

	blockChan := make(chan struct{})
	defer close(blockChan)

	// Block the worker
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			select {
			case <-blockChan:
			case <-ctx.Done():
			}
			return nil
		},
	})

	time.Sleep(10 * time.Millisecond) // let first job start

	job2Run := false
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			job2Run = true
			return nil
		},
	})

	// Stop without drain. It should exit quickly and NOT execute job2.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stopCancel()

	if err := q.Stop(stopCtx, keylane.WithDrain(false)); err != nil {
		t.Errorf("expected Stop to complete successfully, got %v", err)
	}

	if job2Run {
		t.Errorf("expected job2 NOT to run when drain=false")
	}
}

func TestStopReturnsContextErrorOnTimeout(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = q.Start(ctx)

	blockChan := make(chan struct{})
	defer close(blockChan)

	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			select {
			case <-blockChan:
			case <-ctx.Done():
			}
			return nil
		},
	})

	time.Sleep(10 * time.Millisecond)

	// Stop with short timeout. Since worker is blocked, it should timeout and return context.DeadlineExceeded.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stopCancel()

	err := q.Stop(stopCtx, keylane.WithDrain(true))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestStopConcurrentCalls(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = q.Start(ctx)

	// Block the worker
	blockChan := make(chan struct{})
	defer close(blockChan)

	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			<-blockChan
			return nil
		},
	})

	time.Sleep(10 * time.Millisecond)

	errChan := make(chan error, 3)

	// Launch multiple concurrent Stop calls
	for i := 0; i < 3; i++ {
		go func() {
			errChan <- q.Stop(context.Background(), keylane.WithDrain(true))
		}()
	}

	time.Sleep(20 * time.Millisecond)

	// Unblock the worker so all concurrent Stop calls can proceed to successful completion
	blockChan <- struct{}{}

	// Verify all concurrent Stop calls return nil without timing out or returning error
	for i := 0; i < 3; i++ {
		select {
		case err := <-errChan:
			if err != nil {
				t.Errorf("expected nil from concurrent Stop, got %v", err)
			}
		case <-time.After(200 * time.Millisecond):
			t.Fatal("timed out waiting for concurrent Stop calls to complete")
		}
	}
}

func TestStopWithDrainProcessesQueuedJobs(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = q.Start(ctx)

	blockChan := make(chan struct{})
	defer close(blockChan)

	// Block the worker with first job
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			<-blockChan
			return nil
		},
	})

	time.Sleep(10 * time.Millisecond)

	// Submit second job (will sit in the queue)
	job2Run := false
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			job2Run = true
			return nil
		},
	})

	stopDone := make(chan struct{})
	go func() {
		_ = q.Stop(context.Background(), keylane.WithDrain(true))
		close(stopDone)
	}()

	time.Sleep(10 * time.Millisecond)

	// Stop should still be blocking waiting for the queue to drain
	select {
	case <-stopDone:
		t.Fatal("Stop returned early before drain was complete")
	default:
	}

	// Unblock the worker
	blockChan <- struct{}{}

	// Stop should now complete successfully
	select {
	case <-stopDone:
		// success
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Stop timed out")
	}

	if !job2Run {
		t.Error("expected job2 to have run and completed")
	}
}

func TestDrainCompletesAfterJobError(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = q.Start(ctx)

	// Submit a job that returns an error
	errExpected := errors.New("expected failure")
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			return errExpected
		},
	})

	time.Sleep(10 * time.Millisecond)

	// Stop with drain. It should complete quickly and successfully, not blocking or deadlocking.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer stopCancel()

	if err := q.Stop(stopCtx, keylane.WithDrain(true)); err != nil {
		t.Errorf("expected Stop to succeed even after job error, got %v", err)
	}
}

func TestLifecycleSubmitRejectedWhileStopping(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = q.Start(ctx)

	blockChan := make(chan struct{})
	defer close(blockChan)

	// Block the worker with first job
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			<-blockChan
			return nil
		},
	})

	time.Sleep(10 * time.Millisecond)

	stopDone := make(chan struct{})
	go func() {
		_ = q.Stop(context.Background(), keylane.WithDrain(true))
		close(stopDone)
	}()

	time.Sleep(10 * time.Millisecond)

	// Now the queue is stopping. New Submit calls must be rejected immediately with ErrStopped.
	err := q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	})
	if !errors.Is(err, keylane.ErrStopped) {
		t.Errorf("expected ErrStopped while queue is stopping, got %v", err)
	}

	// Unblock the worker so stop completes
	blockChan <- struct{}{}
	<-stopDone
}

func TestLifecycleTrySubmitRejectedWhileStopping(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = q.Start(ctx)

	blockChan := make(chan struct{})
	defer close(blockChan)

	// Block the worker with first job
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			<-blockChan
			return nil
		},
	})

	time.Sleep(10 * time.Millisecond)

	stopDone := make(chan struct{})
	go func() {
		_ = q.Stop(context.Background(), keylane.WithDrain(true))
		close(stopDone)
	}()

	time.Sleep(10 * time.Millisecond)

	// Now the queue is stopping. New TrySubmit calls must be rejected immediately (return false).
	if ok := q.TrySubmit(keylane.Job{
		Key:  "key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	}); ok {
		t.Error("expected TrySubmit to return false while queue is stopping")
	}

	// Unblock the worker so stop completes
	blockChan <- struct{}{}
	<-stopDone
}
