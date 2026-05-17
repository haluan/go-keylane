package core

import (
	"context"
	"testing"
	"time"
)

func TestStopAllWorkersExit(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, err := NewScheduler(2, 4, 10, reg) // 4 workers
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("failed to start scheduler: %v", err)
	}

	// Stop the scheduler
	if err := s.Stop(context.Background(), true); err != nil {
		t.Fatalf("failed to stop scheduler: %v", err)
	}

	// The Stop method itself waits on s.workerWG.Wait(), but let's double check using a channel with a timeout
	done := make(chan struct{})
	go func() {
		s.workerWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		// workers exited successfully
	case <-time.After(500 * time.Millisecond):
		t.Error("workers did not exit in a timely manner (goroutine leak)")
	}
}

func TestStopWorkersDoNotBlockOnReadyChannel(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, err := NewScheduler(2, 2, 10, reg)
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("failed to start scheduler: %v", err)
	}

	// Enqueue a job to trigger ready notification
	job, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 0)
	_, becameReady, _ := s.Enqueue(job)
	if becameReady {
		// Suppress duplicate ready send or send non-blocking
		select {
		case s.ReadyCh <- 0:
		default:
		}
	}

	// Stop without drain
	if err := s.Stop(context.Background(), false); err != nil {
		t.Fatalf("failed to stop scheduler: %v", err)
	}

	// Verify all workers exited
	done := make(chan struct{})
	go func() {
		s.workerWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(500 * time.Millisecond):
		t.Error("workers blocked on ready channel send/receive")
	}
}

func TestStopWorkersDoNotBlockOnDrain(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, err := NewScheduler(2, 2, 10, reg)
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	// Stop with drain
	if err := s.Stop(context.Background(), true); err != nil {
		t.Fatalf("failed to stop: %v", err)
	}

	// Verify all workers exited
	done := make(chan struct{})
	go func() {
		s.workerWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(500 * time.Millisecond):
		t.Error("workers blocked during drain stop")
	}
}
