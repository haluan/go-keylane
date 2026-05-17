package core

import (
	"context"
	"testing"
	"time"
)

func TestInflightCounterReturnsToZero(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, err := NewScheduler(1, 1, 10, reg)
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	if s.Inflight() != 0 {
		t.Errorf("expected initially 0 inflight, got %d", s.Inflight())
	}

	done := make(chan struct{})
	job, _ := NewInternalJob(func(ctx context.Context) error {
		close(done)
		return nil
	}, 0, 0)

	_, becameReady, _ := s.Enqueue(job)
	if becameReady {
		s.ReadyCh <- 0
	}

	select {
	case <-done:
		// wait slightly to ensure inflight was decremented
		time.Sleep(10 * time.Millisecond)
		if s.Inflight() != 0 {
			t.Errorf("expected inflight to return to 0, got %d", s.Inflight())
		}
	case <-time.After(time.Second):
		t.Fatal("job did not execute")
	}
}

func TestInflightIncrementsDuringExecution(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, err := NewScheduler(1, 1, 10, reg)
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	jobStart := make(chan struct{})
	jobBlock := make(chan struct{})
	defer close(jobBlock)

	job, _ := NewInternalJob(func(ctx context.Context) error {
		close(jobStart)
		<-jobBlock
		return nil
	}, 0, 0)

	_, becameReady, _ := s.Enqueue(job)
	if becameReady {
		s.ReadyCh <- 0
	}

	select {
	case <-jobStart:
		// job is running, inflight must be 1
		if s.Inflight() != 1 {
			t.Errorf("expected inflight to be 1 during execution, got %d", s.Inflight())
		}
	case <-time.After(time.Second):
		t.Fatal("job did not start")
	}
}
