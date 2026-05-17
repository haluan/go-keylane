package core

import (
	"context"
	"testing"
)

func TestLifecycleInitialState(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, err := NewScheduler(1, 1, 1, reg)
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}

	if s.state != stateNew {
		t.Errorf("expected initial state to be stateNew, got %d", s.state)
	}
}

func TestLifecycleStartTransitionsToRunning(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, err := NewScheduler(1, 1, 1, reg)
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	if s.state != stateRunning {
		t.Errorf("expected state stateRunning, got %d", s.state)
	}
}

func TestLifecycleStartTwice(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, err := NewScheduler(1, 1, 1, reg)
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	if err := s.Start(ctx); err != ErrQueueAlreadyStarted {
		t.Errorf("expected ErrQueueAlreadyStarted, got %v", err)
	}
}

func TestLifecycleStopTransitionsToStopped(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, err := NewScheduler(1, 1, 1, reg)
	if err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = s.Start(ctx)

	if err := s.Stop(context.Background(), true); err != nil {
		t.Fatalf("failed to stop: %v", err)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.state != stateStopped {
		t.Errorf("expected state stateStopped, got %d", s.state)
	}
}
