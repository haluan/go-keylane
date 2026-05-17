package core

import (
	"context"
	"testing"
)

func TestDrainDetection(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, err := NewScheduler(1, 1, 10, reg)
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}

	// 1. Initial state (empty) -> should be drained
	if !s.isDrained() {
		t.Errorf("expected initially drained")
	}

	// 2. Job in flight -> not drained
	s.inflight.Store(1)
	if s.isDrained() {
		t.Errorf("expected not drained when jobs in flight")
	}
	s.inflight.Store(0)

	// 3. Job enqueued -> not drained
	job, _ := NewInternalJob(func(ctx context.Context) error { return nil }, 0, 0)
	_, err = enqueueIntoShard(&s.shards[0], job)
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	if s.isDrained() {
		t.Errorf("expected not drained when jobs are enqueued")
	}
}
