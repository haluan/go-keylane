package core

import (
	"errors"
	"testing"
)

func TestEnqueueIntoShardAddsJobToCorrectLane(t *testing.T) {
	s := newShard(3, 10)
	job := InternalJob{LaneID: 1, KeyHash: 123}

	_, err := enqueueIntoShard(&s, job)
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	if s.lanes[1].depth() != 1 {
		t.Errorf("lane 1 depth = %d, want 1", s.lanes[1].depth())
	}
}

func TestEnqueueIntoShardMarksShardReady(t *testing.T) {
	s := newShard(3, 10)
	job := InternalJob{LaneID: 1}

	becameReady, err := enqueueIntoShard(&s, job)
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	if !becameReady {
		t.Error("becameReady should be true for first enqueue")
	}
	if !s.ready {
		t.Error("shard should be marked ready")
	}
}

func TestEnqueueIntoShardAlreadyReady(t *testing.T) {
	s := newShard(3, 10)
	s.ready = true

	becameReady, err := enqueueIntoShard(&s, InternalJob{LaneID: 1})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	if becameReady {
		t.Error("becameReady should be false if shard was already ready")
	}
}

func TestEnqueueIntoShardQueueFull(t *testing.T) {
	s := newShard(1, 1)
	_ = s.lanes[0].push(InternalJob{})

	_, err := enqueueIntoShard(&s, InternalJob{LaneID: 0})
	if !errors.Is(err, errLaneQueueFull) {
		t.Errorf("expected errLaneQueueFull, got %v", err)
	}
}

func TestEnqueueIntoShardInvalidLaneID(t *testing.T) {
	s := newShard(1, 10)
	_, err := enqueueIntoShard(&s, InternalJob{LaneID: 99})
	if !errors.Is(err, errInvalidLaneID) {
		t.Errorf("expected errInvalidLaneID, got %v", err)
	}
}

func TestEnqueueIntoShardFailedPushDoesNotMarkReady(t *testing.T) {
	s := newShard(1, 1)
	_ = s.lanes[0].push(InternalJob{}) // fill it

	// This enqueue will fail because queue is full
	becameReady, _ := enqueueIntoShard(&s, InternalJob{LaneID: 0})
	if becameReady {
		t.Error("becameReady should be false on failed push")
	}

	// Reset shard state to check if ready was set even on error
	s2 := newShard(1, 1)
	_ = s2.lanes[0].push(InternalJob{})
	_, _ = enqueueIntoShard(&s2, InternalJob{LaneID: 0})
	if s2.ready {
		t.Error("shard should not be ready if push failed")
	}
}
