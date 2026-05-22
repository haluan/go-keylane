// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"errors"
	"testing"
)

func TestEnqueueIntoShardAddsJobToCorrectLane(t *testing.T) {
	s := newShard(3, 10)
	job := InternalJob{LaneID: 1, KeyHash: 123}

	_, err := enqueueIntoShard(&s, job, false)
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	if s.Lanes[1].depth() != 1 {
		t.Errorf("lane 1 depth = %d, want 1", s.Lanes[1].depth())
	}
}

func TestEnqueueIntoShardMarksShardReady(t *testing.T) {
	s := newShard(3, 10)
	job := InternalJob{LaneID: 1}

	becameReady, err := enqueueIntoShard(&s, job, false)
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	if !becameReady {
		t.Error("becameReady should be true for first enqueue")
	}
	if !s.Ready {
		t.Error("shard should be marked Ready")
	}
}

func TestEnqueueIntoShardAlreadyReady(t *testing.T) {
	s := newShard(3, 10)
	s.Ready = true

	becameReady, err := enqueueIntoShard(&s, InternalJob{LaneID: 1}, false)
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	if becameReady {
		t.Error("becameReady should be false if shard was already Ready")
	}
}

func TestEnqueueIntoShardQueueFull(t *testing.T) {
	s := newShard(1, 1)
	_ = s.Lanes[0].push(InternalJob{})

	_, err := enqueueIntoShard(&s, InternalJob{LaneID: 0}, false)
	if !errors.Is(err, errLaneQueueFull) {
		t.Errorf("expected errLaneQueueFull, got %v", err)
	}
}

func TestEnqueueIntoShardInvalidLaneID(t *testing.T) {
	s := newShard(1, 10)
	_, err := enqueueIntoShard(&s, InternalJob{LaneID: 99}, false)
	if !errors.Is(err, errInvalidLaneID) {
		t.Errorf("expected errInvalidLaneID, got %v", err)
	}
}

func TestEnqueueIntoShardSetsAcceptedAtAfterSuccessfulPush(t *testing.T) {
	s := newShard(1, 10)
	job := InternalJob{LaneID: 0, KeyHash: 1}

	_, err := enqueueIntoShard(&s, job, false)
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	queued, ok := s.Lanes[0].pop()
	if !ok {
		t.Fatal("expected queued job")
	}
	if queued.AcceptedAt.IsZero() {
		t.Error("AcceptedAt should be set after successful admission")
	}
}

func TestEnqueueIntoShardFailedPushLeavesAcceptedAtZero(t *testing.T) {
	s := newShard(1, 1)
	_ = s.Lanes[0].push(InternalJob{})

	_, err := enqueueIntoShard(&s, InternalJob{LaneID: 0}, false)
	if !errors.Is(err, errLaneQueueFull) {
		t.Fatalf("expected errLaneQueueFull, got %v", err)
	}

	queued, ok := s.Lanes[0].pop()
	if !ok {
		t.Fatal("expected only the first queued job")
	}
	if !queued.AcceptedAt.IsZero() {
		t.Error("pre-existing job should not have AcceptedAt stamped by failed enqueue")
	}
}

func TestEnqueueIntoShardSetsEnqueuedAtWhenTrackQueueWait(t *testing.T) {
	s := newShard(1, 10)
	job := InternalJob{LaneID: 0}

	_, err := enqueueIntoShard(&s, job, true)
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	queued, ok := s.Lanes[0].pop()
	if !ok {
		t.Fatal("expected queued job")
	}
	if queued.EnqueuedAt.IsZero() {
		t.Error("EnqueuedAt should be set when trackQueueWait is enabled")
	}
	if queued.AcceptedAt.IsZero() {
		t.Error("AcceptedAt should be set alongside EnqueuedAt")
	}
	if queued.EnqueuedAt != queued.AcceptedAt {
		t.Errorf("EnqueuedAt and AcceptedAt should match on admission, got %v vs %v",
			queued.EnqueuedAt, queued.AcceptedAt)
	}
}

func TestEnqueueIntoShardFailedPushDoesNotMarkReady(t *testing.T) {
	s := newShard(1, 1)
	_ = s.Lanes[0].push(InternalJob{}) // fill it

	// This enqueue will fail because queue is full
	becameReady, _ := enqueueIntoShard(&s, InternalJob{LaneID: 0}, false)
	if becameReady {
		t.Error("becameReady should be false on failed push")
	}

	// Reset shard state to check if Ready was set even on error
	s2 := newShard(1, 1)
	_ = s2.Lanes[0].push(InternalJob{})
	_, _ = enqueueIntoShard(&s2, InternalJob{LaneID: 0}, false)
	if s2.Ready {
		t.Error("shard should not be Ready if push failed")
	}
}
