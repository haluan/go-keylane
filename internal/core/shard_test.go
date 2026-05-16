package core

import "testing"

func TestNewShardCreatesLaneQueues(t *testing.T) {
	laneCount := 3
	queueSize := 10
	s := newShard(laneCount, queueSize)

	if len(s.lanes) != laneCount {
		t.Errorf("len(s.lanes) = %d, want %d", len(s.lanes), laneCount)
	}
	for i := 0; i < laneCount; i++ {
		if s.lanes[i].capacity() != queueSize {
			t.Errorf("lane %d capacity = %d, want %d", i, s.lanes[i].capacity(), queueSize)
		}
	}
}

func TestNewShardStartsNotReady(t *testing.T) {
	s := newShard(3, 10)
	if s.ready {
		t.Error("new shard should not be ready")
	}
}

func TestShardTotalDepthLocked(t *testing.T) {
	s := newShard(3, 10)
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.totalDepthLocked() != 0 {
		t.Errorf("initial total depth = %d, want 0", s.totalDepthLocked())
	}

	_ = s.lanes[0].push(InternalJob{})
	_ = s.lanes[1].push(InternalJob{})
	_ = s.lanes[1].push(InternalJob{})

	if s.totalDepthLocked() != 3 {
		t.Errorf("total depth = %d, want 3", s.totalDepthLocked())
	}
}

func TestShardHasWorkLocked(t *testing.T) {
	s := newShard(3, 10)
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.hasWorkLocked() {
		t.Error("shard should not have work initially")
	}

	_ = s.lanes[2].push(InternalJob{})

	if !s.hasWorkLocked() {
		t.Error("shard should have work after push")
	}
}

func TestShardLaneDepthLocked(t *testing.T) {
	s := newShard(3, 10)
	s.mu.Lock()
	defer s.mu.Unlock()

	_ = s.lanes[1].push(InternalJob{})

	if s.laneDepthLocked(0) != 0 {
		t.Errorf("lane 0 depth = %d, want 0", s.laneDepthLocked(0))
	}
	if s.laneDepthLocked(1) != 1 {
		t.Errorf("lane 1 depth = %d, want 1", s.laneDepthLocked(1))
	}
	if s.laneDepthLocked(999) != 0 {
		t.Errorf("invalid lane depth = %d, want 0", s.laneDepthLocked(999))
	}
}
