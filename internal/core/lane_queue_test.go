package core

import (
	"errors"
	"testing"
)

func TestLaneQueuePushPopFIFO(t *testing.T) {
	q := newLaneQueue(3)
	j1 := InternalJob{KeyHash: 1}
	j2 := InternalJob{KeyHash: 2}

	_ = q.push(j1)
	_ = q.push(j2)

	r1, ok := q.pop()
	if !ok || r1.KeyHash != 1 {
		t.Errorf("pop 1 failed: got %v, ok %v", r1, ok)
	}

	r2, ok := q.pop()
	if !ok || r2.KeyHash != 2 {
		t.Errorf("pop 2 failed: got %v, ok %v", r2, ok)
	}
}

func TestLaneQueueFull(t *testing.T) {
	q := newLaneQueue(2)
	_ = q.push(InternalJob{KeyHash: 1})
	_ = q.push(InternalJob{KeyHash: 2})

	err := q.push(InternalJob{KeyHash: 3})
	if !errors.Is(err, errLaneQueueFull) {
		t.Errorf("expected errLaneQueueFull, got %v", err)
	}
	if !q.isFull() {
		t.Error("expected queue to be full")
	}
}

func TestLaneQueueEmptyPop(t *testing.T) {
	q := newLaneQueue(2)
	_, ok := q.pop()
	if ok {
		t.Error("pop on empty queue should return ok=false")
	}
	if !q.isEmpty() {
		t.Error("expected queue to be empty")
	}
}

func TestLaneQueueDepth(t *testing.T) {
	q := newLaneQueue(5)
	if q.depth() != 0 {
		t.Errorf("initial depth = %d, want 0", q.depth())
	}
	_ = q.push(InternalJob{})
	_ = q.push(InternalJob{})
	if q.depth() != 2 {
		t.Errorf("depth = %d, want 2", q.depth())
	}
	_, _ = q.pop()
	if q.depth() != 1 {
		t.Errorf("depth after pop = %d, want 1", q.depth())
	}
}

func TestLaneQueueWrapAround(t *testing.T) {
	q := newLaneQueue(3)
	// Fill it
	_ = q.push(InternalJob{KeyHash: 1})
	_ = q.push(InternalJob{KeyHash: 2})
	_ = q.push(InternalJob{KeyHash: 3})

	// Pop 2
	_, _ = q.pop()
	_, _ = q.pop()

	// Push 2 (wrap around)
	_ = q.push(InternalJob{KeyHash: 4})
	_ = q.push(InternalJob{KeyHash: 5})

	if q.depth() != 3 {
		t.Errorf("depth = %d, want 3", q.depth())
	}

	// Verify order
	r1, _ := q.pop() // should be 3
	r2, _ := q.pop() // should be 4
	r3, _ := q.pop() // should be 5

	if r1.KeyHash != 3 || r2.KeyHash != 4 || r3.KeyHash != 5 {
		t.Errorf("wrong order after wrap around: %d, %d, %d", r1.KeyHash, r2.KeyHash, r3.KeyHash)
	}
}

func TestLaneQueuePopNRespectsLimit(t *testing.T) {
	q := newLaneQueue(10)
	for i := 0; i < 5; i++ {
		_ = q.push(InternalJob{KeyHash: uint64(i)})
	}

	dst := make([]InternalJob, 0, 10)
	dst = q.popN(3, dst)

	if len(dst) != 3 {
		t.Errorf("popN(3) returned %d items, want 3", len(dst))
	}
	if q.depth() != 2 {
		t.Errorf("remaining depth = %d, want 2", q.depth())
	}
}

func TestLaneQueuePopNPreservesOrder(t *testing.T) {
	q := newLaneQueue(10)
	for i := 0; i < 5; i++ {
		_ = q.push(InternalJob{KeyHash: uint64(i)})
	}

	dst := make([]InternalJob, 0, 10)
	dst = q.popN(10, dst) // Try to pop more than available

	if len(dst) != 5 {
		t.Errorf("popN(10) on 5 items returned %d items, want 5", len(dst))
	}
	for i := 0; i < 5; i++ {
		if dst[i].KeyHash != uint64(i) {
			t.Errorf("item %d has KeyHash %d, want %d", i, dst[i].KeyHash, i)
		}
	}
}

func TestLaneQueueCapacityDoesNotGrow(t *testing.T) {
	q := newLaneQueue(10)
	initialCap := cap(q.items)

	for i := 0; i < 10; i++ {
		_ = q.push(InternalJob{KeyHash: uint64(i)})
	}

	if cap(q.items) != initialCap {
		t.Errorf("expected queue backing slice capacity to remain %d, got %d", initialCap, cap(q.items))
	}
}

func TestLaneQueuePushDoesNotGrowBackingSlice(t *testing.T) {
	q := newLaneQueue(5)
	ptrBefore := &q.items[0]

	for i := 0; i < 5; i++ {
		_ = q.push(InternalJob{KeyHash: uint64(i)})
	}

	ptrAfter := &q.items[0]
	if ptrBefore != ptrAfter {
		t.Error("backing slice array reference changed! Push re-allocated memory!")
	}
}

func TestLaneQueuePopDoesNotChangeCapacity(t *testing.T) {
	q := newLaneQueue(5)
	for i := 0; i < 5; i++ {
		_ = q.push(InternalJob{KeyHash: uint64(i)})
	}

	initialCap := cap(q.items)
	for i := 0; i < 5; i++ {
		_, _ = q.pop()
	}

	if cap(q.items) != initialCap {
		t.Errorf("expected backing slice capacity to remain %d, got %d", initialCap, cap(q.items))
	}
}

func TestLaneQueuePopNReusesDestination(t *testing.T) {
	q := newLaneQueue(10)
	for i := 0; i < 5; i++ {
		_ = q.push(InternalJob{KeyHash: uint64(i)})
	}

	dst := make([]InternalJob, 0, 10)
	ptrBefore := &dst[:10][0]

	dst = q.popN(5, dst)
	ptrAfter := &dst[:10][0]

	if ptrBefore != ptrAfter {
		t.Error("popN grew the slice or re-allocated! Caller-provided destination slice was not reused.")
	}
}

func TestLaneQueueWrapAroundCapacityStable(t *testing.T) {
	q := newLaneQueue(3)
	initialCap := cap(q.items)

	for i := 0; i < 100; i++ {
		_ = q.push(InternalJob{KeyHash: uint64(i)})
		_, _ = q.pop()
	}

	if cap(q.items) != initialCap {
		t.Errorf("expected queue backing slice capacity to remain %d after wrap around, got %d", initialCap, cap(q.items))
	}
}
