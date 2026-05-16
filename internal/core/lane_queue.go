package core

import (
	"errors"
)

var errLaneQueueFull = errors.New("keylane: lane queue full")

type laneQueue struct {
	items []InternalJob
	head  int
	tail  int
	size  int
}

func newLaneQueue(capacity int) laneQueue {
	return laneQueue{
		items: make([]InternalJob, capacity),
	}
}

func (q *laneQueue) push(job InternalJob) error {
	if q.size == len(q.items) {
		return errLaneQueueFull
	}
	q.items[q.tail] = job
	q.tail = (q.tail + 1) % len(q.items)
	q.size++
	return nil
}

func (q *laneQueue) pop() (InternalJob, bool) {
	if q.size == 0 {
		return InternalJob{}, false
	}
	job := q.items[q.head]
	q.items[q.head] = InternalJob{} // Clear reference
	q.head = (q.head + 1) % len(q.items)
	q.size--
	return job, true
}

func (q *laneQueue) popN(limit int, dst []InternalJob) []InternalJob {
	if limit <= 0 || q.size == 0 {
		return dst
	}
	if limit > q.size {
		limit = q.size
	}

	// Ensure we don't grow dst beyond its current capacity to avoid allocations.
	// It is the caller's responsibility to provide a slice with enough capacity.
	avail := cap(dst) - len(dst)
	if limit > avail {
		limit = avail
	}

	start := len(dst)
	dst = dst[:start+limit]
	for i := 0; i < limit; i++ {
		job, _ := q.pop()
		dst[start+i] = job
	}
	return dst
}

func (q *laneQueue) isFull() bool {
	return q.size == len(q.items)
}

func (q *laneQueue) isEmpty() bool {
	return q.size == 0
}

func (q *laneQueue) depth() int {
	return q.size
}

func (q *laneQueue) capacity() int {
	return len(q.items)
}
