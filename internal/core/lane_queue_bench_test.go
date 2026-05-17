package core

import (
	"context"
	"testing"
)

func dummyRun(ctx context.Context) error {
	return nil
}

func BenchmarkLaneQueuePushPop(b *testing.B) {
	q := newLaneQueue(1024)
	job := InternalJob{KeyHash: 1, LaneID: 0, Run: dummyRun}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.push(job)
		_, _ = q.pop()
	}
}

func BenchmarkLaneQueuePopN(b *testing.B) {
	q := newLaneQueue(1024)
	job := InternalJob{KeyHash: 1, LaneID: 0, Run: dummyRun}
	// Fill the queue
	for i := 0; i < 512; i++ {
		_ = q.push(job)
	}

	dst := make([]InternalJob, 0, 512)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// popN into pre-allocated dst
		dst = q.popN(512, dst[:0])
		// push them back
		for _, j := range dst {
			_ = q.push(j)
		}
	}
}

func BenchmarkLaneQueueWrapAround(b *testing.B) {
	q := newLaneQueue(4)
	job := InternalJob{KeyHash: 1, LaneID: 0, Run: dummyRun}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.push(job)
		_, _ = q.pop()
		_ = q.push(job)
		_, _ = q.pop()
		_ = q.push(job)
		_, _ = q.pop()
	}
}
