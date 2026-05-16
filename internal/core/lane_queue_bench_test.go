package core

import "testing"

func BenchmarkLaneQueuePushPop(b *testing.B) {
	q := newLaneQueue(100)
	job := InternalJob{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.push(job)
		_, _ = q.pop()
	}
}

func BenchmarkLaneQueuePopN(b *testing.B) {
	q := newLaneQueue(100)
	job := InternalJob{}
	dst := make([]InternalJob, 0, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			_ = q.push(job)
		}
		dst = dst[:0]
		dst = q.popN(10, dst)
	}
}
