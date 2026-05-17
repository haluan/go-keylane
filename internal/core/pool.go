package core

import "sync"

type jobBatch struct {
	jobs []InternalJob
}

func (b *jobBatch) reset() {
	for i := range b.jobs {
		b.jobs[i] = InternalJob{}
	}
	b.jobs = b.jobs[:0]
}

var jobBatchPool = sync.Pool{
	New: func() any {
		return &jobBatch{
			jobs: make([]InternalJob, 0, 64),
		}
	},
}

func acquireJobBatch(minCapacity int) *jobBatch {
	batch := jobBatchPool.Get().(*jobBatch)
	if cap(batch.jobs) < minCapacity {
		batch.jobs = make([]InternalJob, 0, minCapacity)
	}
	return batch
}

func releaseJobBatch(batch *jobBatch) {
	batch.reset()
	jobBatchPool.Put(batch)
}
