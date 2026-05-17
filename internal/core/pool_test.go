package core

import (
	"context"
	"testing"
)

func TestPooledBatchReset(t *testing.T) {
	batch := acquireJobBatch(10)
	batch.jobs = append(batch.jobs, InternalJob{KeyHash: 123, Run: dummyRun})

	releaseJobBatch(batch)

	if len(batch.jobs) != 0 {
		t.Errorf("expected batch slice length to be reset to 0, got %d", len(batch.jobs))
	}
}

func TestPooledBatchDoesNotRetainJobs(t *testing.T) {
	batch := acquireJobBatch(10)
	batch.jobs = append(batch.jobs, InternalJob{KeyHash: 123, Run: dummyRun})

	// Slice must be fully cleared before returning to pool
	batch.reset()

	// If we access the underlying slice elements up to capacity, they must be empty/zero-value
	underlying := batch.jobs[:cap(batch.jobs)]
	for i, j := range underlying {
		if j.Run != nil || j.KeyHash != 0 {
			t.Errorf("job at index %d was not cleared: %+v", i, j)
		}
	}
}

func TestPooledBatchDoesNotRetainUserData(t *testing.T) {
	userDataRun := func(ctx context.Context) error {
		return nil
	}

	batch := acquireJobBatch(5)
	batch.jobs = append(batch.jobs, InternalJob{KeyHash: 999, Run: userDataRun})

	releaseJobBatch(batch)

	// Fetch another batch and verify it doesn't contain reference to userDataRun
	batch2 := acquireJobBatch(5)
	defer releaseJobBatch(batch2)

	underlying := batch2.jobs[:cap(batch2.jobs)]
	for _, j := range underlying {
		if j.Run != nil {
			t.Error("reclaimed batch retained previous user function reference!")
		}
	}
}
