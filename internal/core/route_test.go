package core

import "testing"

func TestRouteShardIDWithinRange(t *testing.T) {
	shardCounts := []int{1, 2, 8, 16, 64, 256}
	for _, count := range shardCounts {
		for i := uint64(0); i < 1000; i++ {
			id := routeShardID(i, count)
			if id < 0 || id >= count {
				t.Errorf("routeShardID(%d, %d) = %d; out of range [0, %d)", i, count, id, count)
			}
		}
	}
}

func TestRouteShardIDWithShardCountOne(t *testing.T) {
	for i := uint64(0); i < 1000; i++ {
		id := routeShardID(i, 1)
		if id != 0 {
			t.Errorf("routeShardID(%d, 1) = %d; want 0", i, id)
		}
	}
}

func TestRouteJobSameKeySameShard(t *testing.T) {
	job := InternalJob{KeyHash: 12345}
	shardCount := 64
	id1 := routeJob(job, shardCount)
	id2 := routeJob(job, shardCount)

	if id1 != id2 {
		t.Errorf("routeJob same job produced different shard IDs: %d != %d", id1, id2)
	}
}

func TestRouteJobDifferentShardCountsRemainValid(t *testing.T) {
	job := InternalJob{KeyHash: 12345}
	
	tests := []int{1, 7, 13, 100, 1024}
	for _, count := range tests {
		id := routeJob(job, count)
		if id < 0 || id >= count {
			t.Errorf("routeJob(job, %d) = %d; out of range [0, %d)", count, id, count)
		}
	}
}
