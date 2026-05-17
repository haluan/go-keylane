package core

import (
	"context"
	"fmt"
	"testing"
)

// newTestInternalJob returns a mock InternalJob with keyHash and laneID.
func newTestInternalJob(laneID LaneID, keyHash uint64) InternalJob {
	return InternalJob{
		KeyHash: keyHash,
		LaneID:  laneID,
		Run: func(ctx context.Context) error {
			return nil
		},
	}
}

// findKeyForShard finds the first key that routes to the target shardID.
func findKeyForShard(t *testing.T, shardID int, shardCount int) string {
	t.Helper()
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("key-%d", i)
		hash := HashKey(key)
		if routeShardID(hash, shardCount) == shardID {
			return key
		}
	}
	t.Fatalf("failed to find key routing to shard %d", shardID)
	return ""
}

// drainReadyCh drains up to 100 values from the channel, returning them.
func drainReadyCh(ch <-chan int) []int {
	var ids []int
	for {
		select {
		case id := <-ch:
			ids = append(ids, id)
			if len(ids) >= 100 {
				return ids
			}
		default:
			return ids
		}
	}
}
