package core

import (
	"context"
)

// WorkerLoop waits for ready shards and processes them.
// It runs until the context is cancelled.
func (s *Scheduler) WorkerLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case shardID := <-s.ReadyCh:
			if shardID < 0 || shardID >= len(s.shards) {
				continue
			}
			s.processShard(ctx, shardID)
		}
	}
}
