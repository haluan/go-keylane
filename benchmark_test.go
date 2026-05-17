package keylane

import (
	"context"
	"fmt"
	"math/rand"
)

// shared helper for generating random strings
func randomKey(r *rand.Rand, limit int) string {
	return fmt.Sprintf("key-%d", r.Intn(limit))
}

// dummy job run function
func dummyRun(ctx context.Context) error {
	return nil
}

// dummy value job run function
func dummyValueRun(ctx context.Context) (int, error) {
	return 42, nil
}

// helper to initialize a running queue
func setupQueue(shardCount, workerCount, queueSize int, lanes map[Lane]int) (*Queue, context.CancelFunc) {
	cfg := Config{
		ShardCount:       shardCount,
		WorkerCount:      workerCount,
		QueueSizePerLane: queueSize,
		LaneQuotas:       lanes,
	}
	q, err := New(cfg)
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := q.Start(ctx); err != nil {
		cancel()
		panic(err)
	}
	return q, cancel
}
