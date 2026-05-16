package keylane

import (
	"context"
	"sync"
	"github.com/haluan/go-keylane/internal/core"
)

// Queue is the main entry point for the keylane library.
// It manages job routing, queueing, and execution.
type Queue struct {
	config  Config
	sched   *core.Scheduler
	reg     *core.LaneRegistry
	start   sync.Once
	started bool
}


// New creates a new Queue instance with the specified configuration.
func New(config Config) (*Queue, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// Convert map[Lane]int to map[string]int for LaneRegistry
	quotas := make(map[string]int, len(config.LaneQuotas))
	for lane, quota := range config.LaneQuotas {
		quotas[string(lane)] = quota
	}

	reg, err := core.NewLaneRegistry(quotas)
	if err != nil {
		return nil, err
	}

	sched, err := core.NewScheduler(config.ShardCount, config.WorkerCount, config.QueueSizePerLane, reg)
	if err != nil {
		return nil, err
	}

	return &Queue{
		config: config,
		sched:  sched,
		reg:    reg,
	}, nil
}

// Start launches the worker goroutines.
// It returns ErrQueueAlreadyStarted if called more than once.
func (q *Queue) Start(ctx context.Context) error {
	started := false
	q.start.Do(func() {
		for i := 0; i < q.config.WorkerCount; i++ {
			go q.sched.WorkerLoop(ctx)
		}
		q.started = true
		started = true
	})

	if !started {
		return ErrQueueAlreadyStarted
	}
	return nil
}
