package keylane

import "fmt"

type Config struct {
	ShardCount       int
	WorkerCount      int
	QueueSizePerLane int
	LaneQuotas       map[Lane]int
}

// Validate ensures the configuration is valid.
func (c Config) Validate() error {
	if c.ShardCount < 1 {
		return fmt.Errorf("%w: ShardCount must be at least 1", ErrInvalidShardCount)
	}
	if c.WorkerCount < 1 {
		return fmt.Errorf("%w: WorkerCount must be at least 1", ErrInvalidWorkerCount)
	}
	if c.QueueSizePerLane < 1 {
		return fmt.Errorf("%w: QueueSizePerLane must be at least 1", ErrInvalidQueueSize)
	}
	if len(c.LaneQuotas) == 0 {
		return ErrMissingLaneQuotas
	}
	for lane, quota := range c.LaneQuotas {
		if lane == "" {
			return fmt.Errorf("%w: lane name cannot be empty", ErrInvalidLane)
		}
		if quota < 1 {
			return fmt.Errorf("%w: quota for lane %q must be at least 1", ErrInvalidLaneQuota, lane)
		}
	}
	return nil
}

