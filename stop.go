package keylane

import (
	"context"
)

// Stop gracefully stops the queue processing.
func (q *Queue) Stop(ctx context.Context, opts ...StopOption) error {
	cfg := stopConfig{
		drain: true,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	return q.sched.Stop(ctx, cfg.drain)
}
