package keylane

import "time"

// Hooks contains user-definable callbacks for observability events.
type Hooks struct {
	// OnSlowJob is called when a job execution duration meets or exceeds the slow job threshold.
	OnSlowJob func(SlowJobEvent)
}

// SlowJobEvent contains details about a slow job execution.
type SlowJobEvent struct {
	Lane     Lane
	ShardID  int
	Duration time.Duration
}
