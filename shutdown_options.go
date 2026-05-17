package keylane

type StopOption func(*stopConfig)

type stopConfig struct {
	drain bool // default: true
}

// WithDrain configures whether Stop should wait for all queued and in-flight jobs to finish before stopping.
func WithDrain(enabled bool) StopOption {
	return func(c *stopConfig) {
		c.drain = enabled
	}
}
