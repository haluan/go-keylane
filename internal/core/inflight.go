package core

// Inflight returns the number of currently active/in-flight jobs.
func (s *Scheduler) Inflight() int64 {
	return s.inflight.Load()
}
