package core

// isDrained returns true if all lane queues in all shards are empty
// and there are no active/in-flight jobs.
func (s *Scheduler) isDrained() bool {
	if s.inflight.Load() != 0 {
		return false
	}
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.Lock()
		hasWork := shard.hasWorkLocked()
		shard.mu.Unlock()
		if hasWork {
			return false
		}
	}
	// Double check to catch any jobs popped during shard checking
	return s.inflight.Load() == 0
}
