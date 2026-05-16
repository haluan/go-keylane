package core

import "errors"

var errInvalidLaneID = errors.New("keylane: invalid lane id")

// enqueueIntoShard adds a job to the correct lane queue inside the shard.
// It returns becameReady=true if the shard was not ready and is now ready.
func enqueueIntoShard(s *shard, job InternalJob) (becameReady bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if int(job.LaneID) >= len(s.lanes) {
		return false, errInvalidLaneID
	}

	if err := s.lanes[job.LaneID].push(job); err != nil {
		return false, err
	}

	if !s.ready {
		s.ready = true
		becameReady = true
	}

	return becameReady, nil
}
