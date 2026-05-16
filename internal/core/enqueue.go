package core

import "errors"

var errInvalidLaneID = errors.New("keylane: invalid lane id")

// enqueueIntoShard adds a job to the correct lane queue inside the shard.
// It returns becameReady=true if the shard was not Ready and is now Ready.
func enqueueIntoShard(s *shard, job InternalJob) (becameReady bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if int(job.LaneID) >= len(s.Lanes) {
		return false, errInvalidLaneID
	}

	if err := s.Lanes[job.LaneID].push(job); err != nil {
		return false, err
	}

	if !s.Ready {
		s.Ready = true
		return true, nil
	}

	return false, nil
}
