// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"errors"
	"time"
)

var errInvalidLaneID = errors.New("keylane: invalid lane id")

// enqueueIntoShard adds a job to the correct lane queue inside the shard.
// It returns becameReady=true if the shard was not Ready and is now Ready.
// Admission timestamps are stamped immediately before a successful push so queue
// wait excludes time spent waiting on the shard lock or admission checks.
func enqueueIntoShard(s *shard, job InternalJob, stampAcceptedAt, trackQueueWait bool) (becameReady bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if int(job.LaneID) >= len(s.Lanes) {
		return false, errInvalidLaneID
	}

	lane := &s.Lanes[job.LaneID]
	if err := lane.push(job); err != nil {
		return false, err
	}
	if stampAcceptedAt || trackQueueWait {
		lane.stampNewestAccepted(time.Now(), stampAcceptedAt, trackQueueWait)
	}

	if !s.Ready {
		s.Ready = true
		return true, nil
	}

	return false, nil
}
