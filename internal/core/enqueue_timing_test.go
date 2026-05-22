// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "testing"

func TestEnqueueIntoShardSkipsTimestampsWhenTimingDisabled(t *testing.T) {
	s := newShard(1, 10)
	job := InternalJob{LaneID: 0, KeyHash: 1}

	_, err := enqueueIntoShard(&s, job, false, false)
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	queued, ok := s.Lanes[0].pop()
	if !ok {
		t.Fatal("expected queued job")
	}
	if !queued.AcceptedAt.IsZero() {
		t.Error("AcceptedAt should remain zero when stampAcceptedAt and trackQueueWait are false")
	}
	if !queued.EnqueuedAt.IsZero() {
		t.Error("EnqueuedAt should remain zero when timing is disabled")
	}
}
