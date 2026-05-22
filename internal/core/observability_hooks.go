// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"errors"
	"time"
)

// JobOutcome describes how a job finished (internal mirror of keylane.JobOutcome).
type JobOutcome uint8

const (
	JobOutcomeCompleted JobOutcome = iota
	JobOutcomeFailed
	JobOutcomeCanceled
	JobOutcomePanicked
)

func jobOutcomeFromError(err error) JobOutcome {
	if err == nil {
		return JobOutcomeCompleted
	}
	if errors.Is(err, context.Canceled) {
		return JobOutcomeCanceled
	}
	return JobOutcomeFailed
}

func callObservabilityHook(fn func()) {
	defer func() { _ = recover() }()
	fn()
}

// obsHooksNeedRunDuration reports whether the worker must measure run duration for hooks.
func (s *Scheduler) obsHooksNeedRunDuration() bool {
	if !s.Obs.EnableHooks {
		return false
	}
	if s.Obs.OnJobTiming != nil {
		return true
	}
	return s.Obs.OnSlowJob != nil && s.Obs.SlowJobThreshold > 0
}

// jobNeedsWorkerTimestamps reports whether time.Now/Since are required for this job.
func (s *Scheduler) jobNeedsWorkerTimestamps(job InternalJob) (needQueueWait bool, needRunDuration bool) {
	if s.Obs.EnableQueueWaitTiming && !job.AcceptedAt.IsZero() {
		needQueueWait = true
	}
	if s.Obs.TrackQueueWait && !job.EnqueuedAt.IsZero() {
		needQueueWait = true
	}
	needRunDuration = s.Obs.EnableRunTiming || s.obsHooksNeedRunDuration()
	return needQueueWait, needRunDuration
}

func (s *Scheduler) emitObservabilityHooks(shardID int, laneID LaneID, queueWait, runDuration time.Duration, err error) {
	if !s.Obs.EnableHooks {
		return
	}
	outcome := jobOutcomeFromError(err)
	needTiming := s.Obs.OnJobTiming != nil
	needSlow := s.Obs.SlowJobThreshold > 0 && s.Obs.OnSlowJob != nil && runDuration >= s.Obs.SlowJobThreshold
	if !needTiming && !needSlow {
		return
	}

	laneName := s.laneReg.Name(laneID)

	if needTiming {
		fn := s.Obs.OnJobTiming
		callObservabilityHook(func() {
			fn(shardID, laneID, laneName, queueWait, runDuration, outcome)
		})
	}
	if needSlow {
		threshold := s.Obs.SlowJobThreshold
		fn := s.Obs.OnSlowJob
		callObservabilityHook(func() {
			fn(shardID, laneID, laneName, queueWait, runDuration, threshold, outcome)
		})
	}
}
