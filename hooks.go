// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "time"

// RequestHooks contains optional callbacks for the request runtime (SubmitRequest).
// Request hooks are disabled when Observability.EnableHooks is false or callbacks are nil.
// They complement job-level OnJobTiming hooks; both may fire for SubmitRequest work.
type RequestHooks struct {
	OnQueued    func(RequestMeta)
	OnStarted   func(RequestObservation)
	OnCompleted func(RequestObservation)
	OnRejected  func(RequestObservation)
}

// Hooks contains user-definable callbacks for observability events.
type Hooks struct {
	// OnJobTiming is called after each accepted job finishes Run, with queue wait and run duration.
	OnJobTiming func(JobTimingEvent)
	// OnSlowJob is called when a job's run duration meets or exceeds the slow job threshold.
	OnSlowJob func(SlowJobEvent)
	// Request holds optional SubmitRequest lifecycle hooks.
	Request RequestHooks
	// OnAdaptiveQuotaDecision fires after adaptive quota evaluation produces a decision.
	// The callback receives AdaptiveQuotaDecisionEvent (KL-1405 spec name); AdaptiveQuotaEvent is the same type.
	// Successful quota changes and apply failures always invoke this hook when enabled.
	// Hold decisions invoke it only when ObservabilityConfig.EnableAdaptiveDecisionTracing is true.
	OnAdaptiveQuotaDecision func(AdaptiveQuotaDecisionEvent)
	// OnQuotaChange fires after a successful quota policy publish (manual or adaptive).
	OnQuotaChange func(QuotaChangeEvent)
	// OnOverloadPolicyDecision fires when overload policy rejects, sheds, or degrades a request.
	OnOverloadPolicyDecision func(OverloadPolicyEvent)
	// OnPerKeyAdmissionDecision fires when per-key admission throttles, rejects, or sheds a request.
	OnPerKeyAdmissionDecision func(PerKeyAdmissionDecisionEvent)
}

// PerKeyAdmissionDecisionEvent carries per-key admission decision metadata (key_hash only).
type PerKeyAdmissionDecisionEvent struct {
	Decision PerKeyAdmissionDecision
}

// JobTimingEvent contains queue wait and run duration for a completed job.
type JobTimingEvent struct {
	ShardID     int
	LaneID      uint16
	Lane        Lane
	QueueWait   time.Duration
	RunDuration time.Duration
	Outcome     JobOutcome
}

// SlowJobEvent contains details about a slow job execution.
type SlowJobEvent struct {
	ShardID     int
	LaneID      uint16
	Lane        Lane
	QueueWait   time.Duration
	RunDuration time.Duration
	Threshold   time.Duration
	Outcome     JobOutcome
}

// JobOutcome describes how a job finished.
type JobOutcome uint8

const (
	// JobOutcomeCompleted indicates the job returned nil.
	JobOutcomeCompleted JobOutcome = iota
	// JobOutcomeFailed indicates the job returned a non-nil error other than context.Canceled.
	JobOutcomeFailed
	// JobOutcomeCanceled indicates the job returned context.Canceled or was skipped due to worker cancel.
	JobOutcomeCanceled
	// JobOutcomePanicked is reserved for panic recovery; not emitted until panic recovery exists.
	JobOutcomePanicked
)
