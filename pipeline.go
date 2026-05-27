// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
)

// ErrEmptyPipelineStages is returned when a pipeline has no stages.
var ErrEmptyPipelineStages = errors.New("keylane: pipeline has no stages")

// ErrNilPipelineComplete is returned when Pipeline.Complete is nil.
var ErrNilPipelineComplete = errors.New("keylane: nil pipeline complete function")

// ErrAmbiguousPipelineStage is returned when a PipelineStage sets both Run and RunContinuation.
var ErrAmbiguousPipelineStage = errors.New("keylane: pipeline stage must set Run or RunContinuation, not both")

// StageFunc runs one pipeline stage, returning updated state or an error.
type StageFunc[S any] func(context.Context, S) (S, error)

// PipelineStage is one ordered step in a same-state request pipeline.
// Exactly one of Run or RunContinuation must be set.
//
// Experimental: may change before v1.0.
type PipelineStage[S any] struct {
	Meta StageMeta

	// Run is the synchronous stage function.
	Run StageFunc[S]

	// RunContinuation is the optional continuation-aware stage function.
	// When set, the stage may yield execution and resume later via a ContinuationCompleter.
	// Requires Config.Continuation.Enabled on the Queue.
	RunContinuation ContinuationStageFunc[S]
}

// Pipeline is a typed multi-stage request with the same policies as Request.
//
// Experimental: may change before v1.0. In-process orchestration only; not a persistent workflow.
type Pipeline[S any, O any] struct {
	Meta             RequestMeta
	Admission        AdmissionConfig
	Overload         OverloadConfig
	PerKeyAdmission  PerKeyAdmissionConfig
	Retry            RetryPolicy
	Idempotency      Idempotency
	RetrySuppression *RetrySuppressionPolicy

	State    S
	Stages   []PipelineStage[S]
	Complete func(context.Context, S) (O, error)
}

func validatePipeline[S any, O any](p Pipeline[S, O]) error {
	if p.Meta.Key == "" {
		return ErrInvalidKey
	}
	if err := p.Meta.Lane.Validate(); err != nil {
		return err
	}
	if len(p.Stages) == 0 {
		return ErrEmptyPipelineStages
	}
	for i := range p.Stages {
		st := p.Stages[i]
		if err := ValidateStageMeta(st.Meta); err != nil {
			return err
		}
		if st.Run != nil && st.RunContinuation != nil {
			return ErrAmbiguousPipelineStage
		}
		if st.Run == nil && st.RunContinuation == nil {
			return ErrNilJobRun
		}
	}
	if p.Complete == nil {
		return ErrNilPipelineComplete
	}
	return nil
}
