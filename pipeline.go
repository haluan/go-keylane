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

// StageFunc runs one pipeline stage, returning updated state or an error.
type StageFunc[S any] func(context.Context, S) (S, error)

// PipelineStage is one ordered step in a same-state request pipeline.
type PipelineStage[S any] struct {
	Meta StageMeta
	Run  StageFunc[S]
}

// Pipeline is a typed multi-stage request with the same policies as Request.
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
		if st.Run == nil {
			return ErrNilJobRun
		}
	}
	if p.Complete == nil {
		return ErrNilPipelineComplete
	}
	return nil
}
