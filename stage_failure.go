// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"errors"
	"fmt"
)

// StageFailure attributes a pipeline stage error without replacing classified failure semantics.
// Use AsStageFailure or errors.As(err, &sf) where sf is *StageFailure.
// FailureFromFuture still classifies the underlying error via Unwrap.
type StageFailure struct {
	Execution StageExecutionContext
	Stage     StageMeta
	Err       error
}

func (e *StageFailure) Error() string {
	if e == nil {
		return "keylane: stage failure"
	}
	name := e.Stage.Name
	if name == "" && e.Execution.Stage.Name != "" {
		name = e.Execution.Stage.Name
	}
	if e.Err != nil {
		return fmt.Sprintf("keylane: stage %q: %s", name, e.Err.Error())
	}
	return fmt.Sprintf("keylane: stage %q failed", name)
}

func (e *StageFailure) Unwrap() error { return e.Err }

// NewStageFailure wraps err with stage metadata. err must be non-nil.
func NewStageFailure(stage StageMeta, err error) error {
	if err == nil {
		return nil
	}
	return &StageFailure{Stage: stage, Err: err}
}

// NewStageFailureFromExecution wraps err with full execution metadata at failure time.
func NewStageFailureFromExecution(exec StageExecutionContext, err error) error {
	if err == nil {
		return nil
	}
	return &StageFailure{
		Execution: exec,
		Stage:     exec.Stage,
		Err:       err,
	}
}

// AsStageFailure extracts stage attribution from err.
func AsStageFailure(err error) (StageFailure, bool) {
	var sf *StageFailure
	if errors.As(err, &sf) && sf != nil {
		return *sf, true
	}
	return StageFailure{}, false
}
