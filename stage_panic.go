// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "fmt"

func stageRecoveredPanicError(v any) error {
	return PermanentFailure(fmt.Errorf("keylane: stage panic: %v", v))
}

func recoverStageRun[S any](fn func() (S, error)) (state S, err error) {
	defer func() {
		if r := recover(); r != nil {
			var zero S
			state = zero
			err = stageRecoveredPanicError(r)
		}
	}()
	return fn()
}

func recoverStageResult[S any](fn func() (StageResult[S], error)) (result StageResult[S], err error) {
	defer func() {
		if r := recover(); r != nil {
			result = StageResult[S]{}
			err = stageRecoveredPanicError(r)
		}
	}()
	return fn()
}
