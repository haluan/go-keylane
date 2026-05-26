// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "errors"

var ErrContinuationDisabled = errors.New("keylane: continuation disabled")
var ErrContinuationLimitExceeded = errors.New("keylane: continuation limit exceeded")
var ErrContinuationAlreadyCompleted = errors.New("keylane: continuation already completed")
var ErrContinuationCancelled = errors.New("keylane: continuation cancelled")
var ErrContinuationExpired = errors.New("keylane: continuation expired")
var ErrInvalidContinuation = errors.New("keylane: invalid continuation")
var ErrContinuationResumeRejected = errors.New("keylane: continuation resume rejected")
var ErrContinuationLate = errors.New("keylane: continuation late completion")
