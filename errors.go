// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"errors"

	"github.com/haluan/go-keylane/internal/core"
)

var (
	ErrInvalidConfig          = core.ErrInvalidConfig
	ErrInvalidShardCount      = core.ErrInvalidShardCount
	ErrInvalidWorkerCount     = core.ErrInvalidWorkerCount
	ErrInvalidQueueSize       = core.ErrInvalidQueueSize
	ErrInvalidLane            = core.ErrInvalidLane
	ErrInvalidLaneQuota       = core.ErrInvalidLaneQuota
	ErrMissingLaneQuotas      = core.ErrMissingLaneQuotas
	ErrInvalidQuotaPolicy     = core.ErrInvalidQuotaPolicy
	ErrQuotaTooLarge          = core.ErrQuotaTooLarge
	ErrInvalidAdmissionPolicy = core.ErrInvalidAdmissionPolicy
	ErrInvalidOverloadPolicy  = core.ErrInvalidOverloadPolicy
	ErrInvalidLaneClass       = core.ErrInvalidLaneClass
	ErrNilQueue               = errors.New("keylane: nil queue")
	ErrInvalidJob             = core.ErrInvalidJob
	ErrInvalidKey             = core.ErrInvalidKey
	ErrNilJobRun              = core.ErrNilJobRun
	ErrQueueFull              = core.ErrQueueFull
	ErrNotStarted             = core.ErrNotStarted
	ErrQueueNotStarted        = core.ErrQueueNotStarted
	ErrQueueAlreadyStarted    = core.ErrQueueAlreadyStarted
	ErrStopped                = core.ErrStopped
)
