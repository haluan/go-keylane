// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import "errors"

var (
	ErrInvalidConfig                = errors.New("keylane: invalid config")
	ErrInvalidShardCount            = errors.New("keylane: invalid shard count")
	ErrInvalidWorkerCount           = errors.New("keylane: invalid worker count")
	ErrInvalidQueueSize             = errors.New("keylane: invalid queue size per lane")
	ErrInvalidLane                  = errors.New("keylane: invalid lane")
	ErrInvalidLaneQuota             = errors.New("keylane: invalid lane quota")
	ErrMissingLaneQuotas            = errors.New("keylane: missing lane quotas")
	ErrInvalidQuotaPolicy           = errors.New("keylane: invalid quota policy")
	ErrQuotaPolicyVersionConflict   = errors.New("keylane: quota policy version conflict")
	ErrQuotaTooLarge                = errors.New("keylane: quota too large")
	ErrInvalidAdmissionPolicy       = errors.New("keylane: invalid admission policy")
	ErrInvalidOverloadPolicy        = errors.New("keylane: invalid overload policy")
	ErrInvalidAdaptiveQuotaConfig   = errors.New("keylane: invalid adaptive quota config")
	ErrInvalidHotKeyConfig          = errors.New("keylane: invalid hot key config")
	ErrInvalidPerKeyAdmissionConfig = errors.New("keylane: invalid per-key admission config")
	ErrInvalidLaneClass             = errors.New("keylane: invalid lane class")
	ErrInvalidJob                   = errors.New("keylane: invalid job")
	ErrInvalidKey                   = errors.New("keylane: invalid key")
	ErrNilJobRun                    = errors.New("keylane: nil job run function")
	ErrQueueFull                    = errors.New("keylane: queue full")
	ErrNotStarted                   = errors.New("keylane: queue not started")
	ErrQueueNotStarted              = ErrNotStarted
	ErrQueueAlreadyStarted          = errors.New("keylane: queue already started")
	ErrStopped                      = errors.New("keylane: queue stopped")
)
