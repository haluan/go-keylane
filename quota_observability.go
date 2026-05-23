// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "time"

// QuotaChangeSource identifies who initiated a quota policy change.
type QuotaChangeSource string

const (
	// QuotaChangeManual indicates UpdateQuotaPolicy or UpdateLaneQuota.
	QuotaChangeManual QuotaChangeSource = "manual"
	// QuotaChangeAdaptive indicates the adaptive quota controller.
	QuotaChangeAdaptive QuotaChangeSource = "adaptive"
)

// QuotaChangeEvent is emitted after a successful quota policy publish.
type QuotaChangeEvent struct {
	Time time.Time

	Lane Lane

	OldQuota int
	NewQuota int

	Source QuotaChangeSource
	Reason string

	PolicyVersion uint64
	QuotaVersion  uint64
}
