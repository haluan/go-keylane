// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "time"

// OverloadPolicyEvent is emitted when overload policy rejects, sheds, or degrades a request.
// Keep decisions do not emit events by default.
type OverloadPolicyEvent struct {
	Time time.Time

	Lane  Lane
	Class LaneClass

	Action OverloadAction
	Reason OverloadReason

	RetryAfter  time.Duration
	BackoffHint BackoffHint

	GlobalPressure float64
	LanePressure   float64
	QueueDepth     uint32
	MaxQueueDepth  uint32

	PolicyVersion uint64
}
