// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

// recordFailureKind increments failure classification counters (non-retry and retry paths).
func (q *Queue) recordFailureKind(kind FailureKind) {
	if q == nil || kind == FailureNone {
		return
	}
	q.retryObs.recordFailureKind(kind)
}
