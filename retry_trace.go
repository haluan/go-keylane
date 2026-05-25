// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

// RetryFinalState explains how the retry loop ended on a result future.
type RetryFinalState struct {
	Succeeded bool
	Exhausted bool

	StoppedReason     RetryDecisionReason
	SafetyReason      RetrySafetyDecisionReason
	SuppressionReason RetrySuppressionReason
	FailureKind       FailureKind
}

// RetryTrace records retry scheduling decisions and final outcome on a result future.
type RetryTrace struct {
	Attempts []RetryAttempt
	Final    RetryFinalState
}

// FinalStoppedReason returns the primary stop reason (retry, safety, or suppression).
func (t RetryTrace) FinalStoppedReason() (string, bool) {
	if t.Final.Succeeded {
		return "", false
	}
	if t.Final.SuppressionReason != "" && t.Final.SuppressionReason != RetrySuppressionNone {
		return string(t.Final.SuppressionReason), true
	}
	if t.Final.SafetyReason != "" {
		return string(t.Final.SafetyReason), true
	}
	if t.Final.StoppedReason != "" && t.Final.StoppedReason != RetryDecisionNone {
		return string(t.Final.StoppedReason), true
	}
	return "", false
}
