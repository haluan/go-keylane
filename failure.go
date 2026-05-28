// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
)

// FailureKind classifies how a job or request failed.
type FailureKind string

const (
	FailureNone              FailureKind = "none"
	FailureRetryable         FailureKind = "retryable"
	FailurePermanent         FailureKind = "permanent"
	FailureTimeout           FailureKind = "timeout"
	FailureCancelled         FailureKind = "cancelled"
	FailureOverloaded        FailureKind = "overloaded"
	FailureRejected          FailureKind = "rejected"
	FailureDeadlineExhausted FailureKind = "deadline_exhausted"
	FailurePanic             FailureKind = "panic"
	FailureUnknown           FailureKind = "unknown"
)

// Failure is a structured wrapper around an underlying error with classification metadata.
//
// Temporary is true when the failure is transient (currently set alongside Retryable for
// retryable failures). IsRetryable is the policy signal for retry decisions; IsTemporary
// mirrors the same transient hint for callers that distinguish temporary vs permanent errors.
type Failure struct {
	Kind      FailureKind
	Err       error
	Retryable bool
	Temporary bool
	Message   string
}

func (f Failure) Error() string {
	if f.Message != "" {
		return f.Message
	}
	if f.Err != nil {
		return f.Err.Error()
	}
	return string(f.Kind)
}

func (f Failure) Unwrap() error { return f.Err }

func (f Failure) IsRetryable() bool { return f.Retryable }

func (f Failure) IsTemporary() bool { return f.Temporary }

func (f Failure) IsTerminal() bool {
	switch f.Kind {
	case FailureNone, FailureRetryable:
		return false
	default:
		return true
	}
}

// FailureClassifier maps domain errors into keylane failure kinds.
type FailureClassifier func(error) Failure

// FailurePolicy configures optional custom failure classification.
// The struct is extensible for retry settings; today only Classifier is used.
type FailurePolicy struct {
	Classifier FailureClassifier
}

func NewFailure(kind FailureKind, err error) Failure {
	f := Failure{Kind: kind, Err: err}
	switch kind {
	case FailureRetryable:
		f.Retryable = true
		f.Temporary = true
	case FailurePermanent, FailureTimeout, FailureCancelled, FailureOverloaded,
		FailureRejected, FailureDeadlineExhausted, FailurePanic:
		f.Retryable = false
	case FailureUnknown:
		f.Retryable = false
	}
	return f
}

func RetryableFailure(err error) Failure {
	return NewFailure(FailureRetryable, err)
}

func PermanentFailure(err error) Failure {
	return NewFailure(FailurePermanent, err)
}

func TimeoutFailure(err error) Failure {
	return NewFailure(FailureTimeout, err)
}

func CancelledFailure(err error) Failure {
	return NewFailure(FailureCancelled, err)
}

func OverloadedFailure(err error) Failure {
	return NewFailure(FailureOverloaded, err)
}

func RejectedFailure(err error) Failure {
	return NewFailure(FailureRejected, err)
}

func DeadlineExhaustedFailure(err error) Failure {
	return NewFailure(FailureDeadlineExhausted, err)
}

func PanicFailure(err error) Failure {
	return NewFailure(FailurePanic, err)
}

func UnknownFailure(err error) Failure {
	return NewFailure(FailureUnknown, err)
}

// ClassifyFailure classifies err using the default keylane classifier.
func ClassifyFailure(err error) Failure {
	return classifyFailureWithPolicy(err, FailurePolicy{})
}

func classifyFailureWithPolicy(err error, policy FailurePolicy) Failure {
	if err == nil {
		return NewFailure(FailureNone, nil)
	}
	var f Failure
	if errors.As(err, &f) {
		return f
	}
	if policy.Classifier != nil {
		if custom := policy.Classifier(err); custom.Kind != "" && custom.Kind != FailureUnknown {
			return custom
		}
	}
	return classifyDefault(err)
}

func classifyDefault(err error) Failure {
	if err == nil {
		return NewFailure(FailureNone, nil)
	}
	var f Failure
	if errors.As(err, &f) {
		return f
	}
	if errors.Is(err, context.Canceled) {
		return CancelledFailure(err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return TimeoutFailure(err)
	}
	if errors.Is(err, ErrJobPanicked) {
		return PanicFailure(err)
	}
	if isOverloadError(err) {
		return OverloadedFailure(err)
	}
	if isRejectedError(err) {
		f := RejectedFailure(err)
		if errors.Is(err, ErrPerKeyAdmissionThrottled) {
			f.Retryable = true
			f.Temporary = true
		}
		return f
	}
	return UnknownFailure(err)
}

func isOverloadError(err error) bool {
	return errors.Is(err, ErrOverloadRejected) ||
		errors.Is(err, ErrOverloadShed) ||
		errors.Is(err, ErrOverloadDegraded)
}

func isRejectedError(err error) bool {
	if errors.Is(err, ErrAdmissionRejected) {
		return true
	}
	if errors.Is(err, ErrQueueFull) || errors.Is(err, ErrStopped) ||
		errors.Is(err, ErrNotStarted) || errors.Is(err, ErrQueueNotStarted) ||
		errors.Is(err, ErrInvalidLane) || errors.Is(err, ErrInvalidJob) ||
		errors.Is(err, ErrInvalidKey) || errors.Is(err, ErrNilJobRun) {
		return true
	}
	if errors.Is(err, ErrPerKeyAdmissionRejected) ||
		errors.Is(err, ErrPerKeyAdmissionThrottled) ||
		errors.Is(err, ErrPerKeyAdmissionShed) {
		return true
	}
	var admission AdmissionRejectedError
	if errors.As(err, &admission) {
		return true
	}
	var perKey PerKeyAdmissionError
	return errors.As(err, &perKey)
}

// IsFailure reports whether err is or wraps a Failure.
func IsFailure(err error) bool {
	var f Failure
	return errors.As(err, &f)
}

// AsFailure extracts a Failure from err.
func AsFailure(err error) (Failure, bool) {
	var f Failure
	if errors.As(err, &f) {
		return f, true
	}
	return Failure{}, false
}

func (q *Queue) classifyFailure(err error) Failure {
	policy := FailurePolicy{}
	if q != nil {
		policy = q.failurePolicy
	}
	return classifyFailureWithPolicy(err, policy)
}

// failureKindToRequestOutcome maps FailureKind to the existing request outcome enum.
func failureKindToRequestOutcome(kind FailureKind) RequestOutcome {
	switch kind {
	case FailureNone:
		return RequestOutcomeCompleted
	case FailureCancelled:
		return RequestOutcomeCancelled
	case FailureTimeout, FailureDeadlineExhausted:
		return RequestOutcomeTimedOut
	case FailureOverloaded:
		return RequestOutcomeOverloadRejected
	case FailureRejected:
		return RequestOutcomeRejected
	case FailureRetryable, FailurePermanent, FailureUnknown, FailurePanic:
		return RequestOutcomeFailed
	default:
		return RequestOutcomeFailed
	}
}
