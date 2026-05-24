// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"testing"
)

func TestClassifyFailureNil(t *testing.T) {
	f := ClassifyFailure(nil)
	if f.Kind != FailureNone {
		t.Fatalf("kind = %q, want none", f.Kind)
	}
	if f.IsRetryable() {
		t.Fatal("nil should not be retryable")
	}
}

func TestClassifyFailureContext(t *testing.T) {
	f := ClassifyFailure(context.Canceled)
	if f.Kind != FailureCancelled {
		t.Fatalf("kind = %q, want cancelled", f.Kind)
	}
	f = ClassifyFailure(context.DeadlineExceeded)
	if f.Kind != FailureTimeout {
		t.Fatalf("kind = %q, want timeout", f.Kind)
	}
}

func TestClassifyFailureKeylaneErrors(t *testing.T) {
	cases := []struct {
		err  error
		kind FailureKind
	}{
		{ErrOverloadRejected, FailureOverloaded},
		{ErrOverloadShed, FailureOverloaded},
		{ErrAdmissionRejected, FailureRejected},
		{ErrQueueFull, FailureRejected},
		{ErrPerKeyAdmissionRejected, FailureRejected},
	}
	for _, tc := range cases {
		f := ClassifyFailure(tc.err)
		if f.Kind != tc.kind {
			t.Errorf("%v: kind = %q, want %q", tc.err, f.Kind, tc.kind)
		}
	}
}

func TestClassifyFailurePerKeyThrottleRetryable(t *testing.T) {
	f := ClassifyFailure(ErrPerKeyAdmissionThrottled)
	if f.Kind != FailureRejected {
		t.Fatalf("kind = %q", f.Kind)
	}
	if !f.IsRetryable() {
		t.Fatal("throttle should be retryable")
	}
}

func TestClassifyFailureUnknownNotRetryable(t *testing.T) {
	err := errors.New("database temporarily unavailable")
	f := ClassifyFailure(err)
	if f.Kind != FailureUnknown {
		t.Fatalf("kind = %q", f.Kind)
	}
	if f.IsRetryable() {
		t.Fatal("unknown must not be retryable by default")
	}
}

func TestExplicitRetryablePermanent(t *testing.T) {
	r := RetryableFailure(errors.New("transient"))
	if !r.IsRetryable() || r.Kind != FailureRetryable {
		t.Fatalf("retryable: %+v", r)
	}
	p := PermanentFailure(errors.New("bad input"))
	if p.IsRetryable() || p.Kind != FailurePermanent {
		t.Fatalf("permanent: %+v", p)
	}
}

func TestFailureWrapErrorsAs(t *testing.T) {
	inner := errors.New("inner")
	f := RejectedFailure(inner)
	if !errors.Is(f, inner) {
		t.Fatal("errors.Is should find inner")
	}
	var got Failure
	if !errors.As(f, &got) {
		t.Fatal("errors.As Failure")
	}
	if got.Kind != FailureRejected {
		t.Fatalf("kind = %q", got.Kind)
	}
}

func TestCustomClassifier(t *testing.T) {
	policy := FailurePolicy{
		Classifier: func(err error) Failure {
			if errors.Is(err, context.DeadlineExceeded) {
				return PermanentFailure(err)
			}
			return Failure{}
		},
	}
	f := classifyFailureWithPolicy(context.DeadlineExceeded, policy)
	if f.Kind != FailurePermanent {
		t.Fatalf("custom classifier: kind = %q", f.Kind)
	}
}

func TestIsFailureAsFailure(t *testing.T) {
	f := TimeoutFailure(context.DeadlineExceeded)
	if !IsFailure(f) {
		t.Fatal("IsFailure")
	}
	got, ok := AsFailure(f)
	if !ok || got.Kind != FailureTimeout {
		t.Fatalf("AsFailure: %+v ok=%v", got, ok)
	}
}
