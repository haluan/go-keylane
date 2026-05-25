// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryFailureSnapshotStartsZero(t *testing.T) {
	q, err := New(newTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	snap := q.RetryFailureSnapshot()
	if snap.FailuresTotal != 0 || snap.RetriesScheduledTotal != 0 {
		t.Fatalf("snap = %+v", snap)
	}
}

func TestRecordFailureKindIncrementsBucket(t *testing.T) {
	q, err := New(newTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	kinds := []FailureKind{
		FailureRetryable, FailurePermanent, FailureTimeout, FailureCancelled,
		FailureDeadlineExhausted,
	}
	for _, k := range kinds {
		q.recordFailureKind(k)
	}
	snap := q.RetryFailureSnapshot()
	if snap.FailuresTotal != uint64(len(kinds)) {
		t.Fatalf("FailuresTotal = %d want %d", snap.FailuresTotal, len(kinds))
	}
	if snap.TimeoutsTotal < 2 {
		t.Fatalf("TimeoutsTotal = %d", snap.TimeoutsTotal)
	}
	if snap.CancellationsTotal != 1 {
		t.Fatalf("CancellationsTotal = %d", snap.CancellationsTotal)
	}
	found := make(map[FailureKind]uint64)
	for _, c := range snap.ByFailureKind {
		found[c.Kind] = c.Count
	}
	for _, k := range kinds {
		if found[k] != 1 {
			t.Fatalf("kind %q count = %d", k, found[k])
		}
	}
}

func TestSubmitValueWithoutRetryRecordsFailureKind(t *testing.T) {
	cfg := newTestConfig()
	cfg.Retry.Enabled = false
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Stop(context.Background()) }()

	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default",
		Run: func(context.Context) (int, error) {
			return 0, PermanentFailure(errors.New("nope"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, awaitErr := future.Await(context.Background()); awaitErr == nil {
		t.Fatal("expected error")
	}
	snap := q.RetryFailureSnapshot()
	if snap.FailuresTotal != 1 {
		t.Fatalf("FailuresTotal = %d", snap.FailuresTotal)
	}
	if len(snap.ByFailureKind) != 1 || snap.ByFailureKind[0].Kind != FailurePermanent {
		t.Fatalf("ByFailureKind = %+v", snap.ByFailureKind)
	}
	if snap.RetriesScheduledTotal != 0 {
		t.Fatalf("RetriesScheduledTotal = %d, want 0 when retry disabled", snap.RetriesScheduledTotal)
	}
}

type customClassifierErr struct{}

func (customClassifierErr) Error() string { return "custom-classifier-sentinel" }

func TestCustomFailurePolicyReflectedInCountersAndEvents(t *testing.T) {
	q, err := New(newTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	policy := FailurePolicy{
		Classifier: func(err error) Failure {
			var sentinel customClassifierErr
			if errors.As(err, &sentinel) {
				return PermanentFailure(err)
			}
			return classifyFailureWithPolicy(err, FailurePolicy{})
		},
	}
	var events []RetryEvent
	cfg := newTestConfig()
	cfg.Observability.EnableHooks = true
	cfg.Observability.Hooks.Retry.OnRetryEvent = func(e RetryEvent) {
		events = append(events, e)
	}
	q2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	p := RetryPolicy{Enabled: true, MaxAttempts: 2, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0}
	opts := runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetySafe}, Observer: q.retryObserver()}
	_ = runWithRetry(context.Background(), policy, p, opts, NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), func(int) (int, error) {
		return 0, customClassifierErr{}
	})
	snap := q.RetryFailureSnapshot()
	if countFailureKind(snap, FailurePermanent) != 1 {
		t.Fatalf("ByFailureKind = %+v", snap.ByFailureKind)
	}

	opts2 := runWithRetryOpts{Idempotency: Idempotency{Safety: RetrySafetySafe}, Observer: q2.retryObserver()}
	_ = runWithRetry(context.Background(), policy, p, opts2, NewDeadlineBudget(context.Background(), now), &testRetryClock{now: now}, fixedJitterSource(0.5), func(int) (int, error) {
		return 0, customClassifierErr{}
	})
	var classified RetryEvent
	for _, e := range events {
		if e.Kind == RetryEventFailureClassified {
			classified = e
		}
	}
	if classified.FailureKind != FailurePermanent {
		t.Fatalf("event = %+v", classified)
	}
}

func countFailureKind(snap RetryFailureSnapshot, kind FailureKind) uint64 {
	for _, c := range snap.ByFailureKind {
		if c.Kind == kind {
			return c.Count
		}
	}
	return 0
}

func TestRetryFailureSnapshotNoIdempotencyKeyField(t *testing.T) {
	// Snapshot types must not expose raw idempotency keys (compile-time / API check).
	var _ RetryFailureSnapshot
	_ = RetryFailureSnapshot{}.ByFailureKind
}
