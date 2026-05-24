// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"testing"
	"time"
)

func assertFutureFailureKind[T any](t *testing.T, f Future[T], want FailureKind) {
	t.Helper()
	failure, ok := FailureFromFuture(f)
	if !ok {
		t.Fatal("FailureFromFuture: not a result future")
	}
	if failure.Kind != want {
		t.Fatalf("failure kind = %q, want %q", failure.Kind, want)
	}
}

func TestIntegrationFailureQueueFull(t *testing.T) {
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 2,
		LaneQuotas:       map[Lane]int{"default": 1},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	run := func(context.Context) error { <-block; return nil }
	for i := 0; i < 2; i++ {
		if err := q.Submit(ctx, Job{Key: "k", Lane: "default", Run: run}); err != nil {
			t.Fatal(err)
		}
	}
	err = q.Submit(ctx, Job{Key: "k3", Lane: "default", Run: run})
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("err = %v", err)
	}
	f := ClassifyFailure(err)
	if f.Kind != FailureRejected {
		t.Fatalf("kind = %q", f.Kind)
	}
	close(block)
}

func TestIntegrationFailureAdmissionRejected(t *testing.T) {
	q := admissionTestQueue(t)
	fillQueueDepth(t, q, 9)
	err := CheckAdmission(q, AdmissionConfig{Enabled: true, RejectAboveRatio: 0.90}, RequestMeta{Key: "k", Lane: "default"})
	if !errors.Is(err, ErrAdmissionRejected) {
		t.Fatal(err)
	}
	if ClassifyFailure(err).Kind != FailureRejected {
		t.Fatalf("kind = %q", ClassifyFailure(err).Kind)
	}
}

func TestIntegrationFailureOverloadRejected(t *testing.T) {
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[Lane]int{"default": 1},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = q.UpdateOverloadPolicy(OverloadPolicy{
		Default: LaneOverloadPolicy{Class: LaneBestEffort, RejectAboveRatio: 0.01, ShedAboveRatio: 0.01, MaxQueueDepth: 100},
	})
	if err != nil {
		t.Fatal(err)
	}
	fillQueueDepth(t, q, 10)
	err = CheckOverload(q, OverloadConfig{Enabled: true}, RequestMeta{Key: "k", Lane: "default"})
	if err == nil {
		t.Fatal("expected overload reject")
	}
	if ClassifyFailure(err).Kind != FailureOverloaded {
		t.Fatalf("kind = %q", ClassifyFailure(err).Kind)
	}
}

func TestIntegrationFailureCancelledBeforeEnqueue(t *testing.T) {
	q, _ := testRequestQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	future, err := SubmitRequest(ctx, q, Request[sumInput, sumOutput]{
		Meta:  RequestMeta{Key: "k", Lane: "default"},
		Input: sumInput{},
		Handle: func(context.Context, sumInput) (sumOutput, error) {
			return sumOutput{}, nil
		},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("submit err = %v", err)
	}
	assertFutureFailureKind(t, future, FailureCancelled)
}

func TestIntegrationFailureCancelledWhileQueued(t *testing.T) {
	q, queueCtx := testRequestQueue(t)
	blocker := make(chan struct{})
	_ = q.Submit(queueCtx, Job{Key: "block", Lane: "default", Run: func(context.Context) error { <-blocker; return nil }})
	reqCtx, reqCancel := context.WithCancel(context.Background())
	future, err := SubmitRequest(reqCtx, q, Request[sumInput, sumOutput]{
		Meta:   RequestMeta{Key: "k", Lane: "default"},
		Input:  sumInput{},
		Handle: func(context.Context, sumInput) (sumOutput, error) { return sumOutput{}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	reqCancel()
	close(blocker)
	_, awaitErr := future.Await(context.Background())
	if !errors.Is(awaitErr, context.Canceled) {
		t.Fatalf("await err = %v", awaitErr)
	}
	assertFutureFailureKind(t, future, FailureCancelled)
}

func TestIntegrationFailureDeadlineExhaustedWhileQueued(t *testing.T) {
	q, queueCtx := testRequestQueue(t)
	blocker := make(chan struct{})
	_ = q.Submit(queueCtx, Job{Key: "block", Lane: "default", Run: func(context.Context) error { <-blocker; return nil }})
	reqCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	future, err := SubmitRequest(reqCtx, q, Request[sumInput, sumOutput]{
		Meta:   RequestMeta{Key: "k", Lane: "default"},
		Input:  sumInput{},
		Handle: func(context.Context, sumInput) (sumOutput, error) { return sumOutput{}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	close(blocker)
	_, awaitErr := future.Await(context.Background())
	if !errors.Is(awaitErr, context.DeadlineExceeded) {
		t.Fatalf("await err = %v", awaitErr)
	}
	assertFutureFailureKind(t, future, FailureDeadlineExhausted)
}

func TestIntegrationFailureHandlerTimeout(t *testing.T) {
	q, queueCtx := testRequestQueue(t)
	blocker := make(chan struct{})
	_ = q.Submit(queueCtx, Job{Key: "block", Lane: "default", Run: func(context.Context) error { <-blocker; return nil }})
	reqCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	future, err := SubmitRequest(reqCtx, q, Request[sumInput, sumOutput]{
		Meta:  RequestMeta{Key: "k", Lane: "default"},
		Input: sumInput{},
		Handle: func(ctx context.Context, _ sumInput) (sumOutput, error) {
			<-ctx.Done()
			return sumOutput{}, ctx.Err()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	close(blocker)
	_, awaitErr := future.Await(context.Background())
	if !errors.Is(awaitErr, context.DeadlineExceeded) {
		t.Fatalf("await err = %v", awaitErr)
	}
	assertFutureFailureKind(t, future, FailureTimeout)
}

func TestIntegrationSubmitValueDeadlineExhaustedWhileQueued(t *testing.T) {
	q, queueCtx := testRequestQueue(t)
	blocker := make(chan struct{})
	_ = q.Submit(queueCtx, Job{Key: "block", Lane: "default", Run: func(context.Context) error { <-blocker; return nil }})
	reqCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	future, err := SubmitValue(reqCtx, q, ValueJob[int]{
		Key:  "k",
		Lane: "default",
		Run:  func(context.Context) (int, error) { return 0, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	close(blocker)
	_, awaitErr := future.Await(context.Background())
	if !errors.Is(awaitErr, context.DeadlineExceeded) {
		t.Fatalf("await err = %v", awaitErr)
	}
	assertFutureFailureKind(t, future, FailureDeadlineExhausted)
}

func TestIntegrationBudgetTraceFromFuture(t *testing.T) {
	q, ctx := testRequestQueue(t)
	future, err := SubmitRequest(ctx, q, Request[sumInput, sumOutput]{
		Meta:  RequestMeta{Key: "k", Lane: "default"},
		Input: sumInput{A: 1, B: 2},
		Handle: func(context.Context, sumInput) (sumOutput, error) {
			return sumOutput{Sum: 3}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, awaitErr := future.Await(context.Background()); awaitErr != nil {
		t.Fatal(awaitErr)
	}
	trace, ok := BudgetTraceFromFuture(future)
	if !ok {
		t.Fatal("BudgetTraceFromFuture: not a result future")
	}
	if trace.AtSubmit.StartedAt.IsZero() {
		t.Fatal("AtSubmit not set")
	}
	if trace.AtAdmission.StartedAt.IsZero() {
		t.Fatal("AtAdmission not set")
	}
	if trace.AfterQueueWait.StartedAt.IsZero() {
		t.Fatal("AfterQueueWait not set")
	}
	if trace.AtHandlerStart.StartedAt.IsZero() {
		t.Fatal("AtHandlerStart not set")
	}
	if trace.AtCompletion.StartedAt.IsZero() {
		t.Fatal("AtCompletion not set")
	}
}

func TestIntegrationFailureFromFutureAny(t *testing.T) {
	q, ctx := testRequestQueue(t)
	future, err := SubmitRequest(ctx, q, Request[sumInput, sumOutput]{
		Meta:  RequestMeta{Key: "k", Lane: "default"},
		Input: sumInput{},
		Handle: func(context.Context, sumInput) (sumOutput, error) {
			return sumOutput{}, errors.New("boom")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, awaitErr := future.Await(context.Background()); awaitErr == nil {
		t.Fatal("expected error")
	}
	failure, ok := FailureFromFutureAny(future)
	if !ok || failure.Kind != FailureUnknown {
		t.Fatalf("failure = %+v ok=%v", failure, ok)
	}
	trace, ok := BudgetTraceFromFutureAny(future)
	if !ok || trace.AtCompletion.StartedAt.IsZero() {
		t.Fatalf("trace = %+v ok=%v", trace, ok)
	}
}

func TestIntegrationFailureHandlerUnknown(t *testing.T) {
	q, ctx := testRequestQueue(t)
	boom := errors.New("boom")
	future, err := SubmitRequest(ctx, q, Request[sumInput, sumOutput]{
		Meta:  RequestMeta{Key: "k", Lane: "default"},
		Input: sumInput{},
		Handle: func(context.Context, sumInput) (sumOutput, error) {
			return sumOutput{}, boom
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, awaitErr := future.Await(context.Background())
	if !errors.Is(awaitErr, boom) {
		t.Fatalf("await err = %v", awaitErr)
	}
	assertFutureFailureKind(t, future, FailureUnknown)
}

func TestIntegrationFailureSuccess(t *testing.T) {
	q, ctx := testRequestQueue(t)
	future, err := SubmitRequest(ctx, q, Request[sumInput, sumOutput]{
		Meta:  RequestMeta{Key: "k", Lane: "default"},
		Input: sumInput{A: 1, B: 2},
		Handle: func(context.Context, sumInput) (sumOutput, error) {
			return sumOutput{Sum: 3}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	out, awaitErr := future.Await(context.Background())
	if awaitErr != nil {
		t.Fatal(awaitErr)
	}
	if out.Sum != 3 {
		t.Fatalf("sum = %d", out.Sum)
	}
	failure, ok := FailureFromFuture(future)
	if !ok || failure.Kind != FailureNone {
		t.Fatalf("failure = %+v ok=%v", failure, ok)
	}
}

func TestIntegrationRequestObservationFailureKind(t *testing.T) {
	q := admissionTestQueue(t)
	fillQueueDepth(t, q, 9)
	err := CheckAdmission(q, AdmissionConfig{Enabled: true, RejectAboveRatio: 0.90}, RequestMeta{Key: "k", Lane: "default"})
	obs := q.newRequestObservation(RequestMeta{Key: "k", Lane: "default"}, 0, 0, 0, err)
	if obs.FailureKind != FailureRejected {
		t.Fatalf("obs.FailureKind = %q", obs.FailureKind)
	}
}
