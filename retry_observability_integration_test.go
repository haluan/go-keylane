// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func obsRetryQueue(t *testing.T, cfg Config) (*Queue, context.Context) {
	t.Helper()
	return obsRetryQueueWithRequestHooks(t, cfg, RequestHooks{})
}

func fillLanesForGlobalPressure(t *testing.T, q *Queue, ctx context.Context, hold <-chan struct{}, perLane int) {
	t.Helper()
	run := func(context.Context) error { <-hold; return nil }
	for i := 0; i < perLane; i++ {
		if err := q.Submit(ctx, Job{Key: fmt.Sprintf("fill-d-%d", i), Lane: "default", Run: run}); err != nil {
			t.Fatalf("fill default %d: %v", i, err)
		}
		if err := q.Submit(ctx, Job{Key: fmt.Sprintf("fill-c-%d", i), Lane: "critical", Run: run}); err != nil {
			t.Fatalf("fill critical %d: %v", i, err)
		}
	}
}

func obsRetryQueueWithRequestHooks(t *testing.T, cfg Config, hooks RequestHooks) (*Queue, context.Context) {
	t.Helper()
	cfg.Observability = DefaultObservabilityConfig()
	cfg.Observability.EnableHooks = true
	cfg.Observability.Hooks.Request = hooks
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Stop(context.Background()) })
	return q, ctx
}

func TestIntegrationObsRetrySuccessAfterOneRetry(t *testing.T) {
	q, ctx := obsRetryQueue(t, Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 1},
		Retry:      RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0},
	})
	var n atomic.Int32
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default", Idempotency: retrySafeIdempotency(),
		Run: func(context.Context) (int, error) {
			if n.Add(1) < 2 {
				return 0, RetryableFailure(errors.New("t"))
			}
			return 7, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, awaitErr := future.Await(context.Background()); awaitErr != nil {
		t.Fatal(awaitErr)
	}
	trace, ok := RetryTraceFromFuture(future)
	if !ok || !trace.Final.Succeeded {
		t.Fatalf("ok=%v trace=%+v", ok, trace)
	}
	if q.RetryFailureSnapshot().RetriesScheduledTotal < 1 {
		t.Fatal("expected scheduled counter")
	}
}

func TestIntegrationObsSubmitRequestPreservesRequestObservation(t *testing.T) {
	spy := newRequestHookSpy()
	var completedCount atomic.Int32
	hooks := spy.hooks()
	prevCompleted := hooks.OnCompleted
	hooks.OnCompleted = func(obs RequestObservation) {
		completedCount.Add(1)
		if prevCompleted != nil {
			prevCompleted(obs)
		}
	}
	meta := RequestMeta{
		RequestID: "rid-retry",
		Key:       "tenant-retry",
		Lane:      "default",
		Transport: "worker",
		Operation: "charge",
	}
	q, ctx := obsRetryQueueWithRequestHooks(t, Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 1},
		Retry:      RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0},
	}, hooks)
	var n atomic.Int32
	future, err := SubmitRequest(ctx, q, Request[struct{}, int]{
		Meta:        meta,
		Idempotency: retrySafeIdempotency(),
		Handle: func(context.Context, struct{}) (int, error) {
			if n.Add(1) < 2 {
				return 0, RetryableFailure(errors.New("t"))
			}
			return 3, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, awaitErr := future.Await(context.Background()); awaitErr != nil {
		t.Fatal(awaitErr)
	}
	trace, ok := RetryTraceFromFuture(future)
	if !ok || !trace.Final.Succeeded {
		t.Fatalf("trace=%+v ok=%v", trace, ok)
	}
	if q.RetryFailureSnapshot().RetriesScheduledTotal < 1 {
		t.Fatal("expected scheduled retry counter")
	}

	queued := waitRequestMeta(t, spy.queued)
	assertRequestMetaEqual(t, queued, meta)

	started := waitRequestObservation(t, spy.started)
	assertObservationRouting(t, started, meta.Key, meta.Lane)
	if started.RequestID != meta.RequestID {
		t.Errorf("started RequestID = %q, want %q", started.RequestID, meta.RequestID)
	}
	if started.ShardID != q.ShardIDForKey(meta.Key) {
		t.Errorf("ShardID = %d, want %d", started.ShardID, q.ShardIDForKey(meta.Key))
	}
	if started.QueueWait <= 0 {
		t.Errorf("QueueWait = %v, want > 0", started.QueueWait)
	}

	completed := waitRequestObservation(t, spy.completed)
	if completedCount.Load() != 1 {
		t.Fatalf("OnCompleted calls = %d, want 1", completedCount.Load())
	}
	assertObservationRouting(t, completed, meta.Key, meta.Lane)
	if completed.RequestID != meta.RequestID {
		t.Errorf("completed RequestID = %q, want %q", completed.RequestID, meta.RequestID)
	}
	if completed.ShardID != q.ShardIDForKey(meta.Key) {
		t.Errorf("completed ShardID = %d, want %d", completed.ShardID, q.ShardIDForKey(meta.Key))
	}
	if completed.Outcome != RequestOutcomeCompleted {
		t.Errorf("Outcome = %q, want completed", completed.Outcome)
	}
	if completed.Err != nil {
		t.Errorf("Err = %v, want nil", completed.Err)
	}
	if completed.FailureKind != FailureNone {
		t.Errorf("FailureKind = %q, want none", completed.FailureKind)
	}
	if completed.QueueWait <= 0 {
		t.Errorf("QueueWait = %v, want > 0", completed.QueueWait)
	}
	if completed.Run <= 0 {
		t.Errorf("Run = %v, want > 0", completed.Run)
	}
	select {
	case obs := <-spy.completed:
		t.Fatalf("unexpected second OnCompleted: %+v", obs)
	default:
	}
}

func TestIntegrationObsSubmitRequestFailedClassifiesFailureKind(t *testing.T) {
	spy := newRequestHookSpy()
	meta := RequestMeta{
		RequestID: "rid-fail",
		Key:       "tenant-fail",
		Lane:      "default",
		Transport: "worker",
		Operation: "charge",
	}
	handlerErr := errors.New("perm")
	q, ctx := obsRetryQueueWithRequestHooks(t, Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 1},
		Retry:      RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0},
	}, spy.hooks())
	future, err := SubmitRequest(ctx, q, Request[struct{}, int]{
		Meta:        meta,
		Idempotency: retrySafeIdempotency(),
		Handle: func(context.Context, struct{}) (int, error) {
			return 0, PermanentFailure(handlerErr)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, awaitErr := future.Await(context.Background()); awaitErr == nil {
		t.Fatal("expected permanent failure")
	}
	fail, ok := FailureFromFuture(future)
	if !ok || fail.Kind != FailurePermanent {
		t.Fatalf("failure=%+v ok=%v", fail, ok)
	}

	completed := waitRequestObservation(t, spy.completed)
	assertObservationRouting(t, completed, meta.Key, meta.Lane)
	if completed.RequestID != meta.RequestID {
		t.Errorf("RequestID = %q, want %q", completed.RequestID, meta.RequestID)
	}
	if completed.Outcome != RequestOutcomeFailed {
		t.Errorf("Outcome = %q, want failed", completed.Outcome)
	}
	if completed.FailureKind != FailurePermanent {
		t.Errorf("FailureKind = %q, want permanent", completed.FailureKind)
	}
	if !errors.Is(completed.Err, handlerErr) {
		t.Errorf("Err = %v, want %v", completed.Err, handlerErr)
	}
	snap := q.RetryFailureSnapshot()
	if countFailureKind(snap, FailurePermanent) < 1 {
		t.Fatalf("snap=%+v", snap)
	}
	select {
	case obs := <-spy.completed:
		t.Fatalf("unexpected second OnCompleted: %+v", obs)
	default:
	}
}

func TestIntegrationObsPermanentFailureNoSchedule(t *testing.T) {
	q, ctx := obsRetryQueue(t, Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 1},
		Retry:      RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0},
	})
	before := q.RetryFailureSnapshot().RetriesScheduledTotal
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default", Idempotency: retrySafeIdempotency(),
		Run: func(context.Context) (int, error) {
			return 0, PermanentFailure(errors.New("perm"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = future.Await(context.Background())
	trace, ok := RetryTraceFromFuture(future)
	if !ok || trace.Final.StoppedReason != RetryDecisionPermanentFailure {
		t.Fatalf("ok=%v final=%+v", ok, trace.Final)
	}
	if q.RetryFailureSnapshot().RetriesScheduledTotal != before {
		t.Fatal("unexpected scheduled retry")
	}
}

func TestIntegrationObsRetryStormSuppressed(t *testing.T) {
	q, ctx := obsRetryQueue(t, Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas:       map[Lane]int{"default": 1},
		Retry:            RetryPolicy{Enabled: true, MaxAttempts: 5, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0},
		RetrySuppression: RetrySuppressionPolicy{Enabled: true, SuppressWhenOverloaded: true},
	})
	hold := make(chan struct{})
	defer close(hold)
	var effects atomic.Int32
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default", Idempotency: retrySafeIdempotency(),
		Run: func(context.Context) (int, error) {
			if effects.Add(1) == 1 {
				fillQueueForOverload(t, q, ctx, hold, 9)
			}
			return 0, RetryableFailure(errors.New("t"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = future.Await(context.Background())
	if effects.Load() != 1 {
		t.Fatalf("effects=%d", effects.Load())
	}
	trace, ok := RetryTraceFromFuture(future)
	if !ok || !trace.HadSuppression(RetrySuppressionGlobalOverload) {
		t.Fatalf("trace=%+v", trace)
	}
	snap := q.RetryFailureSnapshot()
	if snap.RetriesSuppressedTotal < 1 {
		t.Fatal("expected suppressed counter")
	}
	if suppressionReasonCount(snap, RetrySuppressionGlobalOverload) < 1 {
		t.Fatalf("snap=%+v", snap)
	}
}

func TestIntegrationObsBestEffortLanePressureSuppresses(t *testing.T) {
	q, ctx := obsRetryQueue(t, Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 1},
		Retry:      RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0},
		RetrySuppression: RetrySuppressionPolicy{
			Enabled: true, SuppressNonCriticalWhenPressured: true, SuppressWhenOverloaded: false,
		},
	})
	if _, err := q.UpdateAdmissionPolicy(AdmissionPolicy{
		DefaultClass: LaneBestEffort, DefaultRejectAboveRatio: 0.9, DefaultMaxQueueDepth: 100,
	}); err != nil {
		t.Fatal(err)
	}
	hold := make(chan struct{})
	defer close(hold)
	var effects atomic.Int32
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default", Idempotency: retrySafeIdempotency(),
		Run: func(context.Context) (int, error) {
			if effects.Add(1) == 1 {
				fillQueueForOverload(t, q, ctx, hold, 7)
			}
			return 0, RetryableFailure(errors.New("t"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = future.Await(context.Background())
	if effects.Load() != 1 {
		t.Fatalf("effects=%d", effects.Load())
	}
	trace, ok := RetryTraceFromFuture(future)
	if !ok {
		t.Fatal("missing trace")
	}
	if trace.Final.SuppressionReason != RetrySuppressionGlobalPressure {
		t.Fatalf("final suppression = %q, want global_pressure; trace=%+v pressure=%+v",
			trace.Final.SuppressionReason, trace, q.Pressure())
	}
	if !trace.HadSuppression(RetrySuppressionGlobalPressure) {
		t.Fatalf("trace=%+v", trace)
	}
	if suppressionReasonCount(q.RetryFailureSnapshot(), RetrySuppressionGlobalPressure) < 1 {
		t.Fatalf("snap = %+v", q.RetryFailureSnapshot())
	}
	if q.RetryFailureSnapshot().RetriesSuppressedTotal < 1 {
		t.Fatal("expected suppressed counter")
	}
}

func TestIntegrationObsCriticalLaneBoundedUnderPressure(t *testing.T) {
	const maxAttempts = 4
	q, ctx := obsRetryQueue(t, Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"critical": 1, "default": 1},
		Retry: RetryPolicy{
			Enabled: true, MaxAttempts: maxAttempts, InitialBackoff: time.Millisecond,
			Jitter: false, MinRemainingBudget: 0,
		},
		RetrySuppression: RetrySuppressionPolicy{
			Enabled: true, SuppressNonCriticalWhenPressured: true, SuppressWhenOverloaded: false,
			SuppressLaneAboveRatio: 0.99, SuppressShardAboveRatio: 0.99,
		},
	})
	if _, err := q.UpdateAdmissionPolicy(AdmissionPolicy{
		DefaultClass: LaneBestEffort, DefaultRejectAboveRatio: 0.9, DefaultMaxQueueDepth: 100,
		Lanes: []LanePolicy{
			{Lane: "critical", Class: LaneCritical, RejectAboveRatio: 0.98, MaxQueueDepth: 100},
		},
	}); err != nil {
		t.Fatal(err)
	}

	hold := make(chan struct{})
	var criticalEffects atomic.Int32
	var pressured atomic.Bool
	criticalFuture, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "critical-k", Lane: "critical", Idempotency: retrySafeIdempotency(),
		Run: func(context.Context) (int, error) {
			if criticalEffects.Add(1) == 1 {
				fillLanesForGlobalPressure(t, q, ctx, hold, 7)
				p := q.Pressure()
				if p.IsPressured && !p.IsOverloaded {
					pressured.Store(true)
				}
			}
			return 0, RetryableFailure(errors.New("t"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = criticalFuture.Await(context.Background())
	close(hold) // unblock fill workers before best-effort contrast (same queue, fresh hold)
	if !pressured.Load() {
		t.Fatalf("expected global pressure during critical retry, pressure=%+v", q.Pressure())
	}
	criticalTrace, ok := RetryTraceFromFuture(criticalFuture)
	if !ok {
		t.Fatal("missing critical trace")
	}
	if criticalEffects.Load() < 2 || criticalEffects.Load() > maxAttempts {
		t.Fatalf("critical effects=%d want 2..%d under pressure", criticalEffects.Load(), maxAttempts)
	}
	if criticalTrace.Final.SuppressionReason == RetrySuppressionGlobalPressure {
		t.Fatalf("critical lane should retry under pressure, trace=%+v", criticalTrace)
	}
	if q.RetryFailureSnapshot().RetriesScheduledTotal < 1 {
		t.Fatal("expected scheduled retries for critical lane")
	}

	// Best-effort contrast: same policy/queue, pressure created during first handler (not after drain).
	holdBE := make(chan struct{})
	t.Cleanup(func() { close(holdBE) })
	var beEffects atomic.Int32
	beFuture, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "be-k", Lane: "default", Idempotency: retrySafeIdempotency(),
		Run: func(context.Context) (int, error) {
			if beEffects.Add(1) == 1 {
				fillLanesForGlobalPressure(t, q, ctx, holdBE, 7)
			}
			return 0, RetryableFailure(errors.New("t"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = beFuture.Await(context.Background())
	beTrace, ok := RetryTraceFromFuture(beFuture)
	if !ok {
		t.Fatal("missing best-effort trace")
	}
	if beEffects.Load() != 1 {
		t.Fatalf("best-effort effects=%d want 1 when pressured", beEffects.Load())
	}
	if beTrace.Final.SuppressionReason != RetrySuppressionGlobalPressure {
		t.Fatalf("best-effort final=%+v want global_pressure", beTrace.Final)
	}
	if suppressionReasonCount(q.RetryFailureSnapshot(), RetrySuppressionGlobalPressure) < 1 {
		t.Fatalf("snap=%+v", q.RetryFailureSnapshot())
	}
}

func TestIntegrationObsHotKeySuppressionRecordsReason(t *testing.T) {
	q, ctx := obsRetryQueue(t, Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 32,
		LaneQuotas: map[Lane]int{"default": 1},
		HotKey: HotKeyConfig{
			Enabled: true, MaxTrackedKeysPerShard: 16, DetectionWindow: time.Minute,
			HotKeyDepthRatio: 0.2, HotKeyWaitRatio: 0.5,
		},
		Retry:            RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0},
		RetrySuppression: RetrySuppressionPolicy{Enabled: true, SuppressHotKeyRetry: true},
	})
	if _, err := q.UpdateAdmissionPolicy(AdmissionPolicy{
		DefaultClass: LaneBackground, DefaultRejectAboveRatio: 0.98, DefaultMaxQueueDepth: 200,
	}); err != nil {
		t.Fatal(err)
	}
	hot := "hot-obs"
	for i := 0; i < 20; i++ {
		if err := q.Submit(ctx, Job{Key: hot, Lane: "default", Run: func(context.Context) error { return nil }}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(30 * time.Millisecond)
	snap := q.RetrySuppressionSnapshot(hot, "default", q.ShardIDForKey(hot))
	if !snap.HotKeyCandidate {
		t.Fatalf("expected hot key candidate before retry, snap=%+v", snap)
	}
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: hot, Lane: "default", Idempotency: retrySafeIdempotency(),
		Run: func(context.Context) (int, error) {
			return 0, RetryableFailure(errors.New("t"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = future.Await(context.Background())
	trace, ok := RetryTraceFromFuture(future)
	if !ok || !trace.HadSuppression(RetrySuppressionHotKey) {
		t.Fatalf("trace=%+v", trace)
	}
	failSnap := q.RetryFailureSnapshot()
	if failSnap.RetriesSuppressedTotal < 1 {
		t.Fatal("expected suppressed counter")
	}
	if suppressionReasonCount(failSnap, RetrySuppressionHotKey) < 1 {
		t.Fatalf("snap=%+v", failSnap)
	}
}

func TestIntegrationObsDeadlineStopsBeforeRetrySleep(t *testing.T) {
	submitDeadline := time.Now().Add(40 * time.Millisecond)
	submitCtx, submitCancel := context.WithDeadline(context.Background(), submitDeadline)
	defer submitCancel()
	q, ctx := obsRetryQueue(t, Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 1},
		Retry: RetryPolicy{
			Enabled: true, MaxAttempts: 5, InitialBackoff: 50 * time.Millisecond,
			Jitter: false, MinRemainingBudget: 25 * time.Millisecond,
		},
	})

	future, err := SubmitValue(submitCtx, q, ValueJob[int]{
		Key: "k", Lane: "default", Idempotency: retrySafeIdempotency(),
		Run: func(context.Context) (int, error) {
			return 0, RetryableFailure(errors.New("t"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = future.Await(ctx)
	trace, ok := RetryTraceFromFuture(future)
	if !ok {
		t.Fatal("missing trace")
	}
	if trace.Final.StoppedReason != RetryDecisionDeadlineExhausted &&
		trace.Final.StoppedReason != RetryDecisionBudgetTooSmall {
		t.Fatalf("final=%+v want deadline stop not cancellation", trace.Final)
	}
	if trace.Final.StoppedReason == RetryDecisionContextCancelled {
		t.Fatalf("final=%+v", trace.Final)
	}
	snap := q.RetryFailureSnapshot()
	if snap.RetryDeadlineStoppedTotal < 1 {
		t.Fatalf("RetryDeadlineStoppedTotal = %d", snap.RetryDeadlineStoppedTotal)
	}
}

func TestIntegrationObsIdempotencyUnsafeSuppresses(t *testing.T) {
	q, ctx := obsRetryQueue(t, Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{"default": 1},
		Retry:      RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0},
	})
	var effects atomic.Int32
	future, err := SubmitValue(ctx, q, ValueJob[int]{
		Key: "k", Lane: "default", Idempotency: Idempotency{Safety: RetrySafetyUnsafe},
		Run: func(context.Context) (int, error) {
			effects.Add(1)
			return 0, RetryableFailure(errors.New("t"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = future.Await(context.Background())
	if effects.Load() != 1 {
		t.Fatalf("effects=%d", effects.Load())
	}
	trace, ok := RetryTraceFromFuture(future)
	if !ok || trace.Final.SafetyReason != RetrySafetyDecisionUnsafe {
		t.Fatalf("trace=%+v", trace)
	}
	snap := q.RetryFailureSnapshot()
	if snap.RetrySafetySuppressedTotal < 1 {
		t.Fatal("expected safety suppressed counter")
	}
	if safetyReasonCount(snap, RetrySafetyDecisionUnsafe) < 1 {
		t.Fatalf("snap=%+v", snap)
	}
}

func TestIntegrationObsConcurrentSnapshotsRace(t *testing.T) {
	q, ctx := obsRetryQueue(t, Config{
		ShardCount: 2, WorkerCount: 2, QueueSizePerLane: 32,
		LaneQuotas: map[Lane]int{"default": 2},
		Retry:      RetryPolicy{Enabled: true, MaxAttempts: 2, InitialBackoff: time.Millisecond, Jitter: false, MinRemainingBudget: 0},
	})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, _ = SubmitValue(ctx, q, ValueJob[int]{
					Key: "k", Lane: "default", Idempotency: retrySafeIdempotency(),
					Run: func(context.Context) (int, error) {
						return 0, RetryableFailure(errors.New("t"))
					},
				})
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = q.RetryFailureSnapshot()
			}
		}()
	}
	wg.Wait()
}
