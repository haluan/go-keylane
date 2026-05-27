// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"sync"
	"testing"
	"time"
)

// newContinuationTestQueue returns a started queue with continuation support enabled.
func newContinuationTestQueue(t *testing.T, ctx context.Context, maxPending int) *Queue {
	t.Helper()
	cfg := newTestConfig()
	cfg.Continuation = ContinuationConfig{
		Enabled:    true,
		MaxPending: maxPending,
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { stopTestQueue(t, q) })
	return q
}

// --- Completer exactly-once ---

func TestContinuationCompleteOnce(t *testing.T) {
	cont, complete := NewContinuation[int](context.Background())
	// First Complete returns true and sends to outcome.
	if !complete.Complete(42) {
		t.Fatal("first Complete should return true")
	}
	// Second Complete returns false (Once already fired).
	if complete.Complete(99) {
		t.Fatal("second Complete should return false")
	}
	// Drain the buffered outcome; should be the first value.
	o := <-cont.outcome
	if o.kind != continuationOutcomeComplete {
		t.Fatalf("expected complete outcome, got %v", o.kind)
	}
	if o.state != 42 {
		t.Fatalf("expected state 42, got %v", o.state)
	}
}

func TestContinuationFailOnce(t *testing.T) {
	cont, complete := NewContinuation[int](context.Background())
	err1 := errPermanentForTest
	if !complete.Fail(err1) {
		t.Fatal("first Fail should return true")
	}
	if complete.Fail(err1) {
		t.Fatal("second Fail should return false")
	}
	o := <-cont.outcome
	if o.kind != continuationOutcomeFail {
		t.Fatalf("expected fail outcome, got %v", o.kind)
	}
	if o.err != err1 {
		t.Fatalf("expected err1, got %v", o.err)
	}
}

func TestContinuationCancelOnce(t *testing.T) {
	cont, complete := NewContinuation[int](context.Background())
	if !complete.Cancel(ErrContinuationCancelled) {
		t.Fatal("first Cancel should return true")
	}
	if complete.Cancel(nil) {
		t.Fatal("second Cancel should return false")
	}
	o := <-cont.outcome
	if o.kind != continuationOutcomeCancel {
		t.Fatalf("expected cancel outcome, got %v", o.kind)
	}
}

func TestContinuationCancelNilErrFallsBack(t *testing.T) {
	cont, complete := NewContinuation[int](context.Background())
	complete.Cancel(nil)
	o := <-cont.outcome
	if o.err != ErrContinuationCancelled {
		t.Fatalf("nil cancel err should use ErrContinuationCancelled, got %v", o.err)
	}
}

func TestContinuationCompleteAfterFailReturnsFalse(t *testing.T) {
	_, complete := NewContinuation[int](context.Background())
	complete.Fail(errPermanentForTest)
	if complete.Complete(1) {
		t.Fatal("Complete after Fail should return false")
	}
}

func TestContinuationCompleteNonBlockingWhenOutcomeFull(t *testing.T) {
	cont, completer := NewContinuation[int](context.Background())
	cont.outcome <- continuationOutcome[int]{kind: continuationOutcomeCancel, err: context.Canceled}

	done := make(chan struct{})
	go func() {
		if completer.Complete(1) {
			t.Error("Complete should return false when outcome channel is full")
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Complete blocked with full outcome channel")
	}
}

func TestContinuationDeliverCallsLateWhenOutcomeFull(t *testing.T) {
	reg := newContinuationRegistry(ContinuationConfig{Enabled: true, MaxPending: 10})
	cont, completer := NewContinuation[int](context.Background())
	cont.outcome <- continuationOutcome[int]{kind: continuationOutcomeCancel, err: context.Canceled}

	var lateCalled bool
	setContinuationLateHandler(completer, nil, StageExecutionContext{}, func() {
		reg.recordLate()
		lateCalled = true
	})
	if completer.Complete(42) {
		t.Fatal("expected false when outcome channel is full")
	}
	if !lateCalled {
		t.Fatal("late handler should run when deliver cannot send")
	}
	if snap := reg.snapshot(); snap.LateCompletions != 1 {
		t.Fatalf("LateCompletions = %d, want 1", snap.LateCompletions)
	}
}

func TestContinuationLateCompleteIgnored(t *testing.T) {
	reg := newContinuationRegistry(ContinuationConfig{Enabled: true, MaxPending: 10})
	cont, completer := NewContinuation[int](context.Background())
	cont.ID = ContinuationID(1)
	var closeOnce sync.Once
	closeDone := func() { closeOnce.Do(func() { close(cont.done) }) }
	if err := reg.register(pendingEntry{
		id: cont.ID, shardID: 0, registeredAt: time.Now(), closeDone: closeDone,
	}); err != nil {
		t.Fatal(err)
	}
	if _, found := reg.resolve(cont.ID, continuationOutcomeCancel); !found {
		t.Fatal("expected registered continuation to resolve")
	}
	setContinuationLateHandler(completer, nil, StageExecutionContext{}, func() { reg.recordLate() })
	if completer.Complete(42) {
		t.Fatal("late Complete should return false")
	}
	if snap := reg.snapshot(); snap.LateCompletions != 1 {
		t.Fatalf("LateCompletions = %d, want 1", snap.LateCompletions)
	}
}

func TestContinuationHooksRespectEnableHooks(t *testing.T) {
	ctx := testTimeout(t)
	cfg := newTestConfig()
	cfg.Continuation = ContinuationConfig{Enabled: true, MaxPending: 10}
	cfg.Observability = DefaultObservabilityConfig()
	cfg.Observability.EnableHooks = false
	var yieldedCount int
	cfg.Observability.Hooks.Request.Continuation = ContinuationHooks{
		OnContinuationYielded: func(ContinuationObservation) { yieldedCount++ },
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Stop(context.Background()) })

	ready := make(chan ContinuationCompleter[pState], 1)
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](context.Background())
					ready <- c
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	c := <-ready
	c.Complete(pState{Val: 1})
	if _, err := future.Await(ctx); err != nil {
		t.Fatal(err)
	}
	if yieldedCount != 0 {
		t.Fatalf("OnContinuationYielded count = %d, want 0 when EnableHooks is false", yieldedCount)
	}
}

// --- Registry capacity ---

func TestContinuationRegistryCapacity(t *testing.T) {
	reg := newContinuationRegistry(ContinuationConfig{Enabled: true, MaxPending: 2})

	makeEntry := func(id ContinuationID) pendingEntry {
		done := make(chan struct{})
		var once sync.Once
		return pendingEntry{
			id:           id,
			shardID:      0,
			registeredAt: time.Now(),
			closeDone:    func() { once.Do(func() { close(done) }) },
		}
	}

	if err := reg.register(makeEntry(1)); err != nil {
		t.Fatalf("register 1: %v", err)
	}
	if err := reg.register(makeEntry(2)); err != nil {
		t.Fatalf("register 2: %v", err)
	}
	if err := reg.register(makeEntry(3)); err != ErrContinuationLimitExceeded {
		t.Fatalf("expected ErrContinuationLimitExceeded, got %v", err)
	}
}

func TestContinuationRegistryConcurrentResolve(t *testing.T) {
	const N = 100
	reg := newContinuationRegistry(ContinuationConfig{Enabled: true, MaxPending: N})

	ids := make([]ContinuationID, N)
	for i := range ids {
		ids[i] = ContinuationID(i + 1)
		done := make(chan struct{})
		var once sync.Once
		entry := pendingEntry{
			id:           ids[i],
			shardID:      0,
			registeredAt: time.Now(),
			closeDone:    func() { once.Do(func() { close(done) }) },
		}
		if err := reg.register(entry); err != nil {
			t.Fatalf("register %d: %v", i, err)
		}
	}

	// Resolve all concurrently.
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id ContinuationID) {
			defer wg.Done()
			reg.resolve(id, continuationOutcomeComplete)
		}(id)
	}
	wg.Wait()

	snap := reg.snapshot()
	if snap.Pending != 0 {
		t.Fatalf("pending = %d, want 0", snap.Pending)
	}
	if snap.Completed != N {
		t.Fatalf("completed = %d, want %d", snap.Completed, N)
	}
}

func TestContinuationRegistryLateResolveCounted(t *testing.T) {
	reg := newContinuationRegistry(ContinuationConfig{Enabled: true, MaxPending: 10})
	// Resolve an ID that was never registered.
	_, found := reg.resolve(ContinuationID(999), continuationOutcomeComplete)
	if found {
		t.Fatal("expected not found for unregistered ID")
	}
	snap := reg.snapshot()
	if snap.LateCompletions != 1 {
		t.Fatalf("late completions = %d, want 1", snap.LateCompletions)
	}
}

// --- Pipeline integration: yield and resume ---

func TestPipelineContinuationYieldsAndResumes(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 10)

	yieldedCh := make(chan ContinuationCompleter[pState], 1)
	var resumeRan bool

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](context.Background())
					yieldedCh <- c
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
			{
				Meta: StageMeta{Name: StageBusiness},
				Run: func(_ context.Context, st pState) (pState, error) {
					resumeRan = true
					st.Val = 42
					return st, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for the stage to yield; receive completer via channel (establishes happens-before).
	var c ContinuationCompleter[pState]
	select {
	case c = <-yieldedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for yield")
	}
	c.Complete(pState{Val: 10})

	out, awaitErr := future.Await(context.Background())
	if awaitErr != nil {
		t.Fatalf("Await: %v", awaitErr)
	}
	if !resumeRan {
		t.Fatal("stage 2 should have run after continuation resumed")
	}
	if out.Sum != 42 {
		t.Fatalf("output = %d, want 42", out.Sum)
	}
}

func TestPipelineContinuationResumesNextStage(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 10)

	var stage2Ran bool
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](context.Background())
					go func() { c.Complete(pState{Val: 7}) }()
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
			{
				Meta: StageMeta{Name: StageBusiness},
				Run: func(_ context.Context, st pState) (pState, error) {
					stage2Ran = true
					st.Val *= 2
					return st, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}

	out, awaitErr := future.Await(context.Background())
	if awaitErr != nil {
		t.Fatalf("Await: %v", awaitErr)
	}
	if !stage2Ran {
		t.Fatal("stage 2 should have run")
	}
	if out.Sum != 14 {
		t.Fatalf("output = %d, want 14 (7*2)", out.Sum)
	}
}

func TestPipelineContinuationPreservesExecutionContext(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 10)

	var resumeExec StageExecutionContext
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "my-key", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](context.Background())
					go func() { c.Complete(st) }()
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
			{
				Meta: StageMeta{Name: StageBusiness},
				Run: func(ctx context.Context, st pState) (pState, error) {
					if exec, ok := StageExecutionFromContext(ctx); ok {
						resumeExec = exec
					}
					return st, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := future.Await(context.Background()); err != nil {
		t.Fatalf("Await: %v", err)
	}
	if resumeExec.Key != "my-key" {
		t.Fatalf("resumed key = %q, want %q", resumeExec.Key, "my-key")
	}
	if resumeExec.Stage.Name != StageBusiness {
		t.Fatalf("resumed stage = %q, want %q", resumeExec.Stage.Name, StageBusiness)
	}
}

func TestPipelineContinuationUsesSameShardIdentity(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 10)

	var yieldShardID, resumeShardID int
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "shard-key", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(ctx context.Context, st pState) (StageResult[pState], error) {
					if exec, ok := StageExecutionFromContext(ctx); ok {
						yieldShardID = exec.ShardID
					}
					cont, c := NewContinuation[pState](context.Background())
					go func() { c.Complete(st) }()
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
			{
				Meta: StageMeta{Name: StageBusiness},
				Run: func(ctx context.Context, st pState) (pState, error) {
					if exec, ok := StageExecutionFromContext(ctx); ok {
						resumeShardID = exec.ShardID
					}
					return st, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := future.Await(context.Background()); err != nil {
		t.Fatalf("Await: %v", err)
	}
	if yieldShardID != resumeShardID {
		t.Fatalf("shardID changed across yield/resume: yield=%d resume=%d", yieldShardID, resumeShardID)
	}
}

func TestPipelineContinuationDoesNotRequireSameWorker(t *testing.T) {
	ctx := testTimeout(t)
	cfg := newTestConfig()
	cfg.WorkerCount = 2
	cfg.Continuation = ContinuationConfig{Enabled: true, MaxPending: 10}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Stop(context.Background()) })

	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	if err := q.Submit(ctx, Job{
		Key: "blocker", Lane: "default",
		Run: func(context.Context) error { <-hold; return nil },
	}); err != nil {
		t.Fatal(err)
	}

	ready := make(chan ContinuationCompleter[pState], 1)
	var resumeStageRan bool
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "resume-key", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](context.Background())
					ready <- c
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
			{
				Meta: StageMeta{Name: StageBusiness},
				Run: func(_ context.Context, st pState) (pState, error) {
					resumeStageRan = true
					return st, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}
	(<-ready).Complete(pState{Val: 3})
	if _, err := future.Await(ctx); err != nil {
		t.Fatalf("Await: %v", err)
	}
	if !resumeStageRan {
		t.Fatal("resume stage did not run on a separate scheduler job")
	}
}

func TestPipelineContinuationCompletesFutureOnce(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 10)

	yieldedCh := make(chan ContinuationCompleter[pState], 1)
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](context.Background())
					yieldedCh <- c
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}

	var completer ContinuationCompleter[pState]
	select {
	case completer = <-yieldedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for yield")
	}
	completer.Complete(pState{Val: 1})

	_, err1 := future.Await(context.Background())
	if err1 != nil {
		t.Fatalf("first Await: %v", err1)
	}

	// Call Complete a second time; future must not change.
	completer.Complete(pState{Val: 99})
	out, err2 := future.Await(context.Background())
	if err2 != nil {
		t.Fatalf("second Await: %v", err2)
	}
	if out.Sum != 1 {
		t.Fatalf("output changed after second Complete: got %d", out.Sum)
	}
}

func TestPipelineContinuationFailureCompletesFuture(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 10)

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](context.Background())
					go func() { c.Fail(errPermanentForTest) }()
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, awaitErr := future.Await(context.Background())
	if awaitErr == nil {
		t.Fatal("expected error from Fail")
	}
}

func TestPipelineContinuationStageFailureAttribution(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 10)

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageDBRead},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](context.Background())
					go func() { c.Fail(errPermanentForTest) }()
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, awaitErr := future.Await(context.Background())
	if awaitErr == nil {
		t.Fatal("expected error")
	}

	sf, ok := AsStageFailure(awaitErr)
	if !ok {
		t.Fatalf("expected StageFailure, got %T: %v", awaitErr, awaitErr)
	}
	if sf.Stage.Name != StageDBRead {
		t.Fatalf("stage name = %q, want %q", sf.Stage.Name, StageDBRead)
	}
}

func TestPipelineContinuationSyncStageBeforeYield(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 10)

	var syncRan bool
	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				Run: func(_ context.Context, st pState) (pState, error) {
					syncRan = true
					st.Val = 5
					return st, nil
				},
			},
			{
				Meta: StageMeta{Name: StageDBRead},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](context.Background())
					go func() {
						c.Complete(pState{Val: st.Val * 2})
					}()
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}

	out, awaitErr := future.Await(context.Background())
	if awaitErr != nil {
		t.Fatalf("Await: %v", awaitErr)
	}
	if !syncRan {
		t.Fatal("sync stage before yield should have run")
	}
	if out.Sum != 10 {
		t.Fatalf("output = %d, want 10", out.Sum)
	}
}

func TestPipelineContinuationSyncCompleteAfterYield(t *testing.T) {
	ctx := testTimeout(t)
	q := newContinuationTestQueue(t, ctx, 10)

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageDBRead},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					// RunContinuation returning nil Continuation = synchronous completion
					return StageResult[pState]{State: pState{Val: 99}}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}

	out, awaitErr := future.Await(context.Background())
	if awaitErr != nil {
		t.Fatalf("Await: %v", awaitErr)
	}
	if out.Sum != 99 {
		t.Fatalf("output = %d, want 99", out.Sum)
	}
}

func TestPipelineContinuationDisabledReturnsError(t *testing.T) {
	ctx := testTimeout(t)
	// Queue without continuation enabled.
	q := newStartedTestQueue(t, ctx)

	future, err := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k", Lane: "default"},
		Stages: []PipelineStage[pState]{
			{
				Meta: StageMeta{Name: StageValidate},
				RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
					cont, c := NewContinuation[pState](context.Background())
					go func() { c.Complete(st) }()
					return StageResult[pState]{Continuation: cont}, nil
				},
			},
		},
		Complete: validPipelineComplete(),
	})
	if err != ErrContinuationDisabled {
		t.Fatalf("expected ErrContinuationDisabled at submit, got %v (future err: %v)", err, func() error {
			_, e := future.Await(context.Background())
			return e
		}())
	}
}

func TestPipelineContinuationLimitExceededReturnsStageFailure(t *testing.T) {
	ctx := testTimeout(t)
	// MaxPending=0 with Enabled=true means no global cap. Use MaxPending=1 to force limit.
	cfg := newTestConfig()
	cfg.Continuation = ContinuationConfig{Enabled: true, MaxPending: 1}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}

	var completer1 ContinuationCompleter[pState]
	var mu sync.Mutex

	// First pipeline fills the registry slot.
	_, err = SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k1", Lane: "default"},
		Stages: []PipelineStage[pState]{{
			Meta: StageMeta{Name: StageValidate},
			RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
				cont, c := NewContinuation[pState](context.Background())
				mu.Lock()
				completer1 = c
				mu.Unlock()
				return StageResult[pState]{Continuation: cont}, nil
			},
		}},
		Complete: validPipelineComplete(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wait until the first continuation is registered.
	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return completer1 != nil
	}, 2*time.Second)

	var completer2 ContinuationCompleter[pState]
	// Second pipeline should fail: limit exceeded at register time.
	future2, err2 := SubmitPipeline(ctx, q, Pipeline[pState, pOutput]{
		Meta: RequestMeta{Key: "k2", Lane: "default"},
		Stages: []PipelineStage[pState]{{
			Meta: StageMeta{Name: StageValidate},
			RunContinuation: func(_ context.Context, st pState) (StageResult[pState], error) {
				cont, c := NewContinuation[pState](context.Background())
				mu.Lock()
				completer2 = c
				mu.Unlock()
				return StageResult[pState]{Continuation: cont}, nil
			},
		}},
		Complete: validPipelineComplete(),
	})
	if err2 != nil {
		t.Fatalf("unexpected submit error: %v", err2)
	}

	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return completer2 != nil
	}, 2*time.Second)

	_, awaitErr := future2.Await(context.Background())
	if awaitErr == nil {
		t.Fatal("expected error for limit exceeded")
	}

	if completer2.Complete(pState{Val: 99}) {
		t.Fatal("Complete after registry rejection should return false")
	}
	snap := q.DebugSnapshot().Continuation
	if snap.LateCompletions != 1 {
		t.Fatalf("LateCompletions = %d, want 1", snap.LateCompletions)
	}

	// Clean up: resolve the first continuation.
	completer1.Complete(pState{})
}

// waitUntil polls fn until it returns true or the deadline expires.
func waitUntil(t *testing.T, fn func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if fn() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timeout in waitUntil")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// errPermanentForTest is a test-only permanent error.
var errPermanentForTest = PermanentFailure(errContinuationTestSentinel{})

type errContinuationTestSentinel struct{}

func (errContinuationTestSentinel) Error() string { return "continuation test: permanent failure" }
