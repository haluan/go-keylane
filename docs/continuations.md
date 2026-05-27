# Non-Blocking Continuations (v0.7 — KL-1703)

Continuations extend `SubmitPipeline` so a stage can release the Keylane worker while waiting for slow I/O — a database read, an external API call, a cache miss — and resume the pipeline later through the same key/lane fairness path.

---

## The problem continuations solve

Without continuations, every pipeline stage runs synchronously inside a worker goroutine. A stage that calls a remote service holds the worker for the full round-trip, blocking other queued requests from running on that shard. Under heavy load this causes worker starvation and tail-latency amplification.

Continuations make the slow-I/O pattern explicit: a stage starts the operation, hands the completion handle to the caller, and immediately releases the worker. When the operation finishes the caller drives the pipeline forward via a `ContinuationCompleter`.

---

## Yield/resume lifecycle

```
SubmitPipeline(ctx, q, pipeline)
    │
    ├─ [sync stages run inline, same as before]
    │
    └─ RunContinuation stage called
           │
           ├─ Stage calls NewContinuation, starts async op, returns Continuation
           │
           └─ Worker released ──────────────────────────────────┐
                                                                │ (async op in flight)
                                                                │
                              Caller calls completer.Complete() ┘
                                     │
                              Resume job enqueued to same shard via q.sched
                                     │
                              Worker picks up resume job
                                     │
                              Remaining stages run, Complete() called
                                     │
                              Future resolved
```

Shard identity is preserved logically: the resume job is enqueued using the same key hash, so it lands on the same shard and respects the same per-key admission and lane fairness as the original request. Physical worker identity is not preserved and does not need to be.

---

## Enabling continuations

Continuation support is opt-in per queue:

```go
cfg := keylane.Config{
    // ...
    Continuation: keylane.ContinuationConfig{
        Enabled:            true,
        MaxPending:         500,           // global cap; 0 applies DefaultContinuationMaxPending (256)
        MaxPendingPerShard: 50,            // per-shard cap; 0 = no per-shard cap
    },
}
```

If `Continuation.Enabled` is false, submitting a pipeline with a `RunContinuation` stage returns `ErrContinuationDisabled` immediately at submit time.

---

## Writing a continuation stage

Exactly one of `Run` or `RunContinuation` must be set per stage. A stage with `RunContinuation` may either yield (return a non-nil `Continuation`) or complete synchronously (return a nil `Continuation`):

```go
keylane.PipelineStage[MyState]{
    Meta: keylane.StageMeta{Name: keylane.StageDBRead},
    RunContinuation: func(ctx context.Context, st MyState) (keylane.StageResult[MyState], error) {
        cont, completer := keylane.NewContinuation[MyState](ctx)

        go func() {
            result, err := db.QueryContext(ctx, ...)
            if err != nil {
                completer.Fail(err)
                return
            }
            completer.Complete(MyState{Data: result})
        }()

        // Returning cont != nil yields the worker.
        return keylane.StageResult[MyState]{Continuation: cont}, nil
    },
},
```

To complete synchronously instead:

```go
RunContinuation: func(ctx context.Context, st MyState) (keylane.StageResult[MyState], error) {
    // If the data is already available, return it without yielding.
    return keylane.StageResult[MyState]{State: st}, nil
},
```

---

## ContinuationCompleter

`NewContinuation` returns a `*Continuation` (for returning from the stage) and a `ContinuationCompleter` (for driving resolution):

```go
type ContinuationCompleter[S any] interface {
    Complete(state S) bool   // pipeline resumes with state
    Fail(err error) bool     // pipeline fails at this stage
    Cancel(err error) bool   // pipeline cancelled
}
```

All three methods are exactly-once: the first call wins and returns `true`; subsequent calls are no-ops that return `false`. This means it is safe to call `Complete` and `Fail` concurrently from multiple goroutines — only one will take effect.

---

## Deadline and cancellation semantics

### Request context cancellation while yielded

The resolution goroutine started by the runtime watches `reqCtx` (the request context passed to `SubmitPipeline`). When it fires:

1. The continuation is resolved as cancelled.
2. The future is completed with a cancellation error.
3. Subsequent calls to `Complete` or `Fail` on the completer are no-ops (exactly-once guard).

The underlying async operation is **not** cancelled — the continuation runtime has no way to interrupt it. If the operation should stop, the stage is responsible for propagating `ctx.Done()` to it (e.g., by passing `ctx` to `db.QueryContext`).

### Await context cancellation

Calling `future.Await(awaitCtx)` with a cancelled context cancels the *wait*, not the request. The continuation stays pending; calling `completer.Complete` later still resolves the pipeline and completes the future.

### Late completions after cancellation

If `Complete`, `Fail`, or `Cancel` is called after the continuation was already resolved (cancel, deadline, or an earlier completer call), the call returns `false`, the future is unchanged, `ContinuationSnapshot.LateCompletions` increments, and `OnContinuationLate` may fire when configured.

---

## Retry integration

Continuation failures use the same retry path as synchronous pipelines (`DecideRetry`, idempotency safety, suppression, backoff, and `MinRemainingBudget`). When a continuation stage fails (via `completer.Fail`) and retry is allowed:

- The entire pipeline re-runs from stage 0.
- Attempt count is incremented.
- `priorRuntime` accumulates elapsed time including yield duration, so deadline budget is correctly charged across retries.
- `RetryTraceFromFuture` records attempts and suppression reasons on the pipeline future.

Retries are suppressed when idempotency is unsafe, suppression policy blocks retry under pressure, or the error is permanent. There is no per-stage retry in KL-1703; retry always re-runs the full pipeline.

---

## Goroutine lifecycle

Each yielded continuation starts exactly one resolution goroutine. The goroutine selects on two channels:

```go
select {
case <-reqCtx.Done():      // deadline or cancellation
    // resolve as cancel
case o := <-cont.outcome:  // completer.Complete/Fail/Cancel called
    // check reqCtx.Err(), then dispatch
}
```

The `cont.outcome` channel is buffered (capacity 1), so the completer never blocks. `cont.done` is closed by the registry exactly once, on any resolution. The goroutine exits after the select — there is no leak regardless of which branch fires.

---

## Registry and observability

### Capacity enforcement

The registry enforces `MaxPending` and `MaxPendingPerShard`. Exceeding either limit causes `ErrContinuationLimitExceeded` to be returned from the yielding stage as a `StageFailure`.

### Debug snapshot

```go
snap := q.DebugSnapshot()
snap.Continuation.Pending         // currently yielded continuations
snap.Continuation.MaxPending      // configured cap
snap.Continuation.Completed       // resolved via Complete
snap.Continuation.Failed          // resolved via Fail
snap.Continuation.Cancelled       // resolved via Cancel or deadline
snap.Continuation.LateCompletions // calls after already resolved
snap.Continuation.ResumeRejected  // resume enqueue failures
```

Per-shard breakdown is in `snap.ContinuationPerShard`.

### Hooks

```go
cfg.Observability.Hooks.Request.Continuation = keylane.ContinuationHooks{
    OnContinuationYielded:   func(obs keylane.ContinuationObservation) { /* stage yielded */ },
    OnContinuationCompleted: func(obs keylane.ContinuationObservation) { /* completer accepted */ },
    OnContinuationResumed:   func(obs keylane.ContinuationObservation) { /* resume job started */ },
    OnContinuationFailed:    func(obs keylane.ContinuationObservation) { /* Fail called */ },
    OnContinuationCancelled: func(obs keylane.ContinuationObservation) { /* Cancel or deadline */ },
    OnContinuationLate:      func(obs keylane.ContinuationObservation) { /* late Complete ignored */ },
}
```

Hook order on the happy path: `OnContinuationYielded` → `OnContinuationCompleted` → `OnContinuationResumed` (see [pipeline-observability.md](pipeline-observability.md#continuation-stage)).

`ContinuationObservation` includes `YieldedFor` (time from yield to resolution) and `ResumeQueueWait` (time from resume enqueue to resume job start).

Hooks respect `Observability.EnableHooks` like other request hooks; callbacks are not invoked when hooks are disabled.

---

## Examples

KL-1703 provides yield/resume. For bounded downstream usage, use [backend resource coordination](backend-resource-coordination.md) (KL-1704) and release backend leases in the completer goroutine (`defer lease.Release()`). Concrete pool adapters are KL-1705.

### Slow DB read (yielded)

```go
RunContinuation: func(ctx context.Context, s State) (keylane.StageResult[State], error) {
    cont, complete := keylane.NewContinuation[State](ctx)
    go func() {
        row, err := repo.GetCustomer(ctx, s.CustomerID)
        if err != nil {
            complete.Fail(err)
            return
        }
        s.Customer = row
        complete.Complete(s)
    }()
    return keylane.StageResult[State]{Continuation: cont}, nil
},
```

### External API call (yielded)

```go
RunContinuation: func(ctx context.Context, s State) (keylane.StageResult[State], error) {
    cont, complete := keylane.NewContinuation[State](ctx)
    go func() {
        wallet, err := walletClient.Fetch(ctx, s.CustomerID)
        if err != nil {
            complete.Fail(keylane.RetryableFailure(err)) // or PermanentFailure as appropriate
            return
        }
        s.Wallet = wallet
        complete.Complete(s)
    }()
    return keylane.StageResult[State]{Continuation: cont}, nil
},
```

Pass `ctx` from the stage into your backend call so request cancellation propagates to the I/O you control.

### Cancellation while yielded

When `SubmitPipeline`’s request context is cancelled while a continuation is pending, the runtime resolves the continuation as cancelled and completes the future. `Await` context cancellation does **not** cancel the continuation — only the wait ends.

If the request context is cancelled **before** the stage returns its continuation handle, the continuation is **not** registered (no pending entry, no yield hook).

### Deadline while yielded

When the request deadline expires while yielded, behavior matches v0.6 deadline semantics: the future completes with a classified failure, and late `Complete`/`Fail` calls are ignored (`LateCompletions`, `OnContinuationLate`).

Runnable reference: `go run ./examples/pipeline_continuation` and tests in `continuation_cancellation_test.go`, `continuation_deadline_test.go`.

---

## What continuations are not

Continuations are a yield/resume primitive for explicit I/O hand-off, not a general async runtime:

- **Not coroutines**: there is no suspend/resume within a single stage function. The stage function runs to completion; the continuation is the hand-off point.
- **Not parallel stages**: stages remain sequential. Continuations only free the worker between two sequential stages.
- **Not transparent**: callers must explicitly call `NewContinuation` and manage the completer. The runtime does not automatically detect blocking calls.
- **Not retry-on-resume**: the retry policy applies to the full pipeline, not to individual resume attempts.
- **Backend leases (KL-1704)**: acquire before yield; `defer lease.Release()` in the goroutine that completes the continuation. See [backend-resource-coordination.md](backend-resource-coordination.md).
- **Not pool adapters (KL-1705)**: no `database/sql`/HTTP integration in KL-1704.

See [request-pipeline.md](request-pipeline.md) for the base pipeline model and [stage-execution-context.md](stage-execution-context.md) for execution context propagation across yield/resume.

### Troubleshooting

- **Pending continuations not draining**: check `DebugSnapshot.Continuation` (`Pending`, `MaxPending`, `LateCompletions`) and continuation hook order in [pipeline-observability.md](pipeline-observability.md).
- **Tests**: [pipeline-testing.md](pipeline-testing.md) maps continuation observability and stress coverage to `continuation_observability_test.go` and `pipeline_stress_test.go`.
