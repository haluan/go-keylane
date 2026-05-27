# Request Pipeline (v0.7)

Part of [v0.7.0 — Advanced Request Pipeline & Backend Resource Coordination](v0.7-advanced-request-pipeline-and-resource-coordination.md).

Production HTTP and backend handlers are often multi-step: validate, authorize, database access, external APIs, and response assembly. A single `SubmitRequest` handler hides where time was spent and which step failed.

`SubmitPipeline` adds an ordered, same-state stage model on top of the existing request runtime. It reuses admission, overload, per-key admission, retry, deadline budget, and `Future` completion from [request-runtime.md](request-runtime.md).

---

## When to use pipelines

Use `SubmitPipeline` when you need:

- Stage-level duration and failure attribution in hooks or metrics adapters
- Stage-aware execution context ([stage-execution-context.md](stage-execution-context.md)) and [backend resource coordination](backend-resource-coordination.md)
- Unified hook lifecycle and metrics guidance ([pipeline-observability.md](pipeline-observability.md))

Keep using `SubmitRequest` for single-step work. Existing callers do not need to migrate. See [Migration from SubmitRequest](#migration-from-submitrequest).

---

## Non-goals

Pipelines are **in-process request orchestration**.

They are **not**:

- Persistent workflows or durable sagas
- Distributed queues or cross-replica execution engines
- Exactly-once execution or automatic deduplication across restarts
- A database transaction manager

State and stage progress exist only for the lifetime of the in-memory request on one process.

---

## Core API

```go
type Pipeline[S any, O any] struct {
    Meta             RequestMeta
    Admission        AdmissionConfig
    Overload         OverloadConfig
    PerKeyAdmission  PerKeyAdmissionConfig
    Retry            RetryPolicy
    Idempotency      Idempotency
    RetrySuppression *RetrySuppressionPolicy

    State    S
    Stages   []PipelineStage[S]
    Complete func(context.Context, S) (O, error)
}

func SubmitPipeline[S any, O any](
    ctx context.Context,
    q *keylane.Queue,
    pipeline Pipeline[S, O],
) (keylane.Future[O], error)
```

Each `PipelineStage[S]` has `StageMeta` (low-cardinality `StageName`) and either:

- `Run(ctx, state) (state, error)` — synchronous stage (default)
- `RunContinuation(ctx, state) (StageResult[state], error)` — optional yield; requires `Continuation.Enabled` on the queue ([continuations.md](continuations.md))

`Complete(ctx, state) (O, error)` runs once after all stages succeed.

---

## Execution semantics

Stages run **sequentially in-worker** in declaration order on the same request context:

```text
SubmitPipeline
  -> validate metadata
  -> route by Key / Lane (same as SubmitRequest)
  -> enqueue one job
  -> stage[0] -> stage[1] -> ... -> Complete
  -> Future completes once
```

- First stage error stops the pipeline; `Complete` is not called.
- Context cancellation before or between stages stops execution cooperatively.
- When the deadline budget is exhausted before a stage or `Complete` runs, the pipeline returns a `StageFailure` classified as `deadline_exhausted` (not handler `timeout`).
- Retry is **request-level**: when retry is enabled, the entire pipeline (all stages + `Complete`) is retried as one unit. See [retry-policy.md](retry-policy.md).

### Not supported

- Per-stage retry policy

### Backend resource coordination

Stages may call `AcquireBackend` / `WithBackend` to bound downstream usage per configured resource and backend lane. See [backend-resource-coordination.md](backend-resource-coordination.md). Concrete DB/HTTP pool adapters are covered in [backend-pressure-adapters.md](backend-pressure-adapters.md).

---

## Non-blocking continuations

When `ContinuationConfig.Enabled` is true, a stage may use `RunContinuation` instead of `Run` and return a `*Continuation` from `NewContinuation` to release the worker until external code calls `ContinuationCompleter.Complete`, `Fail`, or `Cancel`. The runtime enqueues a resume job on the same key shard; stages after the yield run on a worker again.

- **Opt-in**: set `Continuation.Enabled` on the queue; see [continuations.md](continuations.md).
- **Limits**: `MaxPending` defaults to 256 when enabled and left at zero; `MaxPendingPerShard` is optional.
- **Retry parity**: continuation failures use the same retry policy, idempotency safety, suppression, and `RetryTraceFromFuture` as `SubmitRequest` / synchronous pipelines (full pipeline restart from stage 0).
- **Cancellation**: request context cancel or deadline while yielded resolves the continuation; late `Complete`/`Fail` increments `LateCompletions` and may fire `OnContinuationLate`.

Runnable example: [`examples/pipeline_continuation`](../examples/pipeline_continuation/).

---

## Stage naming

Use built-in names where they fit (`validate`, `db_read`, `external_api`, …) or short custom names (`enrich_wallet`). Names must be stable and low-cardinality.

**Do not** use tenant IDs, user IDs, raw URL paths, request IDs, or error text as stage names.

---

## Failure attribution

Stage errors are wrapped in `StageFailure` for attribution:

```go
_, err := future.Await(ctx)
if sf, ok := keylane.AsStageFailure(err); ok {
    log.Printf("failed at stage %s", sf.Stage.Name)
}
```

`FailureFromFuture` still returns the same [FailureKind](failure-policy.md) as a single-handler request. The classified kind is derived from the underlying error, not from the stage name.

---

## Observability

Optional hooks on `RequestHooks`:

- `OnStageStarted`
- `OnStageCompleted`
- `OnStageFailed`

See [request-observability.md](request-observability.md) for `StageObservation` fields and label guidance.

Suggested adapter metric names (not exported by the library directly):

| Metric | Labels |
|--------|--------|
| `keylane_pipeline_stage_started_total` | `transport`, `operation`, `lane`, `stage` |
| `keylane_pipeline_stage_completed_total` | `transport`, `operation`, `lane`, `stage`, `outcome` |
| `keylane_pipeline_stage_failed_total` | `transport`, `operation`, `lane`, `stage`, `failure_kind` |
| `keylane_pipeline_stage_duration_seconds` | `transport`, `operation`, `lane`, `stage` |

Do not label metrics with `Key`, `RequestID`, or raw error strings.

---

## Example

```go
type getCustomerState struct {
    CustomerID string
    User       User
    Wallet     Wallet
}

pipeline := keylane.Pipeline[getCustomerState, GetCustomerOutput]{
    Meta: keylane.RequestMeta{
        RequestID: "req-123",
        Key:       "customer-42",
        Lane:      keylane.Lane("read"),
        Operation: "get-customer",
    },
    State: getCustomerState{CustomerID: "customer-42"},
    Stages: []keylane.PipelineStage[getCustomerState]{
        {Meta: keylane.StageMeta{Name: keylane.StageValidate}, Run: validateCustomer},
        {Meta: keylane.StageMeta{Name: keylane.StageDBRead}, Run: fetchCustomerRow},
        {Meta: keylane.StageMeta{Name: keylane.StageExternalAPI}, Run: fetchWallet},
    },
    Complete: buildCustomerResponse,
}

future, err := keylane.SubmitPipeline(ctx, q, pipeline)
out, err := future.Await(ctx)
```

---

## Execution context

Each stage receives a derived `context.Context` with [`StageExecutionContext`](stage-execution-context.md) attached. Read it via `StageExecutionFromContext` inside `Run` and `Complete` without duplicating routing fields in your state struct.

---

## Future.Await

`SubmitPipeline` returns `Future[O]` with the same semantics as `SubmitRequest`:

- `future.Await(ctx)` blocks until the pipeline completes or fails.
- Use the **request** context passed to `SubmitPipeline` for cancellation of in-flight work.
- Use a separate **await** context only to bound how long the caller waits; cancelling the await context does not cancel a yielded continuation ([continuations.md](continuations.md)).
- Never call `Await` on the same queue from inside a stage `Run` function — see README deadlock warning.

`FailureFromFuture`, `RetryTraceFromFuture`, and `AsStageFailure` work the same as for single-handler requests.

---

## Migration from SubmitRequest

| Situation | Recommendation |
|-----------|----------------|
| One handler, no stage metrics | Keep `SubmitRequest` |
| Multi-step with shared state | Introduce `SubmitPipeline` with one `PipelineStage` per step |
| Need yield during slow I/O | Enable `Continuation` and use `RunContinuation` on the slow stage only |
| Already using retry/deadline | Copy `Retry`, `Idempotency`, and admission fields onto `Pipeline` — behavior is request-level |

`SubmitRequest` handlers still receive `StageExecutionContext` (implicit `business` stage). No forced migration.

Runnable sync example: [`examples/pipeline_basics`](../examples/pipeline_basics/).

---

## Related docs

- [v0.7.0 overview](v0.7-advanced-request-pipeline-and-resource-coordination.md)

- [Non-Blocking Continuations](continuations.md) — yield/resume, limits, late completion
- [Stage Execution Context](stage-execution-context.md) — metadata in `context.Context`
- [Request Runtime](request-runtime.md) — `SubmitRequest`, routing, futures
- [Request Observability](request-observability.md) — request and stage hooks
- [Failure Policy](failure-policy.md) — `StageFailure` and classification
- [Retry Policy](retry-policy.md) — request-level retry with pipelines
