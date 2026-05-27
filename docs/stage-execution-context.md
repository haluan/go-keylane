# Stage Execution Context (v0.7)

Part of [v0.7.0 — Advanced Request Pipeline & Backend Resource Coordination](v0.7-advanced-request-pipeline-and-resource-coordination.md).

Pipeline stages and `SubmitRequest` handlers can read immutable execution metadata from `context.Context` without copying routing fields into business state.

---

## What it is

`StageExecutionContext` is a **value snapshot** of request, shard, stage, attempt, and deadline state at a point in execution. It is stored inside the standard Go context via `ContextWithStageExecution` and read with `StageExecutionFromContext`.

It is **not** a replacement for `context.Context`. Cancellation and deadlines still come from the parent context passed to `SubmitRequest` / `SubmitPipeline`.

---

## Reading metadata

```go
exec, ok := keylane.StageExecutionFromContext(ctx)
if ok {
    log.Printf("operation=%s stage=%s shard=%d attempt=%d remaining=%s",
        exec.Operation,
        exec.Stage.Name,
        exec.ShardID,
        exec.Attempt,
        exec.Deadline.Remaining,
    )
}
```

Helpers:

- `StageMetaFromContext(ctx)` — active stage only
- `RequestMetaFromExecution(exec)` — rebuild `RequestMeta` for APIs that expect it

---

## Fields

| Field | Meaning |
|-------|---------|
| `RequestID`, `Key`, `Lane`, `ShardID` | Request identity and routing |
| `Transport`, `Operation` | Low-cardinality observability names |
| `Stage` | Active `StageMeta` (name + optional operation override) |
| `StageIndex` | Zero-based index in the pipeline |
| `StageCount` | Total stages in the pipeline |
| `Attempt` | 1-based retry attempt (matches internal retry loop) |
| `QueueWait` | Queue wait at worker start |
| `Deadline` | `DeadlineBudgetSnapshot` at stage boundary |

### Stage index convention

`StageIndex` is **zero-based**. A three-stage pipeline has indices `0`, `1`, `2`.

`Complete` runs with `StageResponse`, `StageIndex == StageCount`, and `StageCount == len(Stages)`.

### Attempt semantics

- First execution: `Attempt == 1`
- Request-level retry re-runs the full pipeline; each stage sees the current attempt number in context

### Deadline snapshot

`DeadlineBudgetSnapshot` is captured at stage start. Stages can check `Deadline.Remaining` and `Deadline.BudgetExhausted` cooperatively. If the budget is exhausted before a stage starts, the pipeline stops with a `StageFailure` wrapping `deadline_exhausted`.

`Deadline.Runtime` is runtime consumed before the current stage starts within this attempt. For `SubmitPipeline`, it includes request-level runtime from earlier retry attempts plus elapsed pipeline time since the current attempt began. `Remaining` is still refreshed from the caller deadline at each retry attempt.

---

## SubmitPipeline vs SubmitRequest

| API | Stage metadata |
|-----|----------------|
| `SubmitPipeline` | Per-stage context derived for each `Run`; `Complete` uses `StageResponse` |
| `SubmitRequest` | Implicit single stage: `business`, index `0`, count `1` |

Existing `SubmitRequest` callers do not need to migrate to pipelines to read execution metadata.

---

## Logs vs metrics

Safe for **logs and debug hooks** (with care):

- `RequestID`, `Key` — useful for tracing; do not use as Prometheus labels by default

Safe for **metric labels**:

- `transport`, `operation`, `lane`, `stage` (from `Stage.Name`), `outcome`, `failure_kind`

Never use raw URL paths, tenant IDs, customer IDs, or error strings as labels.

### Backend operation from stage context

Stages that call [backend resource coordination](backend-resource-coordination.md) should derive operations from the active stage:

```go
Run: func(ctx context.Context, state State) (State, error) {
    op := keylane.BackendOperationFromStage(ctx, "primary-db", keylane.BackendLaneDBRead)
    return keylane.WithBackend(ctx, q, op, func(ctx context.Context) (State, error) {
        // ... downstream work while lease held
        return state, nil
    })
},
```

`BackendOperationFromStage` copies `Stage.Name` into admission hooks when present.

### Safe diagnostic logging

Log bounded execution dimensions. Do not log `Key` or `RequestID` in default stage logs or metric labels — they are high-cardinality or sensitive. If your platform requires trace correlation, inject a hashed or external trace ID at the transport boundary instead of copying raw request identifiers into stage logs.

```go
exec, ok := keylane.StageExecutionFromContext(ctx)
if ok {
  log.Info("stage start",
    "transport", exec.Transport,
    "operation", exec.Operation,
    "lane", exec.Lane,
    "stage", exec.Stage.Name,
    "shard", exec.ShardID,
    "attempt", exec.Attempt,
    "deadline_remaining", exec.Deadline.Remaining,
  )
  // Metric labels: transport, operation, lane, stage, outcome, failure_kind only
}
```

---

## Failures

Stage errors may wrap `StageFailure` with full `Execution` metadata:

```go
if sf, ok := keylane.AsStageFailure(err); ok {
    _ = sf.Execution.Stage.Name
}
```

`FailureFromFuture` still uses v0.6 classification on the underlying error.

---

## Not supported

- Non-blocking yield/resume (see [continuations.md](continuations.md))
- Per-stage retry policy
- Cross-pod context propagation

Backend resource coordination uses stage metadata from this context via `BackendOperationFromStage`. See [backend-resource-coordination.md](backend-resource-coordination.md).

---

## Related docs

- [v0.7.0 overview](v0.7-advanced-request-pipeline-and-resource-coordination.md)
- [Request Pipeline](request-pipeline.md)
- [Request Observability](request-observability.md)
- [Backend resource coordination](backend-resource-coordination.md)
- [Failure Policy](failure-policy.md)
- [Retry Policy](retry-policy.md)
- [Deadline Budget](deadline-budget.md)
