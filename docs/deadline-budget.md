# Deadline Budget

A **deadline budget** tracks how much caller deadline remains after queue wait and runtime. It supports admission decisions and bounded retry (KL-1602).

go-keylane does **not** extend deadlines automatically. Optional retry sleeps between attempts only when `RetryPolicy.Enabled` is true and enough budget remains.

---

## Lifecycle

```text
submit time          → NewDeadlineBudget(ctx, now)           [AtSubmit]
admission passes     → refreshAt(now)                        [AtAdmission]
queue wait completes → WithQueueWait(wait)                   [AfterQueueWait]
handler start        → refreshAt(now) + reqCtx check         [AtHandlerStart]
handler completes    → WithRuntime(runDur)                   [AtCompletion]
```

`SubmitRequest` records all five phases in a `DeadlineBudgetTrace`. `SubmitValue` records submit, handler start, and completion (admission and queue-wait phases are N/A for the value-job path).

Before each retry, Keylane checks `remaining >= backoff_delay + MinRemainingBudget`. When there is no deadline, budget does not block retry.

---

## API

```go
budget := keylane.NewDeadlineBudget(ctx, time.Now())
budget = budget.WithQueueWait(queueWait)
budget = budget.WithRuntime(runDur)

if budget.HasRemaining(10 * time.Millisecond) {
    // admit or run
}
rem := budget.RemainingAt(time.Now())
exhausted := budget.IsExhaustedAt(time.Now())
```

`WithQueueWaitAt` and `WithRuntimeAt` are deterministic-clock helpers for tests; production code should use `WithQueueWait` / `WithRuntime`.

`SubmitRequest` and `SubmitValue` store the latest budget and full trace on the result future:

```go
budget, ok := keylane.BudgetFromFuture(future)
trace, ok := keylane.BudgetTraceFromFuture(future)

// Type-erased accessors when the output type is not known:
fail, ok := keylane.FailureFromFutureAny(future)
trace, ok = keylane.BudgetTraceFromFutureAny(future)
```

Remaining time uses `Deadline.Sub(now)`, which is monotonic-safe for Go `time.Time` values.

---

## Timeout vs deadline exhausted

| Phase | `context.DeadlineExceeded` classified as |
|-------|------------------------------------------|
| Before handler (queued, budget exhausted) | `deadline_exhausted` |
| During or after handler | `timeout` |

`SubmitRequest` and `SubmitValue` share the same before-handler vs during-handler classification rules.

`context.Canceled` is always `cancelled` — distinct from timeout.

Use `ClassifyContextError` / `ClassifyContextErrorAt` when you have budget and phase context.

---

## Context without deadline

When `ctx` has no deadline, `HasDeadline` is false, `Remaining` is zero, and `Exhausted` is false. Budget checks are no-ops for admission unless you add your own timeout.

---

## Cooperative cancellation

Keylane does not kill running handlers. Budget and classification reflect **context state** at check points. See [cancellation-timeout.md](cancellation-timeout.md).

---

## Related

- [failure-policy.md](failure-policy.md)
- [cancellation-timeout.md](cancellation-timeout.md)
