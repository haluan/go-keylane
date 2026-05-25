# Retry observability

Part of [v0.6.0 — Retry, Deadline & Failure Policy](v0.6-retry-deadline-failure-policy.md).

Keylane exposes retry scheduling, suppression, and final outcomes through low-cardinality counters, optional hooks, and per-future traces.

## Pull snapshot

```go
snap := q.RetryFailureSnapshot()
```

`RetryFailureSnapshot` includes totals (`AttemptsTotal` counts handler invocations only—`attempt_started` is emitted immediately before `run()` and not when the context is already cancelled) and breakdown slices by `failure_kind`, `retry_decision_reason`, `retry_suppression_reason`, and `retry_safety_reason`. Other totals cover scheduled retries, suppressed retries, safety/deadline stops, max-attempt stops, and exhaustion. Slices are built only when the snapshot is pulled.

## Hooks

Enable hooks on the queue:

```go
cfg.Observability.EnableHooks = true
cfg.Observability.Hooks.Retry.OnRetryEvent = func(e keylane.RetryEvent) {
    // handle event
}
```

`OnRetryEvent` receives `RetryEvent` values including `attempt_started` (immediately before each handler run), `failure_classified` (after error classification), scheduled retries, safety suppression, pressure suppression, terminal stops, and success.

Terminal totals map as follows: when a retryable loop ends at `MaxAttempts`, both `max_attempts_stopped` (`RetryMaxAttemptsStoppedTotal`, decision reason) and `exhausted` (`RetryExhaustedTotal`, outcome with `Final.Exhausted`) are emitted; `RetryDeadlineStoppedTotal` on `deadline_stopped` only; context cancellation uses `context_cancelled` (counts in `CancellationsTotal` via `failure_classified`, not deadline stops); permanent failure uses `retry_stopped` for `ByRetryReason` only (no exhausted or max-attempts totals).

## Future trace

```go
trace, ok := keylane.RetryTraceFromFuture(future)
if ok {
    _ = trace.Final.Succeeded
    _ = trace.Final.SuppressionReason
    for _, a := range trace.Attempts { _ = a.Delay }
}
```

`RetryTrace.Final` explains why the retry loop stopped (success, permanent failure, max attempts, deadline, safety, or suppression). `RetryTraceFromFutureAny` works with untyped futures.

## Safe metric labels

Use:

- `lane`
- `failure_kind`
- `retry_decision_reason`
- `retry_suppression_reason`
- `retry_safety_reason`
- `operation` / `idempotency_scope` (when bounded)

Do **not** use as labels:

- raw request `key`
- `request_id`
- `idempotency_key`
- error strings
- user or customer identifiers

`RetryEvent.Key` exists for debugging in hooks only.

## Budget and failure on the same future

```go
failure, _ := keylane.FailureFromFuture(future)
budget, _ := keylane.BudgetFromFuture(future)
trace, _ := keylane.RetryTraceFromFuture(future)
```

---

## Related

- [v0.6.0 overview](v0.6-retry-deadline-failure-policy.md)
- [failure-observability.md](failure-observability.md)
- [retry-policy.md](retry-policy.md)
- [retry-suppression.md](retry-suppression.md)
- [idempotency.md](idempotency.md)
- [benchmarks.md](benchmarks.md)
