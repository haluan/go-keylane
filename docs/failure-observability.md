# Failure observability

Part of [v0.6.0 — Retry, Deadline & Failure Policy](v0.6-retry-deadline-failure-policy.md).

Failure classification counters complement [retry traces](retry-observability.md) and hooks.

---

## RetryFailureSnapshot

```go
snap := q.RetryFailureSnapshot()
```

Pull API — slice fields (`ByFailureKind`, etc.) are allocated **on pull**, not on every classification. Scrape on an interval; do not call on every request hot path.

| Field | Meaning |
|-------|---------|
| `FailuresTotal` | Classified handler failures |
| `TimeoutsTotal` | `timeout` and `deadline_exhausted` kinds |
| `CancellationsTotal` | `cancelled` kind |
| `ByFailureKind` | Per-`FailureKind` breakdown |
| `AttemptsTotal` | Handler runs (`attempt_started` before `run()`) |
| `RetriesScheduledTotal` | Retries that passed gates and slept |
| `RetriesSuppressedTotal` | Retries blocked by safety or suppression |

See [retry-observability.md](retry-observability.md) for retry-specific totals and reason buckets.

---

## When counters increment

- **`failure_classified`** inside `runWithRetry` when a handler error is classified (retry-enabled jobs)
- **`AttemptsTotal`** on each `attempt_started` immediately before `run()` (not when context already cancelled)
- Non-retry `SubmitValue` / `SubmitRequest` completions with classified error via `completeWithFailureObs`

---

## Failure kinds

| Kind | Typical source |
|------|----------------|
| `retryable` | `RetryableFailure`, transient errors |
| `permanent` | `PermanentFailure`, validation |
| `timeout` | `DeadlineExceeded` during handler |
| `cancelled` | Context cancellation |
| `deadline_exhausted` | Budget exhausted while queued |
| `overloaded` / `rejected` | Admission and overload paths |

Custom `FailurePolicy.Classifier` results appear in `ByFailureKind`.

---

## FailureFromFuture and budget

```go
failure, ok := keylane.FailureFromFuture(future)
budget, ok := keylane.BudgetFromFuture(future)
trace, ok := keylane.BudgetTraceFromFuture(future)

failure, ok = keylane.FailureFromFutureAny(future)
```

See [failure-policy.md](failure-policy.md) and [deadline-budget.md](deadline-budget.md).

---

## Request failure hooks

`SubmitRequest` sets `RequestObservation.FailureKind` on `OnCompleted` using the same classifier as futures. Failed handlers emit `Outcome: failed` with `FailurePermanent`, `FailureRetryable`, etc.

```go
hooks.OnCompleted = func(obs keylane.RequestObservation) {
    if obs.Outcome == keylane.RequestOutcomeFailed {
        _ = obs.FailureKind // classified, not unknown for wrapped failures
    }
}
```

---

## Safe labels

Do **not** use as metric labels:

- raw request `key` or tenant id
- `request_id`
- `idempotency_key`
- error message strings

Use bounded labels: `failure_kind`, `lane`, `operation`, `transport`.

---

## Related

- [v0.6.0 overview](v0.6-retry-deadline-failure-policy.md)
- [retry-observability.md](retry-observability.md)
- [failure-policy.md](failure-policy.md)
- [request-observability.md](request-observability.md)
- [observability.md](observability.md)
- [benchmarks.md](benchmarks.md)
