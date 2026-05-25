# Failure Policy

Part of [v0.6.0 — Retry, Deadline & Failure Policy](v0.6-retry-deadline-failure-policy.md).

v0.6.0 adds a first-class **failure classification** model so callers and [retry policy](retry-policy.md) can distinguish retryable errors, permanent failures, timeouts, cancellation, overload, admission rejection, and deadline budget exhaustion.

Classification runs on every handler error before retry, suppression, and observability counters are updated.

---

## Why classification exists

Without explicit failure kinds, every handler error looks the same:

```text
handler returns error → scheduler sees error → caller cannot tell retryable from permanent
```

That becomes dangerous when retry support is added: retrying the wrong failure amplifies load (retry storms).

**Unknown errors are not retryable by default.** Use `RetryableFailure` or a custom `FailureClassifier` when you want retry semantics.

---

## FailureKind values

| Kind | Meaning |
|------|---------|
| `none` | Success |
| `retryable` | Transient failure; safe to retry when policy allows |
| `permanent` | Non-retryable business/validation failure |
| `timeout` | Handler or downstream exceeded time limit during execution |
| `cancelled` | Caller or request context cancelled |
| `overloaded` | System pressure too high (overload policy) |
| `rejected` | Explicit admission rejection (lane, queue full, per-key reject) |
| `deadline_exhausted` | Caller deadline consumed before handler could run |
| `panic` | Reserved for future panic recovery (not emitted by the scheduler today; same as `JobOutcomePanicked`) |
| `unknown` | Plain error with no stronger signal |

---

## API

```go
f := keylane.ClassifyFailure(err)
if f.IsRetryable() { /* backoff */ }
if f.IsTerminal() { /* do not retry */ }

var wrapped keylane.Failure
if errors.As(err, &wrapped) {
    _ = wrapped.Kind
}
```

Constructors: `RetryableFailure`, `PermanentFailure`, `TimeoutFailure`, `CancelledFailure`, `OverloadedFailure`, `RejectedFailure`, `DeadlineExhaustedFailure`, `UnknownFailure`.

### Custom classifier

```go
cfg := keylane.Config{
    FailurePolicy: keylane.FailurePolicy{
        Classifier: func(err error) keylane.Failure {
            if errors.Is(err, myErrTransient) {
                return keylane.RetryableFailure(err)
            }
            return keylane.Failure{}
        },
    },
}
```

When the classifier returns `FailureUnknown` or zero kind, the default classifier runs.

### Default classifier order

1. Typed `Failure` wrappers (`RetryableFailure`, `PermanentFailure`, etc.)
2. `context.Canceled` → `cancelled`
3. `context.DeadlineExceeded` → `timeout` (handler phase) or `deadline_exhausted` (queued phase via budget helpers)
4. Known sentinel errors (`ErrQueueFull`, admission errors, overload)
5. Plain errors → `unknown` (not retryable)

### Warnings

- Do **not** classify business validation errors as `retryable`.
- Do **not** classify unsafe mutation failures as `retryable` unless duplicate safety is solved ([idempotency.md](idempotency.md)).
- Do **not** use raw error strings as metric labels.

---

## Overload vs rejection

| Situation | Kind |
|-----------|------|
| Overload policy reject/shed/degrade | `overloaded` |
| Lane admission reject | `rejected` |
| `ErrQueueFull` | `rejected` |
| Per-key throttle | `rejected` (retryable) |
| Per-key reject/shed | `rejected` |

---

## Timeout vs cancellation vs deadline exhausted

| Situation | Kind |
|-----------|------|
| `context.Canceled` | `cancelled` |
| `context.DeadlineExceeded` during handler | `timeout` |
| Deadline expired while queued (before handler) | `deadline_exhausted` |

See [deadline-budget.md](deadline-budget.md).

---

## Retry integration

Retry is **opt-in** and documented in [retry-policy.md](retry-policy.md). Only `retryable` failures are retried by default. Return `keylane.RetryableFailure(err)` from handlers for transient errors.

---

## Inspecting failures on futures

`SubmitValue` and `SubmitRequest` attach classified failures to the result future:

```go
failure, ok := keylane.FailureFromFuture(future)
if ok {
    switch failure.Kind {
    case keylane.FailurePermanent:
        // business error
    case keylane.FailureRetryable:
        // transient; may have been retried in-worker
    }
}

// When the output type is not known at compile time:
failure, ok := keylane.FailureFromFutureAny(future)
```

`RequestObservation.FailureKind` is set on request hooks (`OnCompleted`). Failed handlers emit `Outcome: failed` with the classified kind. See [request-observability.md](request-observability.md) and [failure-observability.md](failure-observability.md).

---

## Future metrics

Suggested Prometheus labels:

```text
failure_kind
queue_wait_duration
runtime_duration
deadline_remaining_duration
deadline_budget_exhausted
```

Use low-cardinality `failure_kind` only — never raw keys or tenant IDs.

---

## Related

- [v0.6.0 overview](v0.6-retry-deadline-failure-policy.md)
- [deadline-budget.md](deadline-budget.md)
- [cancellation-timeout.md](cancellation-timeout.md)
- [admission-control.md](admission-control.md)
- [overload-policy.md](overload-policy.md)
- [retry-policy.md](retry-policy.md)
- [idempotency.md](idempotency.md)
- [retry-suppression.md](retry-suppression.md)
- [failure-aware-admission.md](failure-aware-admission.md)
