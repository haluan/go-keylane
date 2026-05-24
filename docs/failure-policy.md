# Failure Policy

v0.6 adds a first-class **failure classification** model so callers and future retry policy can distinguish retryable errors, permanent failures, timeouts, cancellation, overload, admission rejection, and deadline budget exhaustion.

This task does **not** implement retries. It defines semantics only.

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

## Request runtime integration

`SubmitRequest` attaches classified failures to the internal future. Use:

```go
failure, ok := keylane.FailureFromFuture(future)
```

`RequestObservation.FailureKind` is set on request hooks. Optional `Hooks.Request.OnFailure` receives `FailureEvent`.

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

- [deadline-budget.md](deadline-budget.md)
- [cancellation-timeout.md](cancellation-timeout.md)
- [admission-control.md](admission-control.md)
- [overload-policy.md](overload-policy.md)
