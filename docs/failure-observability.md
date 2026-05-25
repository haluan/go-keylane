# Failure observability (KL-1605)

Failure classification counters complement retry traces and hooks.

## Counters

`Queue.RetryFailureSnapshot()` includes:

- `FailuresTotal` — classified handler failures
- `TimeoutsTotal` — timeout and deadline-exhausted kinds
- `CancellationsTotal` — cancellation kind
- `ByFailureKind` — per-`FailureKind` breakdown

Counters increment on:

- **`failure_classified` events** inside `runWithRetry` when a handler error is classified (retry-enabled jobs)
- **`AttemptsTotal`** on each `attempt_started` event before `run()` — includes successful first attempts, not only failures
- Non-retry `SubmitValue` / `SubmitRequest` completions with a classified error via `completeWithFailureObs`

## Failure kinds

| Kind | Typical source |
|------|----------------|
| `retryable` | Transient handler errors |
| `permanent` | Non-retryable business errors |
| `timeout` | Deadline exceeded |
| `cancelled` | Context cancellation |
| `deadline_exhausted` | Budget exhausted before retry |
| `overloaded` / `rejected` | Admission and overload paths |

Custom classifiers via `FailurePolicy` are reflected in `ByFailureKind`.

## Safe labels

Same rules as [retry-observability.md](retry-observability.md): use `failure_kind` and bounded `operation` / `lane`; never label metrics with raw keys or idempotency keys.
