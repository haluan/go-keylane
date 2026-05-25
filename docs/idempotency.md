# Idempotency and Duplicate-Safety

Part of [v0.6.0 — Retry, Deadline & Failure Policy](v0.6-retry-deadline-failure-policy.md).

Bounded [retry](retry-policy.md) only retries **retryable** failures. Idempotency adds a second gate: the job must also be **duplicate-safe** before another attempt runs.

> **Retrying writes is a business decision.** Keylane can enforce duplicate-safety hooks, but it **cannot** make side effects idempotent by itself.

**Retryable ≠ safe to retry.** A transient network error on a payment charge is retryable in classification terms but unsafe without idempotency keys or explicit safety metadata.

See also: [retry-policy.md](retry-policy.md) (redirect to failure-policy retry section).

---

## When checks run

- Idempotency metadata is evaluated only when `Config.Retry.Enabled` (or a per-job / per-request retry override) is active.
- `DecideRetry` runs first; `DecideRetrySafety` runs only when a retry would be scheduled, **before** backoff sleep; `DecideRetrySuppression` runs after safety when [retry suppression](retry-suppression.md) is enabled.
- When retry is disabled, idempotency fields are inert (no extra work on the hot path).
- Safety hooks are **not** invoked when `DecideRetry` already rejects (permanent failure, cancellation, deadline budget exhausted, max attempts).

---

## RetrySafety values

| Value | Meaning |
|-------|---------|
| `""` (unspecified) | **Unsafe** when retry is enabled — no silent retries |
| `safe` | Allow retry after retryable failure |
| `unsafe` | Suppress retry unless `AllowUnsafeRetry` is set |
| `requires_check` | Call `IdempotencyPolicy.Hook` when configured |

---

## Safety decision reasons

| Reason | Meaning |
|--------|---------|
| `safe` | Declared safe or retry disabled |
| `unsafe` | Unspecified or declared unsafe |
| `missing_idempotency_key` | `RequireForRetry` and empty `Idempotency.Key` |
| `no_hook` | `requires_check` but `Hook` is nil — hook never ran |
| `hook_allowed` | Hook returned allow |
| `hook_rejected` | Hook returned deny |
| `hook_failed` | Hook panicked |
| `explicit_override` | `AllowUnsafeRetry` on unsafe job |

Reserve `hook_rejected` for denials where the hook actually ran. Use `no_hook` for missing configuration.

---

## Policy precedence

When multiple rules apply:

1. **`AllowUnsafeRetry`** on `RetrySafetyUnsafe` wins over `RequireForRetry` (explicit override).
2. **`RequireForRetry`** suppresses retry for **any** safety value when `Idempotency.Key` is empty.
3. **Safety enum** (`safe`, `unsafe`, `requires_check`) and hook outcome.

---

## Idempotency metadata

Attach to `ValueJob` or `Request`:

```go
Idempotency: keylane.Idempotency{
    Key:       "order-123-charge", // opaque; caller-defined
    Safety:    keylane.RetrySafetySafe,
    Scope:     "payment",
    Operation: "charge",
}
```

`Key` is not format-validated beyond empty checks when `RequireForRetry` is set.

---

## IdempotencyPolicy

```go
cfg := keylane.Config{
    Retry: keylane.RetryPolicy{Enabled: true, MaxAttempts: 3, /* ... */},
    Idempotency: keylane.IdempotencyPolicy{
        RequireForRetry: true, // any job with empty Key suppresses retry
        Hook: func(ctx context.Context, check keylane.RetrySafetyCheck) keylane.RetrySafetyDecision {
            return keylane.RetrySafetyDecision{Allow: true, Reason: keylane.RetrySafetyDecisionHookAllowed}
        },
    },
}
```

**Configuration matrix:**

| RequireForRetry | Hook | requires_check + empty Key | requires_check + key, no hook | requires_check + hook |
|-----------------|------|----------------------------|-------------------------------|----------------------|
| false | nil | unsafe (no hook) → `no_hook` | `no_hook` | hook decides |
| true | nil | `missing_idempotency_key` | `no_hook` | hook decides |
| true | set | `missing_idempotency_key` | hook decides | hook decides |

`RequireForRetry` without `Hook` is valid: cluster-wide missing-key enforcement only. Hooks must return promptly; there is no timeout (Go limitation).

Hook panics are recovered; retry is suppressed (`hook_failed`). The handler still returns the **original** failure on the `Future`.

`ErrRetryUnsafe` is a documentation sentinel for tests and future observability hooks.

---

## Idempotency fields

| Field | Purpose |
|-------|---------|
| `Key` | Stable caller-defined idempotency key across retries of the same logical operation |
| `Safety` | `safe`, `unsafe`, `requires_check`, or unspecified (unsafe when retry on) |
| `Scope` | Low-cardinality domain (e.g. `payment`, `order`) — suitable for bounded metrics |
| `Operation` | Side-effect name (e.g. `charge`, `send-webhook`) |
| `AllowUnsafeRetry` | Dangerous override on `unsafe` jobs; recorded as `explicit_override` on trace |

`IdempotencyPolicy.RequireForRetry` suppresses retry when `Key` is empty (any safety value). `IdempotencyPolicy.Hook` runs for `requires_check` only.

---

## Scenario examples

### Read-only profile fetch

```go
Idempotency: keylane.Idempotency{Safety: keylane.RetrySafetySafe}
```

### Payment charge (unsafe by default)

```go
Idempotency: keylane.Idempotency{
    Safety:    keylane.RetrySafetyUnsafe,
    Key:       "pay-" + paymentID,
    Scope:     "payment",
    Operation: "charge",
}
```

Use `requires_check` + hook that consults your idempotency store before allowing retry.

### Webhook send

```go
Idempotency: keylane.Idempotency{
    Safety:    keylane.RetrySafetyRequiresCheck,
    Key:       deliveryID,
    Scope:     "webhook",
    Operation: "send",
}
```

### Order update with cluster-wide key requirement

```go
// Config.Idempotency.RequireForRetry: true
Idempotency: keylane.Idempotency{
    Safety: keylane.RetrySafetySafe,
    Key:    "order-" + orderID + "-patch",
    Scope:  "order",
    Operation: "update",
}
```

---

## Safe vs unsafe summary

| Work | Suggested safety |
|------|------------------|
| Read-only GET handler | `safe` |
| Idempotent PUT with stable key | `safe` or `requires_check` + hook |
| Create payment / send email | `unsafe` or `requires_check` + store-backed hook |
| Unknown side effects | Leave unspecified → suppressed when retry on |

---

## Observing retry safety on futures

After `SubmitValue` or `SubmitRequest` with retry enabled, pull retry scheduling records from the result future:

```go
trace, ok := keylane.RetryTraceFromFuture(future)
if ok && trace.HadExplicitUnsafeRetry() {
    // AllowUnsafeRetry override was used before a retry sleep
}
```

Each `RetryAttempt` includes `SafetyReason` (for example `explicit_override`, `safe`, `hook_allowed`). Records are appended when `DecideRetry` would schedule a retry and `DecideRetrySafety` runs — including suppressed retries (so `hook_rejected` and `missing_idempotency_key` are visible too).

`RetryTraceFromFutureAny` works with untyped future handles.

---

## Metrics and cardinality

Internal `RetryAttempt` records may include `IdempotencyKey` for debugging. **Do not** use raw idempotency keys as Prometheus labels — cardinality explodes. Prefer bounded `IdempotencyScope` / `IdempotencyOperation` or a hashed key when metrics are added.

---

## In-process vs durable exactly-once

Keylane retry is **in-worker and in-process**. It does not provide durable exactly-once delivery across process restarts or duplicate queue entries. For that, use external idempotency stores, outbox patterns, or deduplication at the API boundary — and use `requires_check` hooks to consult them before approving a retry.

---

## Related

- [v0.6.0 overview](v0.6-retry-deadline-failure-policy.md)
- [failure-policy.md](failure-policy.md) — failure kinds and retry policy
- [retry-policy.md](retry-policy.md) — entry point for retry configuration
- [retry-suppression.md](retry-suppression.md) — pressure-aware retry gate
- [failure-aware-admission.md](failure-aware-admission.md) — failure kinds vs retry storms
- [deadline-budget.md](deadline-budget.md) — budget checks before retry sleep
