# Retry Suppression (KL-1604)

Bounded retry (KL-1602) and duplicate safety (KL-1603) are necessary but not sufficient. KL-1604 adds a **runtime-health gate** so retries do not amplify load when the queue is pressured, overloaded, or already rejecting work.

**Retry is a privilege granted by runtime health, not only by error type.**

See also: [retry-policy.md](retry-policy.md), [idempotency.md](idempotency.md), [failure-aware-admission.md](failure-aware-admission.md).

---

## Three gates before a retry sleep

```text
1. DecideRetry        — failure retryable, attempts, budget, context
2. DecideRetrySafety  — duplicate-safe metadata / hook
3. DecideRetrySuppression — pressure, lane class, hot key, scale signal
```

When suppression applies, the handler still returns the **original classified failure**; suppression is visible on `RetryTrace`.

`RetrySuppressionSnapshot` is read-only: it observes hot-key tracker state and plans per-key mitigation without incrementing admission counters, updating cooldown timestamps, or expiring stale hot-key entries. Stale tracker slots remain in the index; they are treated as absent for suppression decisions only. Enqueue and other mutate paths may still expire stale entries.

---

## Configuration

```go
cfg := keylane.Config{
    Retry: keylane.RetryPolicy{Enabled: true, MaxAttempts: 3, /* ... */},
    RetrySuppression: keylane.RetrySuppressionPolicy{
        Enabled: true,
        // zero value with Enabled: true applies safe-on defaults via Normalize
    },
}
```

Per-job override via `Request.RetrySuppression` or `ValueJob.RetrySuppression` (pointer, nil uses queue policy) wins when `Enabled: true`.

---

## Default behavior when enabled

| Signal | Default |
|--------|---------|
| Global overload | Suppress all lanes |
| Global pressure | Suppress best-effort / background |
| Critical under pressure (not overload) | May retry |
| Overload / admission / per-key failures | Suppress retry |
| Hot key on non-critical lane | Suppress |
| Hot key on critical lane | Suppress unless `AllowCriticalHotKeyRetry: true` (at most one retry) |
| Scale-out recommended | Suppress non-critical |

---

## Observing suppression

```go
trace, ok := keylane.RetryTraceFromFuture(future)
if ok && trace.HadSuppression(keylane.RetrySuppressionGlobalOverload) {
    // retry was suppressed due to queue overload
}
```

`RetryAttempt` records `SuppressionReason`, `PressureRatio` (global depth ratio), and lane/shard depth ratios — not raw idempotency keys.

---

## Related

- [failure-policy.md](failure-policy.md)
- [deadline-budget.md](deadline-budget.md)
