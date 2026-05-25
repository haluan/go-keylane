# Retry Suppression

Part of [v0.6.0 — Retry, Deadline & Failure Policy](v0.6-retry-deadline-failure-policy.md).

Bounded [retry](retry-policy.md) and [duplicate safety](idempotency.md) are necessary but not sufficient. Retry suppression adds a **runtime-health gate** so retries do not amplify load when the queue is pressured, overloaded, or already rejecting work.

> **Retry should not amplify unhealthy traffic.** When the queue is pressured, overloaded, hot-key dominated, or asking for scale-out, retry should be suppressed for non-critical work.

**Retry is a privilege granted by runtime health, not only by error type.**

See also: [failure-aware-admission.md](failure-aware-admission.md).

---

## Three gates before a retry sleep

```text
1. DecideRetry        — failure retryable, attempts, budget, context
2. DecideRetrySafety  — duplicate-safe metadata / hook
3. DecideRetrySuppression — pressure, lane class, hot key, scale signal
```

When suppression applies, the handler still returns the **original classified failure**; suppression is visible on `RetryTrace` (`SuppressionReason`, `RetriesSuppressedTotal`).

`RetrySuppressionSnapshot` is read-only: it observes hot-key tracker state and pressure without mutating admission counters.

---

## RetrySuppressionPolicy fields

| Field | Meaning |
|-------|---------|
| `Enabled` | Master switch; zero value disables suppression |
| `SuppressWhenOverloaded` | Suppress all lanes when global depth ≥ overload ratio |
| `SuppressNonCriticalWhenPressured` | Suppress `background` / `best_effort` when globally pressured |
| `SuppressLaneAboveRatio` | Suppress when lane depth ratio ≥ threshold |
| `SuppressShardAboveRatio` | Suppress when shard depth ratio ≥ threshold |
| `SuppressOverloadFailures` | Do not retry `overloaded` failure kind |
| `SuppressAdmissionFailures` | Do not retry general `rejected` admission failures |
| `SuppressPerKeyAdmissionFailures` | Do not retry per-key throttle/reject/shed |
| `SuppressHotKeyRetry` | Suppress hot-key retries on non-critical lanes |
| `AllowCriticalHotKeyRetry` | Opt-in: at most one retry on critical lane when hot (default `false`) |
| `SuppressWhenScaleOutRecommended` | Suppress non-critical when scale signal recommends scale-out |
| `Hook` | Optional `RetrySuppressionHook` for custom suppress/allow |

Per-job override: `ValueJob.RetrySuppression` or `Request.RetrySuppression` (pointer; `nil` uses queue policy).

### Defaults after `NormalizeRetrySuppressionPolicy`

**Always** when `Enabled: true`:

- `SuppressLaneAboveRatio` → `PressuredDepthRatio` (0.70) if zero
- `SuppressShardAboveRatio` → same if zero

**Safe-on bundle** — only when every other knob is unset: all boolean suppress flags above are `false`, both ratios are zero, and `Hook` is `nil`. Then `Normalize` sets these to `true`:

- `SuppressWhenOverloaded`
- `SuppressNonCriticalWhenPressured`
- `SuppressOverloadFailures`
- `SuppressAdmissionFailures`
- `SuppressPerKeyAdmissionFailures`
- `SuppressHotKeyRetry`
- `SuppressWhenScaleOutRecommended`

`AllowCriticalHotKeyRetry` is never set by normalization; it stays `false` unless you opt in.

**Partial configuration** does not enable the safe-on bundle. For example:

```go
RetrySuppression: keylane.RetrySuppressionPolicy{
    Enabled: true,
    SuppressHotKeyRetry: true,
}
// SuppressWhenOverloaded, SuppressAdmissionFailures, etc. remain false
```

---

## RetrySuppressionHook

```go
Hook: func(ctx context.Context, check keylane.RetrySuppressionCheck) keylane.RetrySuppressionDecision {
    if shouldSuppress(check) {
        return keylane.RetrySuppressionDecision{
            Suppress: true,
            Reason:   keylane.RetrySuppressionHookRejected,
        }
    }
    return keylane.RetrySuppressionDecision{}
}
```

Hook panics are recovered; retry is suppressed with `hook_failed`. Hooks must return promptly.

---

## Suppression vs admission rejection

| Stage | What happens | Observable as |
|-------|----------------|-----------------|
| **Admission** (before enqueue) | New work rejected | Submit error, `rejected` failure kind, request `OnRejected` |
| **Retry suppression** (in worker, after handler failure) | Retry not scheduled; original error returned | `RetryTrace`, `RetriesSuppressedTotal`, same handler error |

Admission protects the queue from **new** work. Suppression prevents **internal** retry amplification on work already admitted.

---

## Lane classes under pressure

| Class | Under pressure (not overload) | Under overload |
|-------|------------------------------|----------------|
| `critical` | May retry (bounded) | Suppressed |
| `normal` | May retry | Suppressed |
| `background` / `best_effort` | Suppressed | Suppressed |

Configure lane class via [admission policy](admission-control.md) / `UpdateAdmissionPolicy`.

---

## Configuration

```go
cfg := keylane.Config{
    Retry: keylane.RetryPolicy{Enabled: true, MaxAttempts: 3},
    RetrySuppression: keylane.RetrySuppressionPolicy{
        Enabled: true,
        // zero value with Enabled: true applies safe-on defaults via Normalize
    },
}
```

---

## Observing suppression

```go
trace, ok := keylane.RetryTraceFromFuture(future)
if ok && trace.HadSuppression(keylane.RetrySuppressionGlobalOverload) {
    // retry was suppressed due to queue overload
}
snap := q.RetryFailureSnapshot()
// snap.BySuppressionReason includes global_overload, global_pressure, hot_key, ...
```

`RetryAttempt` records `SuppressionReason`, `PressureRatio`, and lane/shard depth ratios — not raw idempotency keys.

---

## Related

- [v0.6.0 overview](v0.6-retry-deadline-failure-policy.md)
- [failure-policy.md](failure-policy.md)
- [retry-policy.md](retry-policy.md)
- [idempotency.md](idempotency.md)
- [failure-aware-admission.md](failure-aware-admission.md)
- [retry-observability.md](retry-observability.md)
- [deadline-budget.md](deadline-budget.md)
