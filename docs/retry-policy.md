# Retry Policy

Part of [v0.6.0 — Retry, Deadline & Failure Policy](v0.6-retry-deadline-failure-policy.md).

Bounded in-worker retry for `SubmitValue`, `SubmitRequest`, and `SubmitPipeline`. Retry is **opt-in**: the zero value of `RetryPolicy` disables retry.

For pipelines, retry re-runs the **entire** stage list and `Complete` function on each attempt. Per-stage retry is not supported in KL-1701. See [request-pipeline.md](request-pipeline.md).

When retry is enabled, `StageExecutionContext.Attempt` in each stage context reflects the current 1-based attempt (see [stage-execution-context.md](stage-execution-context.md)).

Before a retry sleeps, Keylane runs three gates: [failure classification](failure-policy.md) → [idempotency safety](idempotency.md) → [retry suppression](retry-suppression.md). See [deadline-budget.md](deadline-budget.md) for budget checks between attempts.

---

## RetryPolicy fields

| Field | Meaning | When `Enabled: true` and zero / unset |
|-------|---------|--------------------------------------|
| `Enabled` | Master switch | `false` = no retry (zero value) |
| `MaxAttempts` | Total attempts including the first run | Defaults to **3** |
| `InitialBackoff` | Base delay before first retry | **10ms** |
| `MaxBackoff` | Cap on exponential backoff | **250ms** |
| `Multiplier` | Exponential factor per attempt | **2.0** |
| `Jitter` | Randomize delay | **true** when `JitterFraction` normalized |
| `JitterFraction` | Fraction of delay used as jitter span | **0.2** |
| `MinRemainingBudget` | Min caller deadline left before scheduling another attempt | **InitialBackoff** if unset |
| `RetryableKinds` | Optional allow-list of `FailureKind` to retry | Empty = default rules (`retryable` only) |

`NormalizeRetryPolicy` applies defaults when `Enabled: true`. `ValidateRetryPolicy` runs at queue start.

### MaxAttempts includes the first attempt

`MaxAttempts: 3` means one initial handler run plus up to **two** retries after failures. There is no separate “retry count” field.

### Backoff and jitter

Delay grows roughly as `InitialBackoff * Multiplier^(attempt-1)`, capped at `MaxBackoff`. With `Jitter: true`, a random fraction of the delay (up to `JitterFraction`) is added to desynchronize retries.

`NormalizeRetryPolicy` sets `JitterFraction` to `0.2` and `Jitter` to `true` when `JitterFraction <= 0`. To keep delays deterministic in tests or examples, set `Jitter: false` and a positive `JitterFraction` (for example `0.01`).

### MinRemainingBudget

Before sleeping for a retry, Keylane checks:

```text
remaining caller deadline >= retry_delay + MinRemainingBudget
```

If not, retry stops with `budget_too_small` or `deadline_exhausted`. See [deadline-budget.md](deadline-budget.md).

### RetryableKinds

By default only `FailureRetryable` (from `RetryableFailure(err)` or classifier) is retried. Set `RetryableKinds` to explicitly allow other kinds (unusual; use with care for `rejected` per-key throttle).

Permanent, cancelled, timeout, deadline-exhausted, overload, rejected, panic, and unknown failures are **not** retried unless listed.

---

## Configuration levels

| Level | Field | Precedence |
|-------|-------|------------|
| Queue | `Config.Retry` | Base policy |
| Job | `ValueJob.Retry` | Value override when `Enabled: true` |
| Request | `Request.Retry` | Value override when `Enabled: true` |

Per-job/request override applies only when `Retry.Enabled: true` (`resolveRetryPolicy` in code). A zero-value `RetryPolicy{}` on the job or request falls back to queue `Config.Retry`. This differs from `RetrySuppression`, which is a pointer field (`nil` uses the queue policy).

```go
cfg := keylane.Config{
    ShardCount: 1, WorkerCount: 2, QueueSizePerLane: 64,
    LaneQuotas: map[keylane.Lane]int{"default": 2},
    Retry: keylane.RetryPolicy{
        Enabled:            true,
        MaxAttempts:        3,
        InitialBackoff:     10 * time.Millisecond,
        MaxBackoff:         250 * time.Millisecond,
        Multiplier:         2,
        Jitter:             true,
        JitterFraction:     0.2,
        MinRemainingBudget: 20 * time.Millisecond,
    },
    Idempotency: keylane.IdempotencyPolicy{RequireForRetry: true},
    RetrySuppression: keylane.RetrySuppressionPolicy{Enabled: true},
}
```

---

## SubmitValue example

```go
var attempts atomic.Int32
future, err := keylane.SubmitValue(ctx, q, keylane.ValueJob[int]{
    Key:  "order-42",
    Lane: "default",
    Retry: keylane.RetryPolicy{Enabled: true, MaxAttempts: 3},
    Idempotency: keylane.Idempotency{
        Safety: keylane.RetrySafetySafe,
        Key:    "order-42-fetch",
        Scope:  "order",
    },
    Run: func(ctx context.Context) (int, error) {
        if attempts.Add(1) < 2 {
            return 0, keylane.RetryableFailure(errors.New("transient"))
        }
        return 42, nil
    },
})
if err != nil {
    return err
}
val, err := future.Await(ctx)
```

Return `keylane.RetryableFailure(err)` for transient handler errors. Plain `errors.New` without classification is **unknown** and not retried.

---

## SubmitRequest example

```go
future, err := keylane.SubmitRequest(ctx, q, keylane.Request[struct{}, int]{
    Meta: keylane.RequestMeta{Key: "tenant-1", Lane: "default", Operation: "sync"},
    Retry: keylane.RetryPolicy{Enabled: true, MaxAttempts: 3},
    Idempotency: keylane.Idempotency{Safety: keylane.RetrySafetySafe, Key: "sync-1"},
    Handle: func(ctx context.Context, _ struct{}) (int, error) {
        return doSync(ctx)
    },
})
```

To use queue `Config.Retry` instead of a per-request override, leave `Retry: keylane.RetryPolicy{}` (or any policy with `Enabled: false`).

---

## Choosing attempts and backoff

| Workload | Guidance |
|----------|----------|
| Fast idempotent reads | `MaxAttempts` 2–3, short `InitialBackoff` |
| Downstream RPC with ~100ms SLA | Match `MinRemainingBudget` to p99 caller deadline minus handler time |
| Write paths | Prefer low `MaxAttempts` + strong idempotency; rely on client retry with keys |
| Overloaded dependencies | Enable [retry suppression](retry-suppression.md); do not only raise `MaxAttempts` |

Pre-enqueue validation and admission failures are never retried in-worker.

---

## Observing retry decisions

```go
trace, ok := keylane.RetryTraceFromFuture(future)
if ok {
    for _, a := range trace.Attempts {
        _ = a.Reason      // retry_decision_reason
        _ = a.Delay
        _ = a.Suppressed
        _ = a.SuppressionReason
    }
    _ = trace.Final.StoppedReason
}
```

Counters: [retry-observability.md](retry-observability.md). Classification: [failure-policy.md](failure-policy.md).

---

## What retry is not

- **Not** a circuit breaker — no open/half-open state across dependencies
- **Not** a workflow engine — no durable step storage or cross-process orchestration
- **Not** automatic for all errors — requires `RetryableFailure` or policy
- **Not** a substitute for idempotency keys at the API or database layer

---

## Related

- [v0.6.0 overview](v0.6-retry-deadline-failure-policy.md)
- [failure-policy.md](failure-policy.md)
- [deadline-budget.md](deadline-budget.md)
- [idempotency.md](idempotency.md)
- [retry-suppression.md](retry-suppression.md)
- [failure-aware-admission.md](failure-aware-admission.md)
- [retry-observability.md](retry-observability.md)
