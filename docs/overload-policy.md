# Overload Policy

## Overview

KL-1403 adds an overload policy engine that evaluates runtime pressure, lane class, and queue state **before enqueue**. It returns a structured decision (`keep`, `reject`, `shed`, `degrade`) and optional backoff hints.

Overload policy only applies to **new** submissions. It does not drop queued work or cancel running jobs.

**Decision before enqueue** — Overload (and admission) run on the submit path before work enters the scheduler queue, so rejected work does not increase `TotalQueued`.

When both overload and admission are enabled, **overload runs first**. Prefer overload-only for new integrations that need shed/degrade semantics.

---

## Overload decision model

| Action | Meaning |
|--------|---------|
| **keep** | Accept the request and continue the normal enqueue path |
| **reject** | Reject before enqueue because the system should not take more work (`ErrOverloadRejected`) |
| **shed** | Drop lower-value work earlier to protect higher-value work (`ErrOverloadShed`) |
| **degrade** | Do not enqueue; caller or middleware may run a cheaper fallback (`ErrOverloadDegraded`) |

Core Keylane does not sleep, retry, or build business fallback responses. `RetryAfter` and `BackoffHint` are **advisory** — they help cooperative clients retry more safely but do not guarantee client compliance.

### When to use each action

| Action | Typical use |
|--------|-------------|
| **shed** | Best-effort or background lanes under pressure |
| **reject** | Accepting more work would worsen queue depth or global pressure |
| **degrade** | Application has a defined cheaper code path (cache hit, stale read, simplified response) |
| **keep** | Normal operation |

**Degrade requires application support** — Keylane signals degrade; your handler or `DegradeHandler` must implement the fallback. Degrade does not happen automatically.

---

## Policy model

```go
type OverloadPolicy struct {
    Default LaneOverloadPolicy
    Lanes   []LaneOverloadPolicy // optional per-lane overrides
}
```

Update at runtime (immutable snapshot, same pattern as quota and admission):

```go
version, err := queue.UpdateOverloadPolicy(keylane.OverloadPolicy{ ... })
snap := queue.CurrentOverloadPolicy()
```

---

## Policy evaluation order

At a high level, evaluation considers:

1. **Global pressure** — depth ratio across the queue
2. **Lane class** — critical tolerates higher pressure than best-effort
3. **Per-lane queue depth** — compare to `MaxQueueDepth` on `LaneOverloadPolicy`
4. **Action** — keep, reject, shed, or degrade per configured rules

Exact reason codes appear on `OverloadError.Decision` and `OverloadPolicyEvent.Reason`.

---

## Retry-After and backoff hints

- **`RetryAfter`** — Suggested duration before retry (Go `time.Duration`)
- **`BackoffHint`** — Structured hint for custom client logic

Backoff hints are advisory. HTTP middleware can expose `Retry-After` when configured; clients may ignore it.

---

## Request integration

```go
future, err := keylane.SubmitRequest(ctx, queue, keylane.Request[I, O]{
    Meta:     meta,
    Overload: keylane.OverloadConfig{Enabled: true},
    Handle:   handle,
})
```

For `Job.Submit`, set `Config.OverloadEnabled: true` on the queue.

Example handling:

```go
decision := /* from overload error or evaluation API */
if decision.Action == keylane.OverloadReject {
    // map to HTTP 429/503; include Retry-After when present
}
```

---

## HTTP middleware

```go
httpkeylane.Middleware(queue, httpkeylane.Config{
    Overload: httpkeylane.OverloadConfig{
        Enabled: true,
        HTTP: httpkeylane.OverloadHTTPConfig{
            EnableRetryAfter: true,
        },
        DegradeHandler: myDegradeHandler, // optional
    },
})
```

Default status mapping:

| Overload action | Default HTTP status |
|-----------------|---------------------|
| `reject` | 503 Service Unavailable |
| `shed` | 429 Too Many Requests |
| `lane_depth_exceeded` | 429 |
| `degrade` | degrade handler, or 503 if none |

Suggested mapping (configurable):

```text
reject  -> 429 Too Many Requests or 503 Service Unavailable
shed    -> 429 Too Many Requests
degrade -> application-defined response
keep    -> continue normal handler path
```

### Retry-After example

When overload rejects with `RetryAfter: 2s` and `EnableRetryAfter: true`:

```text
HTTP/1.1 429 Too Many Requests
Retry-After: 2
```

The header value is whole seconds (`strconv.Itoa` of seconds). Missing `Retry-After` usually means `EnableRetryAfter` is false or `RetryAfter` is zero.

---

## Observability

Per-lane overload decisions in `StatsGCPressure()`:

| Counter | When incremented |
|---------|------------------|
| `OverloadRejected` | `OverloadActionReject` |
| `OverloadShed` | `OverloadActionShed` |
| `OverloadDegrade` | `OverloadActionDegrade` |

Hook (non-keep only):

```go
hooks.OnOverloadPolicyDecision = func(e keylane.OverloadPolicyEvent) { /* ... */ }
```

See [adaptive-observability.md](adaptive-observability.md).

Each non-keep overload decision also increments `Rejected` and `AdmissionRejected` on that lane. Overload rejections do not increase `TotalQueued`.

---

## Adaptive quota integration

When adaptive quota is enabled, per-lane `OverloadRejected` and `OverloadShed` counts feed evaluation ticks. For **background** and **best-effort** lanes, elevated overload counters can trigger a quota **decrease** even when global pressure is below `PressureHigh`.

See [adaptive-quota.md](adaptive-quota.md) and [adaptive-tuning.md](adaptive-tuning.md).

---

## Troubleshooting

### Overload events are missing

- **`OnOverloadPolicyDecision` nil or hooks off** — Enable `Observability.EnableHooks`.
- **Only keep decisions** — Keep does not emit events by design.
- **Overload disabled** — `OverloadConfig.Enabled: false` or queue `OverloadEnabled: false` for jobs.

### Retry-After header is missing

- Set `HTTP.EnableRetryAfter: true` in middleware config.
- Overload decision must set non-zero `RetryAfter`.
- Keep decisions never set retry headers.

### Critical traffic still rejected

- Critical shifts thresholds; it does not disable overload. Check depth and global pressure.
- Consider admission and overload thresholds together — see [lane-priority.md](lane-priority.md).

---

## Benchmarks

```bash
go test -bench='BenchmarkEvaluateOverload|BenchmarkCheckOverload' -benchmem ./internal/core .
```

Expected: **0 allocs/op** on the successful `keep` path when hooks are disabled.

See [benchmarks/adaptive-quota.md](benchmarks/adaptive-quota.md).

---

## Related documentation

- [admission-control.md](admission-control.md)
- [lane-priority.md](lane-priority.md)
- [production-tuning.md](production-tuning.md)
- [adaptive-observability.md](adaptive-observability.md)
