# Overload Policy

KL-1403 adds an overload policy engine that evaluates runtime pressure, lane class, and queue state before enqueue. It returns a structured decision (`keep`, `reject`, `shed`, `degrade`) and optional backoff hints.

Overload policy only applies to **new** submissions. It does not drop queued work or cancel running jobs.

## Actions

| Action | Meaning |
|--------|---------|
| `keep` | Enqueue normally |
| `reject` | Reject before enqueue with `ErrOverloadRejected` |
| `shed` | Intentional pre-enqueue load shedding (`ErrOverloadShed`) |
| `degrade` | Do not enqueue; caller/middleware may run a cheaper fallback (`ErrOverloadDegraded`) |

Core Keylane does not sleep, retry, or build business fallback responses. `RetryAfter` and `BackoffHint` are guidance for callers and HTTP middleware.

## Policy model

```go
type OverloadPolicy struct {
    Default LaneOverloadPolicy
    Lanes   []LaneOverloadPolicy // optional per-lane overrides
}
```

Update at runtime (immutable snapshot, same pattern as quota and admission policy):

```go
version, err := queue.UpdateOverloadPolicy(keylane.OverloadPolicy{ ... })
snap := queue.CurrentOverloadPolicy()
```

## Request integration

```go
future, err := keylane.SubmitRequest(ctx, queue, keylane.Request[I, O]{
    Meta:     meta,
    Overload: keylane.OverloadConfig{Enabled: true},
    Handle:   handle,
})
```

When both `Overload` and `Admission` are enabled, **overload runs first**. Prefer overload-only for new integrations.

For `Job.Submit`, set `Config.OverloadEnabled: true` on the queue.

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

- `reject` → 503 Service Unavailable
- `shed` → 429 Too Many Requests
- `lane_depth_exceeded` → 429
- `degrade` → degrade handler, or 503 if none

## Observability

Per-lane overload decisions are exposed via `Queue.StatsGCPressure()`:

| Counter | When incremented |
|---------|------------------|
| `OverloadRejected` | `OverloadActionReject` before enqueue |
| `OverloadShed` | `OverloadActionShed` before enqueue |
| `OverloadDegrade` | `OverloadActionDegrade` before enqueue |

```go
snap := queue.StatsGCPressure()
for _, lane := range snap.Lanes {
    _ = lane.Counters.OverloadRejected
    _ = lane.Counters.OverloadShed
    _ = lane.Counters.OverloadDegrade
}
```

Each non-keep overload decision also increments `Rejected` and `AdmissionRejected` on that lane. Overload rejections do not increase `TotalQueued`.

Structured reason codes and policy version are available on `OverloadError.Decision` and `DebugSnapshot.OverloadPolicyVersion`.

## Benchmarks

```bash
go test -bench='BenchmarkEvaluateOverload|BenchmarkCheckOverload' -benchmem ./internal/core .
```

Expected: **0 allocs/op** on the successful `keep` path.

See also [admission-control.md](admission-control.md) and [production-tuning.md](production-tuning.md).
