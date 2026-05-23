# Request Observability

The request runtime emits structured metadata at each lifecycle stage: queued, started, completed, and rejected. This data is available through request hooks and an optional HTTP-specific observe callback.

---

## Overview

Observability is optional and zero-allocation when disabled. When hooks are nil and `Observe` is not configured, Keylane does not copy headers, query strings, or path segments. No label maps are allocated per request.

Enable hooks via `ObservabilityConfig`:

```go
obs := keylane.DefaultObservabilityConfig()
obs.Hooks.Request = keylane.RequestHooks{
    OnQueued:    func(m keylane.RequestMeta) { /* request accepted into queue */ },
    OnStarted:   func(o keylane.RequestObservation) { /* worker started handler */ },
    OnCompleted: func(o keylane.RequestObservation) { /* handler returned */ },
    OnRejected:  func(o keylane.RequestObservation) { /* request rejected before or after submit */ },
}
cfg.Observability = obs
```

---

## RequestObservation Fields

`RequestObservation` is the snapshot passed to `OnStarted`, `OnCompleted`, and `OnRejected` hooks, and to the HTTP `ObserveFunc`.

```go
type RequestObservation struct {
    RequestID string         // from RequestMeta.RequestID
    Key       string         // routing key
    Lane      Lane           // execution lane
    ShardID   int            // shard index (hash(Key) % ShardCount)

    Transport string         // transport name, e.g. "http"
    Operation string         // stable operation name, e.g. "POST /payments"

    QueueWait time.Duration  // time from enqueue to worker start (zero for rejected requests)
    Run       time.Duration  // handler execution time (zero for skipped/rejected requests)

    Outcome RequestOutcome   // classification of how the request ended
    Err     error            // underlying error, if any
}
```

`QueueWait` and `Run` are populated in `OnStarted` and `OnCompleted`. They are zero for requests rejected before or during enqueue (`OnRejected`).

---

## RequestOutcome Values

| Outcome | Constant | Trigger |
|---------|----------|---------|
| `"completed"` | `RequestOutcomeCompleted` | Handler returned nil error |
| `"failed"` | `RequestOutcomeFailed` | Handler returned a non-context error |
| `"cancelled"` | `RequestOutcomeCancelled` | `context.Canceled` |
| `"timed_out"` | `RequestOutcomeTimedOut` | `context.DeadlineExceeded` |
| `"rejected"` | `RequestOutcomeRejected` | Queue full, stopped, or not started |
| `"admission_rejected"` | `RequestOutcomeAdmissionRejected` | Pressure-based admission rejected |

---

## HTTP Status Capture

When using `httpkeylane.Middleware`, the HTTP status code written by the handler is captured and passed to the `ObserveFunc` callback:

```go
type ObserveFunc func(HTTPRequestMetadata, keylane.RequestObservation)

type HTTPRequestMetadata struct {
    Method     string
    Path       string
    StatusCode int // first status written by handler or middleware; 0 if no write occurred
}
```

`StatusCode` reflects the first `WriteHeader` call. If the handler writes a body without calling `WriteHeader`, the status is 200.

---

## Operation Names

`RequestMeta.Operation` is an optional stable operation name for observability labels. Set it via `OperationFunc` in the HTTP middleware config:

```go
cfg := httpkeylane.Config{
    KeyFunc:  httpkeylane.HeaderKey("X-Tenant-ID"),
    LaneFunc: httpkeylane.MethodLaneMapper(),
    OperationFunc: func(r *http.Request) string {
        return r.Method + " /payments"
    },
}
```

---

## Cardinality Guidance

**Do not use raw HTTP paths as operation names.** Raw paths create an unbounded label set:

```text
Bad:  /users/123, /users/456, /payments/pay_abc, /payments/pay_xyz
Good: GET /users/{id}, POST /payments
```

High-cardinality labels cause metric storage explosion in Prometheus and similar systems. Use stable, parameterized operation names with low cardinality (< 100 distinct values is a safe rule of thumb).

The `Path` field in `HTTPRequestMetadata` contains the raw request path — it is available for logging but should not be used as a metric label without normalization.

---

## RequestHooks

`RequestHooks` are set on `ObservabilityConfig.Hooks.Request`:

```go
type RequestHooks struct {
    OnQueued    func(RequestMeta)          // fires after successful enqueue
    OnStarted   func(RequestObservation)   // fires when worker begins handling
    OnCompleted func(RequestObservation)   // fires after handler returns
    OnRejected  func(RequestObservation)   // fires when request is rejected at any stage
}
```

Hooks fire synchronously on the worker goroutine (`OnStarted`, `OnCompleted`) or on the calling goroutine (`OnQueued`, `OnRejected`). Keep hooks fast and non-blocking.

---

## Low-Allocation Mode

When hooks are nil and `Observe` is not set:
- No `RequestObservation` is allocated per request.
- No headers, query strings, or path segments are copied.
- No label maps are created.

When only `Observe` is set (no `RequestHooks`), observations are built only at request completion — not at start — reducing allocations for high-throughput paths.

---

## Full Example

```go
obs := keylane.DefaultObservabilityConfig()
obs.Hooks.Request = keylane.RequestHooks{
    OnCompleted: func(o keylane.RequestObservation) {
        fmt.Printf("key=%s lane=%s shard=%d outcome=%s queue_wait=%s run=%s\n",
            o.Key, o.Lane, o.ShardID, o.Outcome, o.QueueWait, o.Run)
    },
    OnRejected: func(o keylane.RequestObservation) {
        fmt.Printf("rejected key=%s outcome=%s err=%v\n", o.Key, o.Outcome, o.Err)
    },
}

queue, _ := keylane.New(keylane.Config{
    ShardCount:       4,
    WorkerCount:      2,
    QueueSizePerLane: 100,
    LaneQuotas:       map[keylane.Lane]int{"write": 2, "read": 4},
    Observability:    obs,
})
```

For HTTP-specific observability (method, path, status code), use `Config.Observe` in `httpkeylane.Middleware`. See [http-middleware.md](http-middleware.md).
