# Request Runtime

The request runtime is the typed, request-scoped execution layer added in v0.3. It wraps Keylane's lane-sharded scheduler with structured input/output, metadata routing, cancellation semantics, and observability hooks.

---

## Overview

The core path:

```text
caller
  -> SubmitRequest[I, O]
  -> validate RequestMeta
  -> route by Key
  -> enqueue into Lane
  -> worker starts request
  -> handler runs
  -> Future completes
  -> Await returns output/error
```

Use the request runtime when you need typed responses, request-level observability, or HTTP middleware integration. For fire-and-forget work without a return value, `Queue.Submit` is simpler.

---

## Core API

```go
// Submit a typed request and get a Future.
func SubmitRequest[I any, O any](
    ctx context.Context,
    q *Queue,
    req Request[I, O],
) (Future[O], error)

// Await the result. Blocks until the request completes or ctx is cancelled.
func (f Future[O]) Await(ctx context.Context) (O, error)
```

---

## RequestMeta

`RequestMeta` holds routing and identity fields for a request. All fields except `Key` and `Lane` are optional.

```go
type RequestMeta struct {
    RequestID string  // optional caller-provided identity (not used for routing)
    Key       string  // routes the request to a shard (required)
    Lane      Lane    // selects the workload class queue (required)
    Transport string  // optional transport name, e.g. "http" or "worker"
    Operation string  // optional stable low-cardinality operation name
}
```

- `Key` must be non-empty. The scheduler hashes it to pick a shard.
- `Lane` must be a valid, configured lane. An empty lane is rejected.
- `Transport` is set automatically to `"http"` by `httpkeylane.Middleware`. Set it manually for other transports.
- `Operation` is for observability only. Use a stable, low-cardinality value like `"create-payment"`, not a raw URL path.

---

## Request[I, O]

`Request[I, O]` bundles the metadata, typed input, and handler function.

```go
type Request[I any, O any] struct {
    Meta      RequestMeta
    Admission AdmissionConfig  // optional per-request admission override
    Input     I
    Handle    func(context.Context, I) (O, error)
}
```

The handler receives a context derived from the context passed to `SubmitRequest`. If the request context is cancelled while the handler is running, the same context is passed to `Handle` — the handler should observe `ctx.Done()` for cooperative cancellation.

---

## SubmitRequest[I, O]

```go
future, err := keylane.SubmitRequest(ctx, queue, req)
if err != nil {
    // request was not enqueued: ctx cancelled, invalid meta, admission rejected, queue full
    return err
}
```

`SubmitRequest` returns immediately. The handler runs when a worker picks up the job. The returned `Future` resolves when the handler returns.

**Validation order:**
1. `q == nil` → `ErrNilQueue`
2. `req.Meta.Key == ""` → `ErrInvalidKey`
3. `req.Meta.Lane` invalid → `ErrInvalidLane`
4. `req.Handle == nil` → error
5. `ctx.Err() != nil` → context error
6. Admission check → `ErrAdmissionRejected`
7. `q.Submit` → `ErrQueueFull`, `ErrStopped`, etc.

---

## Future and Await

```go
out, err := future.Await(ctx)
if err != nil {
    // handler returned an error, or request was cancelled/timed out
    return err
}
// use out
```

`Await` blocks until the request completes or until `ctx` is cancelled. The context passed to `Await` controls **how long the caller waits** — it does not cancel the underlying request. See [cancellation-timeout.md](cancellation-timeout.md) for the distinction.

---

## Key Routing

The scheduler hashes `Key` to select a shard:

```text
shard = hash(Key) % ShardCount
```

Requests with the same key always route to the same shard. This provides noisy-neighbor isolation: heavy traffic from one key does not starve another key on a different shard.

**Choose keys that represent stable business identities:** tenant ID, customer ID, order ID. Avoid high-churn ephemeral keys (random UUIDs, timestamps) — they scatter work uniformly across shards and defeat isolation.

---

## Lane Fairness

Within a shard, each lane has a bounded queue and a quota. The scheduler processes up to `quota` jobs from a lane before moving to the next. This ensures fairness between workload classes even when one lane is heavily loaded.

```text
shard
  ├── lane: "payment"  quota=3  queue=[job, job, job, ...]
  └── lane: "webhook"  quota=1  queue=[job, ...]
```

Configure lanes and quotas via `Config.LaneQuotas`. Every `Key` used in `RequestMeta.Lane` must appear in `LaneQuotas`.

---

## Shard Identity

A shard is a scheduling isolation unit — a lock bucket and a set of per-lane queues. The scheduler assigns workers to shards dynamically. Keylane does not assign a fixed worker to a fixed shard. The scheduler chooses which worker handles which shard internally.

Do not rely on worker identity as a user-visible guarantee.

---

## Full Example

```go
type Input struct {
    TenantID string
    Amount   int64
}

type Output struct {
    PaymentID string
}

req := keylane.Request[Input, Output]{
    Meta: keylane.RequestMeta{
        RequestID: "req-123",
        Key:       "tenant-42",
        Lane:      keylane.Lane("write"),
        Operation: "create-payment",
    },
    Input: Input{
        TenantID: "tenant-42",
        Amount:   125000,
    },
    Handle: func(ctx context.Context, in Input) (Output, error) {
        // handler runs inside the scheduler
        return Output{PaymentID: "pay_123"}, nil
    },
}

future, err := keylane.SubmitRequest(ctx, queue, req)
if err != nil {
    return err
}

out, err := future.Await(ctx)
if err != nil {
    return err
}

_ = out.PaymentID
```

---

## Transport-Agnostic Usage

`SubmitRequest` is not HTTP-specific. Set `Transport` to identify the caller:

```go
meta := keylane.RequestMeta{
    Key:       workerID,
    Lane:      keylane.Lane("background"),
    Transport: "worker",
    Operation: "reconcile-accounts",
}
```

`httpkeylane.Middleware` sets `Transport = "http"` automatically.

---

## Guarantees

- Same key always routes to the same shard (for a given `ShardCount`).
- `Future.Await` is safe to call from multiple goroutines.
- Handler errors are propagated through `Future.Await`.
- `OnQueued`, `OnStarted`, `OnCompleted`, `OnRejected` hooks fire at lifecycle boundaries.

## Non-Guarantees

- Keylane does not assign a fixed worker per shard.
- Keylane does not guarantee execution order across different keys.
- Keylane does not persist requests across process restarts.
- Keylane does not forcibly cancel running handlers. See [cancellation-timeout.md](cancellation-timeout.md).

---

## Request pipelines (v0.7)

For multi-step handlers with stage-level observability, use [`SubmitPipeline`](request-pipeline.md). Pipelines reuse the same routing, admission, retry, and `Future` semantics as `SubmitRequest`. Stages run sequentially; optional non-blocking yield/resume is enabled with [`ContinuationConfig`](continuations.md).

Both APIs attach [`StageExecutionContext`](stage-execution-context.md) to the handler context so stages can read shard, stage, attempt, and deadline metadata via `StageExecutionFromContext`.

When request-level retry is enabled, `exec.Deadline` in that context is refreshed at the start of each retry attempt (queue wait and runtime still accumulate on the underlying budget). Stages and handlers should read `exec.Deadline` per attempt rather than caching it across failures.
