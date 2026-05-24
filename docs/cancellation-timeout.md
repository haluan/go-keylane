# Cancellation and Timeout

Keylane uses Go context cancellation semantics throughout the request runtime. It does not forcibly kill running handler goroutines. Handlers must observe `context.Context` for cooperative cancellation.

---

## Overview

Two contexts are relevant in the request runtime:

- **Request context** — passed to `SubmitRequest`. Controls request execution: submission, queueing, and handler invocation.
- **Await context** — passed to `Future.Await`. Controls how long the caller blocks waiting for the result.

These are independent. You can cancel `Await` without affecting the underlying request.

---

## Cooperative Cancellation

Keylane cannot forcibly interrupt arbitrary Go code. A handler that ignores `ctx.Done()` may continue until it returns.

For handlers that do blocking work, observe the context:

```go
Handle: func(ctx context.Context, in Input) (Output, error) {
    select {
    case result := <-doWork():
        return result, nil
    case <-ctx.Done():
        return Output{}, ctx.Err()
    }
}
```

For HTTP handlers wrapped by `httpkeylane.Middleware`, the context is `r.Context()` from the incoming request.

---

## Before Enqueue

If the request context is already cancelled when `SubmitRequest` is called, the request is rejected immediately and not enqueued:

```go
ctx, cancel := context.WithCancel(context.Background())
cancel() // already cancelled

_, err := keylane.SubmitRequest(ctx, queue, req)
// err is context.Canceled — request was never submitted
```

---

## While Queued

If the request context is cancelled after `SubmitRequest` returns but before a worker picks up the job, the worker skips the handler when it dequeues the job:

```text
queued request
  -> context cancelled
  -> worker dequeues job
  -> runtime checks ctx.Err()
  -> handler is skipped
  -> Future completes with context error
```

The `Future` completes with `context.Canceled` and `Await` returns that error.

---

## While Running

If the request context is cancelled while the handler is executing, the handler receives the same context. The handler observes `ctx.Done()` and returns early:

```go
Handle: func(ctx context.Context, in Input) (Output, error) {
    if err := ctx.Err(); err != nil {
        return Output{}, err
    }
    // continue with work
    return doWork(ctx, in)
}
```

The middleware (via `r.Context().Done()`) or the caller is responsible for cancelling the context. Keylane propagates the context but does not inject additional cancellation.

---

## Await Timeout

`Future.Await` accepts a separate context that controls how long the caller waits:

```go
future, err := keylane.SubmitRequest(requestCtx, queue, req)
if err != nil {
    return err
}

awaitCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
defer cancel()

out, err := future.Await(awaitCtx)
if errors.Is(err, context.DeadlineExceeded) {
    // The caller stopped waiting.
    // The underlying request may still complete if requestCtx is still active.
    return err
}
```

**Important:** `Await` timeout means the caller stopped waiting. It does not cancel the underlying request. If `requestCtx` is still active, the handler may complete and the `Future` will hold the result — but the caller has already moved on.

---

## Caller Abandonment

In the HTTP middleware, `r.Context()` serves as both the request context and the await context. When the HTTP client disconnects, `r.Context()` is cancelled. This causes:

1. `Future.Await` to return with `context.Canceled`.
2. The handler (if already running) to receive a cancelled context via `r.Context()`.
3. The middleware to return without writing a response.

A handler that ignores `r.Context().Done()` will continue running until it returns. The scheduler does not terminate it. The response will be discarded since the connection is closed.

---

## Non-Guarantees

- Keylane cannot forcibly interrupt arbitrary Go code.
- A handler that ignores `ctx.Done()` may continue until it returns naturally.
- `Await` timeout does not cancel the underlying request execution.
- Keylane does not replace application-level timeout design. If a handler must complete within a deadline, the handler itself must enforce that deadline.

---

## Failure classification

v0.6 maps context and scheduler errors to `FailureKind` for structured handling:

| Situation | `FailureKind` |
|-----------|---------------|
| `context.Canceled` on request context | `cancelled` |
| `context.DeadlineExceeded` while handler runs | `timeout` |
| Deadline expired before handler (queued) | `deadline_exhausted` |
| `Await` context timeout (caller wait only) | Classify via `ClassifyFailure(awaitErr)` — typically `cancelled` or `timeout` on await ctx |

```go
failure := keylane.ClassifyFailure(awaitErr)
_ = failure.Kind
```

See [failure-policy.md](failure-policy.md) and [deadline-budget.md](deadline-budget.md).
