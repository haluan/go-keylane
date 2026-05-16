# Phase 4 — Future / SubmitValue / Await

Phase 4 adds typed return-value support on top of the fire-and-forget scheduler from Phase 3.

## Overview

Users can submit a `ValueJob[T]` and receive a `Future[T]` they can `Await` from the caller side.

### Key Types

#### ValueJob[T]
A generic job definition that returns a value of type `T` and an error.

```go
type ValueJob[T any] struct {
    Key  string
    Lane Lane
    Run  func(context.Context) (T, error)
}
```

#### Future[T]
An interface representing a result that will be available later.

```go
type Future[T any] interface {
    Await(context.Context) (T, error)
    Done() <-chan struct{}
}
```

## Usage

```go
future, err := keylane.SubmitValue(ctx, q, keylane.ValueJob[int]{
    Key:  "customer-123",
    Lane: "payment",
    Run: func(ctx context.Context) (int, error) { 
        return 42, nil 
    },
})

// Block until result or context cancellation
value, err := future.Await(ctx)
```

## Semantics

### First-Completion Wins
The internal `resultFuture` uses `sync.Once` to ensure that only the first call to `complete` (from the job runner) determines the result. Subsequent calls are ignored.

### Await and Context
`Await(ctx)` waits for the job to complete or for the provided context to be cancelled.
> [!IMPORTANT]
> Cancelling the `Await` context does **not** cancel the underlying job execution. The job will continue to run to completion in the background.

### Error Handling
`SubmitValue` always returns a non-nil `Future`, even if the initial submission fails (e.g., due to a full queue or invalid configuration). In such cases, the `Future` is pre-completed with the error, and `Await` will return that error immediately.

## Deadlock Warning: Avoid Await inside Workers

Never call `Await` on a future inside a `Run` function that is executed by the same `Queue`. 

### Deadlock Example

If your `WorkerCount` is 1, and the single worker processes a job that then blocks on `Await` for another job in the same queue, the system will **deadlock forever**.

```go
q, _ := keylane.New(keylane.Config{WorkerCount: 1, ...})
_ = q.Start(ctx)

// This job will DEADLOCK
_ = q.Submit(ctx, keylane.Job{
    Key: "j1", Lane: "default", Run: func(ctx context.Context) error {
        f, _ := keylane.SubmitValue(ctx, q, otherJob)
        val, _ := f.Await(ctx) // Blocks the only worker; otherJob never runs
        return nil
    },
})
```

### Safe Alternatives
*   **Independent Submission**: Submit all required jobs from the caller side and aggregate results there using a `sync.WaitGroup` or by awaiting multiple futures sequentially.
*   **Decoupled Queues**: If jobs must wait for each other, use separate `Queue` instances to avoid circular dependencies in the worker pool.
*   **Larger Worker Pools**: While increasing `WorkerCount` can mitigate the issue, it only delays the problem. Architecture should ideally avoid worker-side blocking.

### Await Timeout and Starvation
Using `Await` with a timeout (e.g., `context.WithTimeout`) prevents the **caller** from blocking indefinitely. However, it does **not** solve the problem of **scheduler starvation**. If a worker is stuck in an `Await` call, it is unavailable to process other shards until the call returns (either through completion or timeout). The timeout merely protects the caller, not the queue's throughput.
