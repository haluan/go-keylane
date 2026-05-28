# Go-Keylane Quickstart Guide

Welcome to the Quickstart Guide! This page walks you through the step-by-step process of installing `go-keylane`, configuring an active queue, submitting your first jobs, and performing a clean shutdown.

---

## 1. Installation

Add `go-keylane` to your active Go module workspace:

```bash
go get github.com/haluan/go-keylane
```

---

## 2. Configuration Setup

For v0.8, start with production-safe defaults and validate before constructing the queue:

```go
import "github.com/haluan/go-keylane"

cfg := keylane.ProductionDefaults()
// Tune lanes and capacity for your service, then validate:
report := keylane.ValidateConfig(cfg)
if report.HasErrors() {
    panic(report.Err())
}

q, err := keylane.New(cfg)
if err != nil {
    panic(err)
}
```

Hand-rolled `Config` is fine for learning. Set `ShardCount`, `WorkerCount`, `QueueSizePerLane`, and `LaneQuotas` explicitly. See [production-minimal.md](production-minimal.md) and [production-hardening.md](production-hardening.md).

---

## 3. Starting the Scheduler Loop

Workers are managed via standard Go contexts. Starting the scheduler allocates worker goroutines and listens for enqueued jobs:

```go
import "context"

ctx, cancel := context.WithCancel(context.Background())
defer cancel()

// Start the worker scheduler loop
if err := q.Start(ctx); err != nil {
    panic(err)
}
```

---

## 4. Submitting Work

`go-keylane` provides two entry pathways to execute your code:

### Pathway A: Fire-and-Forget Job (`q.Submit`)
Use `Submit` when you want to queue an action and immediately continue without waiting for the outcome:

```go
job := keylane.Job{
    Key:  "tenant-alpha",
    Lane: "webhook",
    Run: func(ctx context.Context) error {
        // Asynchronous processing logic here
        return nil
    },
}

if err := q.Submit(ctx, job); err != nil {
    // Fails with ErrQueueFull if the webhook queue is saturated
    panic(err)
}
```

### Pathway B: Request-Response Value Job (`keylane.SubmitValue`)
Use `SubmitValue` when the caller needs to block and await a return value or a specific execution outcome:

```go
valJob := keylane.ValueJob[int]{
    Key:  "tenant-alpha",
    Lane: "payment",
    Run: func(ctx context.Context) (int, error) {
        // Computation or API transaction call
        return 42, nil
    },
}

future, err := keylane.SubmitValue(ctx, q, valJob)
if err != nil {
    panic(err)
}

// Block the current thread to receive the computation result
val, err := future.Await(ctx)
if err != nil {
    panic(err)
}

fmt.Printf("Received payment return: %d\n", val)
```

---

## 5. Graceful Teardown

To shut down cleanly and ensure that enqueued jobs are completely processed before workers exit, call `Stop` with `WithDrain(true)`:

```go
// Block until all queued jobs have finished processing
err := q.Stop(ctx, keylane.WithDrain(true))
if err != nil {
    fmt.Printf("Graceful teardown returned error: %v\n", err)
}
```
