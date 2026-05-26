# go-keylane

![CI](https://github.com/haluan/go-keylane/actions/workflows/ci.yml/badge.svg)

A Go library for routing jobs by key into deterministic execution lanes, improving fairness, isolation, and tail-latency control in high-throughput backend services.

> Status: v0.6.0 (in progress) — failure classification, deadline budget, and bounded retry on top of v0.5 hot key / autoscaling signals. Public APIs may still evolve before a stable v1.0.

---

## Installation

To start using `go-keylane` in your project, install the module via Go CLI:

```bash
go get github.com/haluan/go-keylane
```

---

## Core Concepts

- **Key**: A business identity (e.g., tenant ID, customer ID, order ID) used to route execution deterministically.
- **Lane**: A classification grouping similar jobs (e.g., payment, audit, webhook) with distinct processing priorities.
- **Shard**: A concurrency isolation bucket derived from hashing the job's Key, preventing noisy neighbors from blocking quiet peers.
- **Quota**: The maximum number of jobs from a specific Lane that a worker will process in a single pass over a Shard.
- **Worker**: A goroutine thread running in the scheduler loop that pops ready shards and processes ready lane queues.

---

## Fire-and-Forget Example

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/haluan/go-keylane"
)

func main() {
	cfg := keylane.Config{
		ShardCount:       8,
		WorkerCount:      2,
		QueueSizePerLane: 100,
		LaneQuotas: map[keylane.Lane]int{
			"payment": 3,
			"webhook": 1,
		},
	}

	q, err := keylane.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start workers
	if err := q.Start(ctx); err != nil {
		log.Fatal(err)
	}

	// Submit a fire-and-forget job
	err = q.Submit(ctx, keylane.Job{
		Key:  "tenant-123",
		Lane: "payment",
		Run: func(ctx context.Context) error {
			fmt.Println("Processing payment asynchronously!")
			return nil
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Stop gracefully when done
	_ = q.Stop(ctx, keylane.WithDrain(true))
}
```

---

## SubmitValue & Await Example

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/haluan/go-keylane"
)

func main() {
	q, _ := keylane.New(keylane.Config{
		ShardCount:       4,
		WorkerCount:      2,
		QueueSizePerLane: 100,
		LaneQuotas:       map[keylane.Lane]int{"payment": 1},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	// Submit a job expecting a result
	future, err := keylane.SubmitValue(ctx, q, keylane.ValueJob[string]{
		Key:  "user-456",
		Lane: "payment",
		Run: func(ctx context.Context) (string, error) {
			return "processed_invoice_99", nil
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Await the value with a timeout context
	awaitCtx, awaitCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer awaitCancel()

	result, err := future.Await(awaitCtx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Received result: %s\n", result)
}
```

---

## Non-blocking continuations (KL-1703)

Pipeline stages can **yield** while slow I/O runs outside the Keylane worker (database read, external API, and similar). The stage returns a `Continuation`; when your async work finishes, `ContinuationCompleter.Complete` enqueues a **resume job** on the same key shard. Physical worker identity is not preserved across yield/resume.

Opt in per queue:

```go
cfg.Continuation = keylane.ContinuationConfig{Enabled: true} // MaxPending defaults to 256
```

KL-1703 is a **handoff primitive only**. Backend in-process coordination (KL-1704) and optional pool pressure adapters (`database/sql`, custom API pools — KL-1705) are documented in [backend-resource-coordination.md](docs/backend-resource-coordination.md) and [backend-pressure-adapters.md](docs/backend-pressure-adapters.md).

Guide: [continuations.md](docs/continuations.md) · Pipeline integration: [request-pipeline.md](docs/request-pipeline.md)

```bash
go run ./examples/pipeline_continuation
```

---

## Configuration

The `Config` struct controls how shard isolation, worker pools, and lane-level processing quotas are scoped globally:

| Option | Type | Description |
| :--- | :--- | :--- |
| `ShardCount` | `int` | Number of distinct concurrency buckets. Keys are hashed into these shards. |
| `WorkerCount` | `int` | Number of parallel worker goroutines popping active shards. |
| `QueueSizePerLane` | `int` | Bounded queue capacity allocated per lane inside a single shard. |
| `LaneQuotas` | `map[Lane]int` | The relative execution limits processed per lane in one worker pass. |
| `Observability` | `ObservabilityConfig` | Configuration settings controlling queue wait time, stats, or slow job hooks. |

---

## v0.6.0 — Retry, Deadline & Failure Policy

v0.6.0 makes failure handling explicit: retries are **classified**, **bounded**, **deadline-aware**, **duplicate-safe**, **pressure-aware**, and **observable**. Retry is disabled by default.

Full guide: [v0.6.0 overview](docs/v0.6-retry-deadline-failure-policy.md).

### Minimal retry (safe read)

```go
future, _ := keylane.SubmitValue(ctx, q, keylane.ValueJob[string]{
    Key: "user-123", Lane: "read",
    Retry: keylane.RetryPolicy{Enabled: true, MaxAttempts: 3},
    Idempotency: keylane.Idempotency{Safety: keylane.RetrySafetySafe},
    Run: fetchProfile,
})
```

### Unsafe mutation (no automatic retry)

```go
future, _ := keylane.SubmitValue(ctx, q, keylane.ValueJob[string]{
    Key: "payment-123", Lane: "payment",
    Retry: keylane.RetryPolicy{Enabled: true, MaxAttempts: 3},
    Idempotency: keylane.Idempotency{Safety: keylane.RetrySafetyUnsafe},
    Run: chargePayment,
})
```

### Retry trace

```go
trace, ok := keylane.RetryTraceFromFuture(future)
if ok {
    fmt.Println(trace.Final.Succeeded, trace.Final.SuppressionReason)
}
```

Key docs: [retry policy](docs/retry-policy.md) · [failure policy](docs/failure-policy.md) · [idempotency](docs/idempotency.md) · [retry suppression](docs/retry-suppression.md) · [observability](docs/retry-observability.md)

Runnable examples: `go run ./examples/retry_policy`, `./examples/deadline_budget`, `./examples/idempotency_retry`, `./examples/retry_suppression`, `./examples/failure_observability`.

---

## v0.4 Adaptive Quota & Overload Policy

Keylane can optionally react to runtime pressure with bounded quota updates, lane priority classes, per-lane admission policy, overload decisions, and backoff hints.

Start here:

- [Adaptive Quota](docs/adaptive-quota.md)
- [Lane Priority](docs/lane-priority.md)
- [Overload Policy](docs/overload-policy.md)
- [Adaptive Tuning](docs/adaptive-tuning.md)
- [Adaptive Observability](docs/adaptive-observability.md)
- [Adaptive Benchmarks](docs/benchmarks/adaptive-quota.md)

---

## Request Runtime (v0.3)

Keylane can run request-scoped work through a lane-sharded fairness runtime.

Use `SubmitRequest[I, O]` for transport-agnostic typed request execution, or use `httpkeylane.Middleware` to integrate with `net/http`. Both paths support cancellation, timeout semantics, pressure-based admission control, and per-request observability.

See:

- [Request Runtime](docs/request-runtime.md) — `SubmitRequest[I,O]`, `RequestMeta`, `Future.Await`, key routing, lane fairness
- [Request Pipeline](docs/request-pipeline.md) — `SubmitPipeline`, ordered stages, stage observability (v0.7 KL-1701)
- [Stage Execution Context](docs/stage-execution-context.md) — `StageExecutionFromContext`, attempt, deadline snapshot (v0.7 KL-1702)
- [HTTP Middleware](docs/http-middleware.md) — `httpkeylane.Middleware`, key and lane helpers, route rules, status codes
- [Cancellation and Timeout](docs/cancellation-timeout.md) — cooperative cancellation, await semantics, non-guarantees
- [Admission Control](docs/admission-control.md) — pressure-based request gating, 503/429, process-local scope
- [Request Observability](docs/request-observability.md) — `RequestObservation`, outcomes, operation naming, cardinality guidance

---

## What go-keylane is

`go-keylane` is a Go lane-sharded concurrency control library for shaping request execution in-process. It helps services bound in-flight work, reduce goroutine explosion, smooth allocation bursts, and expose production visibility into queue wait, run duration, hot shards, hot lanes, and pressure.

It provides:

- **Noisy key/tenant isolation** — deterministic shard routing by `Key`
- **Fair resource distribution** — lane quotas across workload classes on a shared worker pool
- **Bounded queues and workers** — map-free ring buffers and optional pooling on worker paths

---

## What go-keylane is NOT

`go-keylane` is **not**:
- A replacement for the Go scheduler or the OS scheduler.
- A distributed queue or message broker (like RabbitMQ or Redis).
- A persistent job system (it operates entirely in-memory and state is lost on process restart).

> [!IMPORTANT]
> **go-keylane does not avoid Go GC pauses.**
> go-keylane helps shape GC pressure caused by uncontrolled concurrency, goroutine explosion, unbounded queues, and allocation bursts. See [docs/gc-pressure-shaping.md](docs/gc-pressure-shaping.md).

---

## Await Deadlock Risk Warning

> [!CAUTION]
> **Never call `Await` inside a worker `Run` function on the same queue.**
>
> Doing so creates a high risk of **resource exhaustion deadlocks**. If your `WorkerCount` is small (e.g. 1) and that worker picks up a job that blocks on `Await` for another job submitted to the *same* queue, the worker will wait forever, deadlock the scheduler, and starve all other tasks.
>
> **Safe Alternatives:**
> - Submit separate fire-and-forget jobs and coordinate results in the outer caller context using standard tools like `sync.WaitGroup`.
> - Use independent `Queue` instances if jobs must have caller-dependent execution pipelines.

---

## Continuous integration

GitHub Actions runs on every pull request and on pushes to `main` (see [`.github/workflows/ci.yml`](.github/workflows/ci.yml)):

- `gofmt` check
- `go mod tidy` integrity (root `go.mod` / `go.sum`)
- `go vet` and `go test` for the root module and adapter modules (`httpkeylane`, `metrics/prometheus`, `tracing/otel`)
- `go test -race` for the same modules (separate job)

Run the same checks locally:

```bash
make ci
make ci-race
```

Or step by step:

```bash
go mod tidy
git diff --exit-code go.mod
# if go.sum exists:
# git diff --exit-code go.sum
test -z "$(gofmt -l .)"
go vet ./...
go test ./...
go test -race ./...
cd httpkeylane && go vet ./... && go test ./...
cd httpkeylane && go test -race ./...
cd metrics/prometheus && go test ./...
cd tracing/otel && go test ./...
```

---

## Documentation

### v0.6.0 failure classification, deadline budget & retry

- [v0.6.0 Overview](docs/v0.6-retry-deadline-failure-policy.md)
- [Retry Policy](docs/retry-policy.md)
- [Failure Policy](docs/failure-policy.md)
- [Deadline Budget](docs/deadline-budget.md)
- [Idempotency & Retry Safety](docs/idempotency.md)
- [Retry Suppression](docs/retry-suppression.md)
- [Failure-Aware Admission](docs/failure-aware-admission.md)
- [Failure Observability](docs/failure-observability.md)
- [Retry Observability](docs/retry-observability.md)

### v0.5 hot key, shard pressure & autoscaling

- [v0.5 Overview](docs/v0.5-hot-key-autoscaling-signals.md)
- [Hot Key Detection](docs/hot-key-detection.md)
- [Per-Key Admission Policy](docs/per-key-admission-policy.md)
- [Shard Pressure Diagnostics](docs/shard-pressure-diagnostics.md)
- [Autoscaling Signals](docs/autoscaling-signals.md)
- [DebugSnapshot](docs/debug-snapshot.md)
- [Configuration](docs/configuration.md)
- [Metrics](docs/metrics.md)
- [Hot Key & Scale Pressure Runbook](docs/runbooks/hot-key-and-scale-pressure.md)
- [v0.5.0 Release Notes](docs/releases/v0.5.0.md)

### v0.4 adaptive quota and overload

- [Adaptive Quota](docs/adaptive-quota.md)
- [Lane Priority](docs/lane-priority.md)
- [Overload Policy](docs/overload-policy.md)
- [Adaptive Tuning](docs/adaptive-tuning.md)
- [Adaptive Observability](docs/adaptive-observability.md)
- [Adaptive Benchmarks](docs/benchmarks/adaptive-quota.md)
- [v0.4.0 Release Notes](docs/releases/v0.4.0.md)

### v0.3 request runtime

- [Request Runtime](docs/request-runtime.md)
- [HTTP Middleware](docs/http-middleware.md)
- [Cancellation and Timeout](docs/cancellation-timeout.md)
- [Admission Control](docs/admission-control.md)
- [Request Observability](docs/request-observability.md)
- [v0.3 Testing Guide](docs/v0.3-request-runtime-testing.md)
- [v0.3.0 Release Notes](docs/releases/v0.3.0.md)

### v0.2 guides

- [GC pressure shaping](docs/gc-pressure-shaping.md)
- [Observability](docs/observability.md)
- [Debugging](docs/debugging.md)
- [Production tuning](docs/production-tuning.md)
- [Benchmarks](docs/benchmarks.md)
- [Prometheus adapter](docs/metrics-prometheus.md)
- [OpenTelemetry adapter](docs/tracing-opentelemetry.md)
- [Release notes](docs/releases/README.md)

### Architecture and operations

- [Quickstart Guide](docs/quickstart.md)
- [Architecture & Design Details](docs/design.md)
- [Operational & Production Guidance](docs/production-guidance.md)
- [Glossary of Terms](docs/glossary.md)

---

## License

`go-keylane` is distributed under the GNU General Public License v3. See the [LICENSE](LICENSE) file for details.
