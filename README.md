# go-keylane

![CI](https://github.com/haluan/go-keylane/actions/workflows/ci.yml/badge.svg)

A Go library for routing jobs by key into deterministic execution lanes, improving fairness, isolation, and tail-latency control in high-throughput backend services.

> Status: v0.8.0 (pre-v1.0) — v0.7 pipelines, continuations, and backend coordination on top of v0.6 retry/deadline/failure policy. See [API stability](docs/api-stability.md) and [public API inventory](docs/public-api-inventory.md).

---

## Installation

To start using `go-keylane` in your project, install the module via Go CLI:

```bash
go get github.com/haluan/go-keylane
```

---

## v0.8.0 adoption (recommended)

Start with the canonical example and walkthrough:

```bash
go run ./examples/production-minimal
```

- [production-minimal example](examples/production-minimal/) · [walkthrough](docs/production-minimal.md)
- [production hardening hub](docs/production-hardening.md) · [v0.8.0 release notes](docs/releases/v0.8.0.md)
- [examples guide](docs/examples.md) · [migration v0.7 → v0.8](docs/migration/v0.7-to-v0.8.md)
- `ProductionDefaults()` + `ValidateConfig` before `New`
- [observability contract](docs/observability-contract.md) · [lifecycle / shutdown](docs/runtime-lifecycle-hardening.md) · [performance baselines](docs/performance-regression.md)

Compile-check all examples: `make verify-examples`.

**Not a workflow engine, distributed queue, or exactly-once system** — in-process lane-sharded concurrency control only. State is lost on process restart.

---

## Core Concepts

- **Key**: A business identity (e.g., tenant ID, customer ID, order ID) used to route execution deterministically.
- **Lane**: A classification grouping similar jobs (e.g., payment, audit, webhook) with distinct processing priorities.
- **Shard**: A concurrency isolation bucket derived from hashing the job's Key, preventing noisy neighbors from blocking quiet peers.
- **Quota**: The maximum number of jobs from a specific Lane that a worker will process in a single pass over a Shard.
- **Worker**: A goroutine thread running in the scheduler loop that pops ready shards and processes ready lane queues.

---

## Fire-and-Forget Example

> Prefer [examples/production-minimal](examples/production-minimal/) (`ProductionDefaults`, `ValidateConfig`, graceful `Stop` with drain). The snippet below uses a hand-rolled `Config` for illustration.

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

> See also [examples/submit-value-await](examples/submit-value-await/) and [docs/production-minimal.md](docs/production-minimal.md).

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

## v0.7.0 — Advanced Request Pipeline & Backend Resource Coordination

Full guide: [v0.7.0 overview](docs/v0.7-advanced-request-pipeline-and-resource-coordination.md) · Release notes: [v0.7.0](docs/releases/v0.7.0.md)

**Request pipelines** — `SubmitPipeline` runs ordered stages with shared state, stage-level failure attribution, and the same admission/retry/deadline semantics as `SubmitRequest`. Pipelines are in-process orchestration, not persistent workflows.

**Stage execution context** — each stage reads `StageExecutionFromContext` for shard, lane, stage name, attempt, and deadline snapshot without duplicating routing fields in your state struct.

**Continuations** — opt-in yield/resume releases the worker during slow I/O; `ContinuationCompleter.Complete` resumes on the same key shard. Shard identity end-to-end: yes. Worker blocked end-to-end: no.

**Backend coordination** — `WithBackend` bounds in-process downstream usage per configured resource and backend lane (`db_read`, `external_api`, …).

**Pool pressure adapters** — optional `SQLDBPressureAdapter` / `APIClientPressureAdapter` observe external pool saturation; keylane does not auto-reject from pool telemetry unless your app gates on snapshots.

### Minimal pipeline example

```go
future, _ := keylane.SubmitPipeline(ctx, q, keylane.Pipeline[state, output]{
    Meta: keylane.RequestMeta{Key: "customer-42", Lane: "read", Operation: "get-customer"},
    Stages: []keylane.PipelineStage[state]{
        {Meta: keylane.StageMeta{Name: keylane.StageValidate}, Run: validate},
        {Meta: keylane.StageMeta{Name: keylane.StageDBRead}, Run: fetchRow},
    },
    Complete: buildResponse,
})
out, err := future.Await(ctx)
```

Key docs: [request-pipeline](docs/request-pipeline.md) · [stage-execution-context](docs/stage-execution-context.md) · [continuations](docs/continuations.md) · [backend-resource-coordination](docs/backend-resource-coordination.md) · [backend-pressure-adapters](docs/backend-pressure-adapters.md) · [pipeline-observability](docs/pipeline-observability.md)

```bash
go run ./examples/pipeline_basics
go run ./examples/pipeline_continuation
go run ./examples/backend_coordination
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
- [Request Pipeline](docs/request-pipeline.md) — `SubmitPipeline`, ordered stages, stage observability
- [Stage Execution Context](docs/stage-execution-context.md) — `StageExecutionFromContext`, attempt, deadline snapshot (v0.7)
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

### v0.8.0 API stability and configuration

- [Production Hardening](docs/production-hardening.md)
- [v0.8.0 Release Notes](docs/releases/v0.8.0.md)
- [Examples Guide](docs/examples.md)
- [Production-Minimal Walkthrough](docs/production-minimal.md)
- [API Compatibility](docs/api-compatibility.md)
- [API Stability](docs/api-stability.md)
- [Public API Inventory](docs/public-api-inventory.md)
- [Config Validation](docs/config-validation.md)
- [Config Versioning](docs/config-versioning.md)
- [Production Defaults](docs/production-defaults.md)
- [Compatibility Rules](docs/compatibility-rules.md)
- [Observability Contract](docs/observability-contract.md)
- [Runtime Lifecycle Hardening](docs/runtime-lifecycle-hardening.md)
- [Performance Regression](docs/performance-regression.md)
- [Migration v0.7 → v0.8](docs/migration/v0.7-to-v0.8.md)

### v0.7.0 pipelines, continuations & backend coordination

- [v0.7.0 Overview](docs/v0.7-advanced-request-pipeline-and-resource-coordination.md)
- [Request Pipeline](docs/request-pipeline.md)
- [Stage Execution Context](docs/stage-execution-context.md)
- [Continuations](docs/continuations.md)
- [Backend Resource Coordination](docs/backend-resource-coordination.md)
- [Backend Pressure Adapters](docs/backend-pressure-adapters.md)
- [Pipeline Observability](docs/pipeline-observability.md)
- [Pipeline Testing](docs/pipeline-testing.md)
- [Pipeline Benchmarks](docs/benchmarks/pipeline.md)
- [v0.7.0 Release Notes](docs/releases/v0.7.0.md)

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

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on the development process
and contribution expectations.

## License

`go-keylane` is distributed under the GNU General Public License v3. See the [LICENSE](LICENSE) file for details.
