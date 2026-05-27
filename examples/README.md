# Go-Keylane Examples

This directory contains runnable, fully-contained examples demonstrating the core usage patterns of the `go-keylane` library.

---

## 1. Directory Structure

- **[fire_and_forget](fire_and_forget/)**: A basic guide showing how to initialize a queue, submit several light jobs asynchronously, start workers, and perform a graceful teardown.
- **[submit_value](submit_value/)**: Demonstrates the `SubmitValue` and `Await` models to route jobs and block until execution results or computation values are returned to the caller.
- **[business_service](business_service/)**: A realistic simulated enterprise billing payment service incorporating high-priority billing lanes, background webhooks, audit log aggregation, context cancellation check handling, and graceful drain procedures under load.
- **[prometheus](prometheus/)**: Registers the optional Prometheus collector and prints one text scrape.
- **[otel_hooks](otel_hooks/)**: Wires the optional OpenTelemetry hook adapter and records spans to an in-memory exporter.
- **[v0.5-hot-key-autoscaling](v0.5-hot-key-autoscaling/)**: Enables v0.5 hot key detection (observe mode), prints `DebugSnapshot` hot keys and `ScaleSignal`.
- **[retry_policy](retry_policy/)**: Bounded retry with `RetryableFailure` and safe idempotency.
- **[deadline_budget](deadline_budget/)**: `context.WithTimeout` submit and `BudgetFromFuture`.
- **[idempotency_retry](idempotency_retry/)**: Safe vs unsafe idempotency under retry.
- **[retry_suppression](retry_suppression/)**: Retry suppressed when the queue is overloaded.
- **[failure_observability](failure_observability/)**: `RetryFailureSnapshot` and `RetryTraceFromFuture`.

### v0.7.0 — pipelines, continuations, backend coordination

- **[pipeline_basics](pipeline_basics/)**: Two-stage synchronous `SubmitPipeline` and `Future.Await`.
- **[pipeline_continuation](pipeline_continuation/)**: Non-blocking yield, async `Complete`, and resume on the same key shard.
- **[backend_coordination](backend_coordination/)**: `BackendResources` config and `WithBackend` inside a pipeline stage.
- **[backend_pressure_sql](backend_pressure_sql/)**: `SQLDBPressureAdapter` with a stub `database/sql` stats reader (no real DB).
- **[backend_pressure_api](backend_pressure_api/)**: `APIClientPressureAdapter` with a custom bounded-client reader (no network).

---

## 2. Running Examples

You can run any example directly from the repository root:

```bash
# Run Fire and Forget Example
go run ./examples/fire_and_forget

# Run Submit Value Example
go run ./examples/submit_value

# Run Business Service Example
go run ./examples/business_service

# Run Prometheus adapter example
go run ./examples/prometheus

# Run OpenTelemetry hooks example
go run ./examples/otel_hooks

# Run v0.5 hot key & autoscaling example
go run ./examples/v0.5-hot-key-autoscaling

# v0.6.0 retry, deadline, idempotency, suppression, observability
go run ./examples/retry_policy
go run ./examples/deadline_budget
go run ./examples/idempotency_retry
go run ./examples/retry_suppression
go run ./examples/failure_observability

# v0.7.0 pipelines, continuations, backend coordination
go run ./examples/pipeline_basics
go run ./examples/pipeline_continuation
go run ./examples/backend_coordination
go run ./examples/backend_pressure_sql
go run ./examples/backend_pressure_api
```

---

## 3. v0.7.0 example details

### pipeline_basics

| | |
|--|--|
| **Demonstrates** | `SubmitPipeline` with two sync `Run` stages and `Complete` |
| **Run** | `go run ./examples/pipeline_basics` |
| **Expected output** | `result=20` (validate sets 10, business doubles) |
| **Caveat** | In-process only; not a workflow engine. See [request-pipeline.md](../docs/request-pipeline.md). |

### pipeline_continuation

| | |
|--|--|
| **Demonstrates** | `RunContinuation`, `NewContinuation`, async `Complete`, resume |
| **Run** | `go run ./examples/pipeline_continuation` |
| **Expected output** | `sum=7 pending=0` after async completer runs |
| **Caveat** | Requires `Continuation.Enabled`. Do not `Await` the same queue from inside a stage. See [continuations.md](../docs/continuations.md). |

### backend_coordination

| | |
|--|--|
| **Demonstrates** | `BackendResources` + `WithBackend` / `BackendOperationFromStage` in a pipeline |
| **Run** | `go run ./examples/backend_coordination` |
| **Expected output** | `ok=true backend_lanes=1` (resource snapshot when debug snapshot enabled) |
| **Caveat** | In-process admission only; configure `MaxInFlight` per deployment. See [backend-resource-coordination.md](../docs/backend-resource-coordination.md). |

### backend_pressure_sql

| | |
|--|--|
| **Demonstrates** | `SQLDBPressureAdapter` mapping pool stats into `BackendPressure` |
| **Run** | `go run ./examples/backend_pressure_sql` |
| **Expected output** | One line with `resource=primary-db`, `pressure=…`, `saturated=…` |
| **Caveat** | Uses a stub DB; observational only — does not reject requests unless you gate on pressure. See [backend-pressure-adapters.md](../docs/backend-pressure-adapters.md). |

### backend_pressure_api

| | |
|--|--|
| **Demonstrates** | `APIClientPressureAdapter` with a custom `ResourcePressureReader` |
| **Run** | `go run ./examples/backend_pressure_api` |
| **Expected output** | Pressure snapshot lines for configured API resource/lane |
| **Caveat** | No `net/http.Transport` universal adapter; wire your bounded client. Observational only. |
