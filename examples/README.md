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
```
