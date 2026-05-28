# Go-Keylane Examples

Runnable examples for `go-keylane`. Full guide: [docs/examples.md](../docs/examples.md).

Verify all examples compile:

```bash
./scripts/verify-examples.sh
# or: make verify-examples
```

---

## v0.8 production path (start here)

| Example | Run | Expected |
|---------|-----|----------|
| [production-minimal](production-minimal/) | `go run ./examples/production-minimal` | `result=ok` |
| [request-runtime](request-runtime/) | `go run ./examples/request-runtime` | `accepted=… rejected_queue_full=…` |
| [submit-basic](submit-basic/) | `go run ./examples/submit-basic` | `rejected=queue_full` or `accepted=ok` |
| [submit-value-await](submit-value-await/) | `go run ./examples/submit-value-await` | `success_and_failure=ok` |
| [cancel-await](cancel-await/) | `go run ./examples/cancel-await` | `cancel=ok` |
| [timeout-await](timeout-await/) | `go run ./examples/timeout-await` | `timeout=ok` |
| [shutdown-submit](shutdown-submit/) | `go run ./examples/shutdown-submit` | `stopped=ok` |

Walkthrough: [docs/production-minimal.md](../docs/production-minimal.md).

For HTTP services, see [docs/http-middleware.md](../docs/http-middleware.md) after [request-runtime](request-runtime/).

---

## v0.8 opt-in / experimental

> **Experimental:** enable only after validating semantics for your workload.

| Example | Run | Notes |
|---------|-----|-------|
| [safe-retry](safe-retry/) | `go run ./examples/safe-retry` | Retry + idempotent safe path |
| [unsafe-mutation-no-retry](unsafe-mutation-no-retry/) | `go run ./examples/unsafe-mutation-no-retry` | Writes without retry |
| [pipeline-with-backend-resources](pipeline-with-backend-resources/) | `go run ./examples/pipeline-with-backend-resources` | Pipeline + `WithBackend` |
| [backend-resource-coordination](backend-resource-coordination/) | `go run ./examples/backend-resource-coordination` | Standalone lease API |
| [observability-contract](observability-contract/) | `go run ./examples/observability-contract` | Hooks + low-cardinality labels |

Deeper dives (v0.7 style, still valid):

| Example | Run |
|---------|-----|
| [pipeline_basics](pipeline_basics/) | `go run ./examples/pipeline_basics` |
| [pipeline_continuation](pipeline_continuation/) | `go run ./examples/pipeline_continuation` |
| [backend_coordination](backend_coordination/) | `go run ./examples/backend_coordination` |

---

## Observability adapters (optional modules)

| Example | Run |
|---------|-----|
| [prometheus](prometheus/) | `cd examples/prometheus && go run .` |
| [otel_hooks](otel_hooks/) | `cd examples/otel_hooks && go run .` |

---

## Legacy examples (pre–v0.8 config style)

These use hand-rolled `Config` instead of `ProductionDefaults()`. Prefer [production-minimal](production-minimal/) for new integrations.

| Example | Run |
|---------|-----|
| [fire_and_forget](fire_and_forget/) | `go run ./examples/fire_and_forget` |
| [submit_value](submit_value/) | `go run ./examples/submit_value` |
| [business_service](business_service/) | `go run ./examples/business_service` |

### v0.5–v0.6

```bash
go run ./examples/v0.5-hot-key-autoscaling
go run ./examples/retry_policy
go run ./examples/deadline_budget
go run ./examples/idempotency_retry
go run ./examples/retry_suppression
go run ./examples/failure_observability
```

### v0.7 pipelines & backend pressure

```bash
go run ./examples/pipeline_basics
go run ./examples/pipeline_continuation
go run ./examples/backend_coordination
go run ./examples/backend_pressure_sql
go run ./examples/backend_pressure_api
```

---

## v0.7 example details

### pipeline_basics

| | |
|--|--|
| **Demonstrates** | `SubmitPipeline` with two sync `Run` stages and `Complete` |
| **Run** | `go run ./examples/pipeline_basics` |
| **Expected output** | `result=20` |
| **Caveat** | In-process only; not a workflow engine. See [request-pipeline.md](../docs/request-pipeline.md). |

### pipeline_continuation

| | |
|--|--|
| **Demonstrates** | `RunContinuation`, `NewContinuation`, async `Complete`, resume |
| **Run** | `go run ./examples/pipeline_continuation` |
| **Expected output** | `sum=7 pending=0` |
| **Caveat** | Requires `Continuation.Enabled`. Do not `Await` the same queue from inside a stage. See [continuations.md](../docs/continuations.md). |

### backend_coordination

| | |
|--|--|
| **Demonstrates** | `BackendResources` + `WithBackend` in a pipeline |
| **Run** | `go run ./examples/backend_coordination` |
| **Expected output** | `ok=true backend_lanes=1` |
| **Caveat** | In-process admission only. See [backend-resource-coordination.md](../docs/backend-resource-coordination.md). |

### backend_pressure_sql / backend_pressure_api

Observational pool pressure adapters (stub DB / no network). See [backend-pressure-adapters.md](../docs/backend-pressure-adapters.md).
