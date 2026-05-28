# Examples guide (v0.8)

Runnable examples live under [`examples/`](../examples/). Verify they compile:

```bash
./scripts/verify-examples.sh
# or: make verify-examples
```

## Start here (production-oriented)

| Example | Run | Expected |
|---------|-----|----------|
| [production-minimal](../examples/production-minimal/) | `go run ./examples/production-minimal` | `result=ok` |
| [request-runtime](../examples/request-runtime/) | `go run ./examples/request-runtime` | `accepted=… rejected_queue_full=…` |
| [submit-basic](../examples/submit-basic/) | `go run ./examples/submit-basic` | `rejected=queue_full` or `accepted=ok` |
| [submit-value-await](../examples/submit-value-await/) | `go run ./examples/submit-value-await` | `success_and_failure=ok` |
| [cancel-await](../examples/cancel-await/) | `go run ./examples/cancel-await` | `cancel=ok` |
| [timeout-await](../examples/timeout-await/) | `go run ./examples/timeout-await` | `timeout=ok` |
| [shutdown-submit](../examples/shutdown-submit/) | `go run ./examples/shutdown-submit` | `stopped=ok` |

Walkthrough: [production-minimal.md](production-minimal.md).

For HTTP services, continue with [http-middleware.md](http-middleware.md) after [request-runtime](../examples/request-runtime/).

## Opt-in subsystems (experimental)

| Example | Notes |
|---------|--------|
| [safe-retry](../examples/safe-retry/) | Retry + idempotent safe path |
| [unsafe-mutation-no-retry](../examples/unsafe-mutation-no-retry/) | Writes without retry |
| [pipeline-with-backend-resources](../examples/pipeline-with-backend-resources/) | Pipeline + `WithBackend` |
| [backend-resource-coordination](../examples/backend-resource-coordination/) | Standalone lease API |
| [pipeline_basics](../examples/pipeline_basics/) | Sync pipeline only |
| [pipeline_continuation](../examples/pipeline_continuation/) | Continuation opt-in |

## Observability adapters (optional modules)

| Example | Module |
|---------|--------|
| [observability-contract](../examples/observability-contract/) | Core hooks + contract inventory |
| [prometheus](../examples/prometheus/) | `metrics/prometheus` (separate `go.mod`) |
| [otel_hooks](../examples/otel_hooks/) | `tracing/otel` (separate `go.mod`) |

See [observability-contract.md](observability-contract.md).

## Legacy examples

Older examples use hand-rolled `Config` instead of `ProductionDefaults()`. They remain valid but are not the recommended v0.8 entry path:

- [fire_and_forget](../examples/fire_and_forget/)
- [submit_value](../examples/submit_value/)
- [business_service](../examples/business_service/)

Full catalog: [examples/README.md](../examples/README.md).

## Related

- [api-compatibility.md](api-compatibility.md)
- [migration/v0.7-to-v0.8.md](migration/v0.7-to-v0.8.md)
- [production-hardening.md](production-hardening.md)
