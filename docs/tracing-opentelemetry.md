# OpenTelemetry tracing adapter

The core [`github.com/haluan/go-keylane`](../) module does **not** import OpenTelemetry. Optional hooks live in:

**Module:** `github.com/haluan/go-keylane/tracing/otel`

## Quick start

```go
import (
    keylaneotel "github.com/haluan/go-keylane/tracing/otel"
    "go.opentelemetry.io/otel"
)

tracer := otel.Tracer("my-service")
cfg.Observability = keylane.ObservabilityConfig{
    EnableHooks: true,
    Hooks: keylaneotel.NewHooks(keylaneotel.Options{
        Tracer:          tracer,
        RecordQueueWait: true,
        RecordRunTime:   true,
    }),
}
```

See [examples/otel_hooks](../examples/otel_hooks/) for a minimal runnable sample.

## Disable completely

```go
Hooks: keylaneotel.NewHooks(keylaneotel.Options{Disabled: true})
// or EnableHooks: false in ObservabilityConfig
```

## Span attributes

| Attribute | Description |
|-----------|-------------|
| `keylane.shard_id` | Shard index |
| `keylane.lane` | Lane name |
| `keylane.queue_wait_ms` | Queue wait before `Run` (optional) |
| `keylane.run_ms` | User `Run` duration (optional) |
| `keylane.pressure_ratio` | Depth ratio when `RecordPressure` and `Queue` are set |
| `keylane.queue_depth` | Total queued depth (with pressure) |
| `keylane.inflight_jobs` | In-flight count (with pressure) |
| `keylane.outcome` | `completed`, `failed`, `canceled` |

Spans: `keylane.job` (per completed job via `OnJobTiming`), `keylane.slow_job` (via `OnSlowJob`).

## Context limitations

The adapter records **worker-scoped** spans using `context.Background()`. For end-to-end request tracing, propagate your application context inside the job `Run` function; do not rely on this adapter alone for caller correlation.

## Low-allocation mode

Use `keylane.LowAllocationObservabilityConfig()` or set `EnableHooks: false` so hook callbacks (and OTEL span creation) are not invoked on the worker path.

## Testing adapters

```bash
make test-adapters
# or:
cd tracing/otel && go test ./...
```
