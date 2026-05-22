# OpenTelemetry adapter for Keylane

Optional tracing hooks for [`github.com/haluan/go-keylane`](../../). The core module has **no** OpenTelemetry dependency.

## Install

```bash
go get github.com/haluan/go-keylane/tracing/otel
```

## Usage

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

Set `Disabled: true` or omit `Tracer` to return empty hooks (no overhead).

## Span attributes

| Attribute | When |
|-----------|------|
| `keylane.shard_id` | Always on job spans |
| `keylane.lane` | Always |
| `keylane.queue_wait_ms` | `RecordQueueWait` |
| `keylane.run_ms` | `RecordRunTime` |
| `keylane.pressure_ratio` | `RecordPressure` + `Queue` set |
| `keylane.outcome` | Always |

Spans: `keylane.job` (every completed job), `keylane.slow_job` (slow-job hook).

Worker spans use `context.Background()`. For request-scoped traces, wrap user `Run()` with your application context separately.

## Low-allocation mode

Use `LowAllocationObservabilityConfig()` or `EnableHooks: false` so hooks are not invoked.
