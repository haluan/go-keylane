# Production tuning — observability modes

KL-1207 adds explicit control over scheduler observability overhead. Use this guide with the [benchmark suite](../benchmarks/README.md) to choose a mode for your workload.

## Principle

Low-allocation observability mode reduces optional scheduler instrumentation in the hot path. It does **not** eliminate Go GC pauses. It helps Keylane avoid adding unnecessary allocation pressure while shaping concurrency caused by uncontrolled concurrency, goroutine explosion, allocation bursts, and request pile-up.

Keylane does **not** avoid Go GC pauses.

## Visibility mode (default)

Use `keylane.DefaultObservabilityConfig()` or omit `Config.Observability` (resolved to defaults at `New`):

- `EnableStats`, `EnableCounters`, `EnableDebugSnapshot`: on
- `EnableQueueWaitTiming`, `EnableRunTiming`, `EnableHooks`: on
- Best for staging, incident response, and tuning when you need queue-wait/run-duration signals and hooks

## Low-allocation mode

Use `LowAllocationMode: true` or `keylane.LowAllocationObservabilityConfig()`:

| Feature | Low-allocation |
|---------|----------------|
| `StatsGCPressure()` | Available (pull API; may allocate on call) |
| Per-lane counters (`Submitted`, `Completed`, …) | On |
| `DebugSnapshot()` | On (on-demand) |
| `Pressure()` | Always available (cheap depth ratio) |
| Queue-wait timing in `StatsGCPressure` | Off |
| Run-duration timing in `StatsGCPressure` | Off |
| `OnJobTiming` / `OnSlowJob` hooks | Off |
| v1 `TrackQueueWait` on `Stats()` | Off unless you set it |

```go
cfg := keylane.Config{
    ShardCount:       16,
    WorkerCount:      4,
    QueueSizePerLane: 10000,
    LaneQuotas:       map[keylane.Lane]int{"default": 2},
    Observability:    keylane.LowAllocationObservabilityConfig(),
}
```

When `LowAllocationMode` is true, the preset wins at `New` even if other `Enable*` fields are set.

## Granular flags

For advanced setups, set `EnableStats`, `EnableCounters`, `EnableQueueWaitTiming`, `EnableRunTiming`, `EnableHooks`, and `EnableDebugSnapshot` individually. Legacy fields remain:

- `TrackQueueWait` — v1 `Stats()` queue-wait only (independent of `EnableQueueWaitTiming`)
- `SlowJobThreshold` + `Hooks` — honored only when `EnableHooks` is true

## When to use which mode

| Situation | Recommendation |
|-----------|------------------|
| Production hot path, latency-sensitive | Low-allocation |
| Debugging tail latency, slow jobs, queue wait | Visibility (default) |
| Periodic ops dashboards | Either; call `StatsGCPressure()` on an interval (not every submit) |
| Deep incident drill-down | Visibility + occasional `DebugSnapshot()` |

## Benchmarking both modes

```bash
go test -bench='BenchmarkKeylaneSubmit.*Observability|BenchmarkKeylaneSubmitValue.*Observability' -benchmem ./benchmarks
go test -bench='BenchmarkKeylaneWorker.*Observability' -benchmem ./internal/core
go test -bench=BenchmarkKeylaneDebugSnapshotOnDemand -benchmem ./benchmarks
```

Compare with `benchstat` (`-count=5` recommended). Root `BenchmarkGCPressureLowAllocationMode` measures **sync.Pool** batch recycling (`DisablePooling`), not observability mode.

## Optional adapters (KL-1208)

Prometheus and OpenTelemetry live in **separate modules** — the core package never imports them.

| Adapter | Module | Integration |
|---------|--------|-------------|
| Prometheus | `github.com/haluan/go-keylane/metrics/prometheus` | Pull collector on `StatsGCPressure()` + `Pressure()` |
| OpenTelemetry | `github.com/haluan/go-keylane/tracing/otel` | `NewHooks()` wired into `Observability.Hooks` |

- See [metrics-prometheus.md](metrics-prometheus.md) and [tracing-opentelemetry.md](tracing-opentelemetry.md).
- In low-allocation mode: Prometheus scrape stays off the hot path; disable `EnableHooks` to avoid OTEL span creation per job.
- Do not add job `Key` or request IDs as metric/trace labels.

## Pull API cost

- `StatsGCPressure()` and `DebugSnapshot()` may allocate when called; that is acceptable on demand.
- Do not call them on every submit; sample on a timer or when handling admin/debug requests.
- `Pressure()` is intended for cheap admission checks and remains available regardless of debug snapshot settings.
