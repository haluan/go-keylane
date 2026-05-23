# Production tuning

Guidance for shard keys, lanes, workers, queue capacity, pressure-based admission, observability overhead, and optional metrics/tracing adapters.

For lifecycle errors (`ErrQueueFull`, shutdown, Await deadlocks), see [production-guidance.md](production-guidance.md). For signals and troubleshooting, see [debugging.md](debugging.md) and [observability.md](observability.md).

## Principle

`keylane` helps shape GC pressure caused by uncontrolled concurrency. It does **not** avoid Go GC pauses. See [gc-pressure-shaping.md](gc-pressure-shaping.md).

Low-allocation observability reduces optional hot-path instrumentation; it does not eliminate runtime GC.

---

## Shard keys

The **Key** routes work to a deterministic shard for isolation.

- Use stable logical identities: `tenant_id`, `customer_id`, `merchant_uuid`.
- Avoid high-churn keys (random request IDs, timestamps) — they spread load across all shards and defeat noisy-neighbor isolation.
- A single hot key concentrates load on one shard; shard round-robin still prevents that shard from starving *other* shards, but queues inside the hot shard can saturate.

---

## Lanes

Lanes separate **workload classes**, not individual requests.

- Examples: `payment`, `audit`, `webhook`, `sensitive`.
- Keep a small static set (roughly ≤ 10 lanes). Each lane allocates queue storage per shard.
- Do **not** use tenant IDs or request IDs as lane names — memory and snapshot cost grow with every registered lane.
- Use **Job.Key** for per-tenant routing; use **Lane** for priority/SLO class.

---

## Lane quotas

`LaneQuotas` limits how many jobs per lane a worker processes in one pass over a shard.

- Higher quota on a lane gives it more share of that shard's worker time.
- Low quota on a latency-sensitive lane can starve it when a noisy lane shares the shard.
- Tune together with `WorkerCount` and observed `HotLanes` from `DebugSnapshot()`.

### Runtime quota updates

Lane names are fixed when the queue is created. Per-lane drain quotas can be changed at runtime with `UpdateQuotaPolicy` (see `QuotaPolicy` in the root package).

- Updates publish an immutable policy snapshot; workers load the snapshot once at the start of each shard drain cycle.
- A drain cycle in progress keeps the policy it loaded; the next cycle uses the newest snapshot.
- Queued jobs are not dropped; running jobs are not cancelled.
- Call `CurrentQuotaPolicy()` to inspect the active version and quotas.
- Updates are rejected while the queue is stopping or stopped (`ErrStopped`). Updates are allowed before `Start` and while running.

---

## Worker count vs GOMAXPROCS

- **CPU-bound jobs:** start with `WorkerCount <= GOMAXPROCS` to limit context switching.
- **Blocking I/O in `Run`:** you may increase workers above core count because workers spend time waiting on external calls.
- If workers are saturated but queue wait stays high, more workers or higher lane quotas may help; if run duration dominates, fix `Run` or downstream latency first.

---

## Shard count

Shards are lock isolation buckets. Each shard pre-allocates per-lane queues.

- More shards → less contention, higher static memory footprint.
- Rule of thumb: shard count at least **4–8×** `WorkerCount` for many workloads.

---

## Queue capacity

`QueueSizePerLane` bounds memory and defines when `Submit` returns `ErrQueueFull`.

- **Larger queues** absorb bursts but can **hide overload** by turning rejection into queue wait latency.
- Prefer bounded capacity plus **early reject** when depth is high.
- Use `Pressure()` before admitting work:

```go
p := q.Pressure()
if p.IsPressured || p.IsOverloaded {
    return errTooBusy // shed load before Submit
}
```

`Pressure()` reflects queue depth vs total capacity (thresholds at 70% / 90% depth ratio). It is not CPU, GC, or end-to-end latency SLOs.

---

## Observability modes

### Visibility mode (default)

Use `keylane.DefaultObservabilityConfig()` or omit `Config.Observability` (resolved to defaults at `New`):

- `EnableStats`, `EnableCounters`, `EnableDebugSnapshot`: on
- `EnableQueueWaitTiming`, `EnableRunTiming`, `EnableHooks`: on
- Best for staging, incident response, and tuning when you need queue-wait/run-duration signals and hooks

### Low-allocation mode

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

### Granular flags

For advanced setups, set `EnableStats`, `EnableCounters`, `EnableQueueWaitTiming`, `EnableRunTiming`, `EnableHooks`, and `EnableDebugSnapshot` individually. Legacy fields remain:

- `TrackQueueWait` — v1 `Stats()` queue-wait only (independent of `EnableQueueWaitTiming`)
- `SlowJobThreshold` + `Hooks` — honored only when `EnableHooks` is true

### When to use which mode

| Situation | Recommendation |
|-----------|------------------|
| Production hot path, latency-sensitive | Low-allocation |
| Debugging tail latency, slow jobs, queue wait | Visibility (default) |
| Periodic ops dashboards | Either; call `StatsGCPressure()` on an interval (not every submit) |
| Deep incident drill-down | Visibility + occasional `DebugSnapshot()` |

### Benchmarking both modes

```bash
go test -bench='BenchmarkKeylaneSubmit.*Observability|BenchmarkKeylaneSubmitValue.*Observability' -benchmem ./benchmarks
go test -bench='BenchmarkKeylaneWorker.*Observability' -benchmem ./internal/core
go test -bench=BenchmarkKeylaneDebugSnapshotOnDemand -benchmem ./benchmarks
```

Compare with `benchstat` (`-count=5` recommended). Root `BenchmarkGCPressureLowAllocationMode` measures **sync.Pool** batch recycling (`DisablePooling`), not observability mode.

---

## Optional adapters

Prometheus and OpenTelemetry live in **separate modules** — the core package never imports them.

| Adapter | Module | Integration |
|---------|--------|-------------|
| Prometheus | `github.com/haluan/go-keylane/metrics/prometheus` | Pull collector on `StatsGCPressure()` + `Pressure()` |
| OpenTelemetry | `github.com/haluan/go-keylane/tracing/otel` | `NewHooks()` wired into `Observability.Hooks` |

- See [metrics-prometheus.md](metrics-prometheus.md) and [tracing-opentelemetry.md](tracing-opentelemetry.md).
- In low-allocation mode: Prometheus scrape stays off the hot path; disable `EnableHooks` to avoid OTEL span creation per job.
- Do not add job `Key` or request IDs as metric/trace labels.

---

## Pull API cost

- `StatsGCPressure()` and `DebugSnapshot()` may allocate when called; that is acceptable on demand.
- Do not call them on every submit; sample on a timer or when handling admin/debug requests.
- `Pressure()` is intended for cheap admission checks and remains available regardless of debug snapshot settings.

---

## Related documentation

- [benchmarks.md](benchmarks.md) — evaluate allocation and fairness
- [gc-pressure-shaping.md](gc-pressure-shaping.md) — positioning and limits
- [observability.md](observability.md) — API overview
