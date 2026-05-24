# Observability (v0.2)

The core `keylane` package provides dependency-free observability: depth, counters, queue-wait and run-duration timing, debug snapshots, pressure signals, and optional hooks. Prometheus and OpenTelemetry are **optional adapters** in separate modules.

> **Trade-off:** More instrumentation improves diagnosis. [Low-allocation mode](production-tuning.md) reduces hot-path overhead when you do not need per-job timing or hooks.

---

## How the pieces fit together

```text
StatsGCPressure()     -> cumulative operational snapshot (counters, depths, timing)
Stats()               -> legacy v1 snapshot (depth + legacy throughput fields)
DebugSnapshot()       -> point-in-time depths, hot shard/lane rankings
Pressure()            -> cheap depth ratio for admission / early reject
Hooks                 -> per-job timing and slow-job callbacks
Optional adapters     -> Prometheus pull, OpenTelemetry spans
```

| Layer | Public API | Purpose |
|-------|------------|---------|
| Stats snapshot | `Queue.StatsGCPressure()` | Cumulative counters, depths, queue-wait and run timing since start |
| Legacy stats | `Queue.Stats()` | v1 deep-copy snapshot; use for depth layout and legacy lane totals |
| Counters | `LaneCountersGCPressure` in snapshot | `Submitted`, `Accepted`, `Rejected`, `Completed`, `Failed`, `QueueFull`, … |
| Queue wait | `QueueWaitStatsGCPressure` in snapshot | Time from admission until `Run` starts |
| Run duration | `RunStatsGCPressure` in snapshot | Time inside `Run` only |
| Debug | `Queue.DebugSnapshot()` | Hot shard/lane, per-shard/lane depth at call time |
| Pressure | `Queue.Pressure()` | `IsHealthy`, `IsPressured`, `IsOverloaded`, depth ratios |
| Hooks | `Hooks.OnJobTiming`, `OnSlowJob`, `OnQuotaChange`, `OnAdaptiveQuotaDecision`, `OnOverloadPolicyDecision` | Custom or adapter integration |
| Adaptive debug | `Queue.AdaptiveDebugSnapshot()` | Controller state + per-lane adaptive counters |
| Adapters | separate modules | [Prometheus](metrics-prometheus.md), [OpenTelemetry](tracing-opentelemetry.md) |

---

## StatsGCPressure (primary pull API)

```go
snap := q.StatsGCPressure()
fmt.Printf("queued=%d in_flight=%d\n", snap.TotalQueued, snap.TotalInFlight)
for _, lane := range snap.Lanes {
    c := lane.Counters
    fmt.Printf("lane=%s submitted=%d completed=%d queue_full=%d\n",
        lane.Name, c.Submitted, c.Completed, c.QueueFull)
    fmt.Printf("  queue_wait_avg=%v run_avg=%v\n",
        lane.QueueWait.AverageDuration(), lane.Run.AverageDuration())
}
```

- **Counters** are cumulative and best-effort under concurrency (not an audit log).
- **Queue wait** and **run duration** require `EnableQueueWaitTiming` / `EnableRunTiming` (on in default visibility mode; off in low-allocation mode).
- Call on a **timer** or admin path — not on every `Submit`.

---

## Queue wait vs run duration

Always inspect these separately when p99 latency is high:

| Pattern | Likely cause |
|---------|----------------|
| High queue wait, low run duration | Scheduler backlog, hot lane/shard, low workers or quotas |
| Low queue wait, high run duration | Slow `Run`, DB/HTTP, or blocking in user code |
| Both high | Overload plus slow work once admitted |

See [debugging.md](debugging.md) for a full symptom table.

---

## DebugSnapshot and Pressure

**`Pressure()`** — cheap check for admission control (queue depth vs capacity only; not CPU or GC):

```go
p := q.Pressure()
if p.IsOverloaded {
    return errSchedulerBusy // reject or degrade before Submit
}
```

**`DebugSnapshot()`** — deeper view including `HotShards` and `HotLanes` ranked by depth:

```go
snap := q.DebugSnapshot()
for _, hs := range snap.HotShards {
    fmt.Printf("hot shard %d depth=%d ratio=%.2f\n", hs.ShardID, hs.Depth, hs.DepthRatio)
}
```

`DebugSnapshot` is a near-time diagnostic view under concurrent workers, not a global stop-the-world snapshot.

---

## Hooks

Enabled when `Observability.EnableHooks` is true (off in low-allocation mode).

- **`OnJobTiming`** — after each job: shard, lane, queue wait, run duration, outcome.
- **`OnSlowJob`** — when run duration ≥ `SlowJobThreshold`.
- **`Hooks.Request`** (`OnQueued`, `OnStarted`, `OnCompleted`, `OnRejected`) — `SubmitRequest` lifecycle with transport-agnostic `RequestObservation` (queue wait, run, outcome). Complements job-level timing; both may fire for the same work.

HTTP middleware (`httpkeylane`) sets `RequestMeta.Transport` / `Operation` and optional `Config.Observe` for status codes; configure request hooks on `keylane.Config.Observability.Hooks.Request`.

Hooks run outside scheduler locks. Nil hooks are safe. Hook panics are recovered.

For OpenTelemetry, use the [tracing adapter](tracing-opentelemetry.md) instead of hand-rolling exporters on the hot path unless you need custom behavior.

---

## Visibility vs low-allocation mode

| Mode | Config | Timing in StatsGCPressure | Hooks |
|------|--------|---------------------------|-------|
| Visibility (default) | `DefaultObservabilityConfig()` or omit | On | On |
| Low-allocation | `LowAllocationObservabilityConfig()` | Off | Off |

`Pressure()`, counters, and on-demand `DebugSnapshot()` remain useful in low-allocation production paths. Details: [production-tuning.md](production-tuning.md).

---

## Optional adapters

The core module does **not** import Prometheus or OpenTelemetry.

| Adapter | Module | Integration |
|---------|--------|-------------|
| Prometheus | `github.com/haluan/go-keylane/metrics/prometheus` | `NewCollector(q, opts)` — scrape `StatsGCPressure()` + `Pressure()` |
| OpenTelemetry | `github.com/haluan/go-keylane/tracing/otel` | `NewHooks(opts)` on `Observability.Hooks` |

Do not label metrics or spans with job `Key`, request IDs, or other high-cardinality values. Use static lane names and `shard_id` only.

---

## High-cardinality warning

**Lanes** must be a small static set (`payment`, `audit`, `webhook`). Do not use tenant IDs or request IDs as lane names — internal structures are allocated per registered lane.

Use **`Job.Key`** for per-tenant routing into shards; keep lanes as workload classes.

---

## v0.4 quota, overload, and adaptive hooks

When `Observability.EnableHooks` is true:

| Hook | When it fires |
|------|----------------|
| `OnQuotaChange` | After every successful quota publish (`source=manual` or `source=adaptive`) |
| `OnAdaptiveQuotaDecision` | Adaptive evaluation outcome (change, apply failure, optional hold tracing) |
| `OnOverloadPolicyDecision` | Overload reject, shed, or degrade (not on keep) |

Use `Queue.AdaptiveDebugSnapshot()` for controller state and per-lane adaptive stats. `AdaptiveQuotaSnapshot()` is deprecated.

Full event fields, reason codes, example flows, and troubleshooting: **[adaptive-observability.md](adaptive-observability.md)**.

Also see [adaptive-quota.md](adaptive-quota.md), [overload-policy.md](overload-policy.md), and [benchmarks/adaptive-quota.md](benchmarks/adaptive-quota.md).

---

## v0.5 diagnostics (hot key, shard pressure, autoscaling)

When KL-1501–1504 features are enabled, `DebugSnapshot()` (version `"4"`) adds bounded v0.5 fields:

| Field | API | Purpose |
|-------|-----|---------|
| `PressureSummary` | `Queue.PressureSummary()` | Global/shard pressure class, hot shards, lane dominance |
| `ScaleSignal` | `Queue.ScaleSignal()` | Autoscaling recommendation with reason and scope |
| `PerKeyAdmissionSnapshots` | in `DebugSnapshot()` | Per-key mitigation state (hash-only by default) |
| `HotKeyCandidates` | per shard in `DebugSnapshot().Shards[]` | Hot key candidates with `KeyHash`, ratios, status |
| `HotKeys` | `DebugSnapshot().HotKeys` | Spec-aligned flat list (`HotKeyCandidateSnapshot`, stable-sorted) |
| `Mitigations` | `DebugSnapshot().Mitigations` | Spec-aligned `PerKeyMitigationSnapshot` list |
| `ShardPressure` | `DebugSnapshot().ShardPressure` | Per-shard pressure snapshots (flat slice) |

**Spec name mapping (KL-1505)**

| Spec | Implementation |
|------|----------------|
| `Observer` | `Hooks` struct with optional function fields |
| `OnShardPressure` | `OnShardPressureSummary` |
| `ShardPressureEvent` | `ShardPressureSummaryEvent` |
| `HotKeyCandidateSnapshot` | `HotKeyCandidateSnapshot` (includes `LastSeenUnixNano`) |
| `PerKeyMitigationSnapshot` | `PerKeyMitigationSnapshot` (see also `PerKeyAdmissionSnapshots`) |

**When to use which pull API**

- **`ScaleSignal()`** — autoscaler input: `Recommended`, `Reason`, `Scope`, `PressureRatio`, worker and queue ratios. Cheap enough for periodic polling.
- **`PressureSummary()`** — mitigation vs scale classification (`Class`, `ScaleRelevant`, `MitigationRelevant`, per-shard `Class`).
- **`DebugSnapshot()`** — full point-in-time view including v0.5 sections above plus hot shard/lane rankings.

**Privacy:** `HotKeyConfig.ExposeRawKey` defaults to `false`. Snapshots, hooks, and Prometheus metrics emit **`KeyHash` only** — never raw tenant keys unless you explicitly opt in.

**Optional v0.5 hooks** (require `Observability.EnableDebugSnapshot` and `EnableHooks`; set `Observability = DefaultObservabilityConfig()` before assigning callbacks):

| Hook | When it fires |
|------|----------------|
| `OnHotKeyCandidate` | After `DebugSnapshot()` observes a hot key candidate |
| `OnShardPressureSummary` | After `PressureSummary()` completes |
| `OnScaleSignal` | After `ScaleSignal()` with `DiagnosticsEnabled=true` |
| `OnPerKeyAdmissionDecision` | Per-key throttle/reject/shed at admission (existing KL-1502) |

Hooks fire outside scheduler locks with panic recovery. Nil callbacks are no-ops.

**Why CPU/memory can stay flat during backpressure:** keylane shapes concurrency and queue depth; high queue wait with low CPU often means worker saturation or localized hot keys, not necessarily high host CPU. Use `WorkerBusyRatio`, `PressureSummary.Class`, and queue-wait metrics together — see [pressure-diagnostics.md](pressure-diagnostics.md) and [hot-key-mitigation.md](hot-key-mitigation.md).

**v0.5 tests and benchmarks:**

```bash
go test ./... -run 'HotKey|PerKey|ShardPressure|ScaleSignal|Scenario|Leak|Race|V05'
go test ./... -bench 'HotKey|PerKey|ShardPressure|ScaleSignal|Snapshot|V05|Baseline' -benchmem .
cd metrics/prometheus && go test ./...
```

See [v0.5-runtime-signals.md](v0.5-runtime-signals.md), [shard-pressure-diagnostics.md](shard-pressure-diagnostics.md), [autoscaling-signals.md](autoscaling-signals.md), and [benchmarks.md](benchmarks.md).

---

## Related documentation

- [debugging.md](debugging.md) — production troubleshooting
- [production-tuning.md](production-tuning.md) — capacity and observability modes
- [gc-pressure-shaping.md](gc-pressure-shaping.md) — what keylane does and does not control
- [phase-6-observability.md](phase-6-observability.md) — detailed phase implementation notes
