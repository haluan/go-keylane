# Phase 6: Observability

Go-Keylane provides lightweight, dependency-free observability built directly into the scheduler. This design allows users to monitor depth, throughput counters, wait latency, and slow jobs without pulling in heavy external telemetry SDKs like Prometheus or OpenTelemetry.

---

## 1. Queue Stats Snapshot

The `Queue.Stats()` method returns a deep-copy, thread-safe value snapshot of the entire queue's state.

For GC pressure diagnostics (queued depth, in-flight jobs, and cumulative per-lane counters), use `Queue.StatsGCPressure()` instead. Each lane exposes `Counters` (`Submitted`, `Accepted`, `Rejected`, `Completed`, `Failed`, `QueueFull`, `Canceled`, `Panicked`). See `LaneCountersGCPressure` godoc for semantics. v1 `Stats()` retains legacy fields such as `SubmittedTotal` (successful enqueues only).

```go
stats := q.Stats()
fmt.Printf("Active Workers: %d\n", stats.WorkerCount)
fmt.Printf("Total Queued Depth: %d\n", stats.TotalDepth)

for _, shard := range stats.Shards {
    fmt.Printf("Shard %d (Ready: %v, Depth: %d):\n", shard.ShardID, shard.Ready, shard.TotalDepth)
    for _, lane := range shard.Lanes {
        fmt.Printf("  - Lane %s: Depth=%d, Quota=%d, Capacity=%d\n", 
            lane.Lane, lane.Depth, lane.Quota, lane.Capacity)
    }
}
```

---

## 2. Debug Snapshot and Pressure (KL-1205)

For **point-in-time** queue state (not cumulative metrics), use `Queue.DebugSnapshot()` and `Queue.Pressure()`.

| API | Use when |
|-----|----------|
| `Pressure()` | Cheap admission/degradation check (`IsHealthy`, `IsPressured`, `IsOverloaded`) |
| `DebugSnapshot()` | Full diagnostic view: per-shard/lane depth, hot rankings, embedded pressure |
| `StatsGCPressure()` | Cumulative counters and timing since queue start |

`Pressure` uses queue depth vs total capacity (`PressuredDepthRatio` = 0.70, `OverloadedDepthRatio` = 0.90). It is **queue-depth pressure only** — not CPU, GC, or latency SLOs.

```go
p := q.Pressure()
if p.IsOverloaded {
    return errSchedulerBusy
}

snap := q.DebugSnapshot()
for _, hs := range snap.HotShards {
    fmt.Printf("hot shard %d depth=%d ratio=%.2f\n", hs.ShardID, hs.Depth, hs.DepthRatio)
}
```

`DebugSnapshot` is a **near-time** diagnostic view: safe under concurrent workers, but not a globally atomic stop-the-world snapshot. It does not expose mutable scheduler internals.

---

## 3. Cumulative Lane Counters (StatsGCPressure)

`Queue.StatsGCPressure()` exposes per-lane `Counters` since queue start:

- **`Submitted`**: Every admission attempt after the lane is resolved.
- **`Accepted`**: Successfully enqueued jobs.
- **`Rejected`**: Failed admission (includes queue-full and stopped-queue cases).
- **`QueueFull`**: Rejections due to bounded lane capacity.
- **`Completed`**, **`Failed`**, **`Canceled`**: Terminal worker outcomes (`context.Canceled` counts as canceled, not failed).
- **`Panicked`**: Reserved; always zero until panic recovery exists.

Counters are best-effort under concurrency and are not durable audit logs.

---

## 4. Queue Wait Duration (StatsGCPressure)

When `EnableQueueWaitTiming` is true (default in visibility mode), `StatsGCPressure()` includes queue-wait timing for **accepted** jobs, from admission until execution starts (before `Run()`). It does **not** include user job run time or caller latency after submit.

Low-allocation mode (`LowAllocationObservabilityConfig` or `LowAllocationMode: true`) disables hot-path queue-wait samples; `QueueWait.Count` stays zero. See [production-tuning.md](production-tuning.md).

Global, per-lane, and per-shard `QueueWait` fields expose:

- **`Count`**: Jobs that started execution and contributed a wait sample.
- **`TotalNanos`**: Sum of queue-wait durations.
- **`MaxNanos`**: Maximum observed queue-wait duration.

Use `AverageDuration()` / `MaxDuration()` helpers on `QueueWaitStatsGCPressure` for convenience.

**v1 opt-in:** `Config.Observability.TrackQueueWait` gates legacy `QueueWaitCount` / `QueueWaitTotalNanos` on `Stats()` only. StatsGCPressure queue-wait uses `EnableQueueWaitTiming` (separate flag).

| Metric | Meaning |
|--------|---------|
| Queued depth | How much work is waiting right now |
| Per-lane counters | How much work has passed through each lane over time |
| Queue wait duration | How long accepted work waited before execution |
| Run duration | How long user code runs after execution starts |

---

## 5. Run Duration (StatsGCPressure)

When `EnableRunTiming` is true (default in visibility mode), `StatsGCPressure()` includes run-duration timing for **accepted** jobs, from immediately before `Run()` until `Run()` returns. It does **not** include queue wait or caller latency before submit.

Low-allocation mode disables hot-path run-duration samples; `Run.Count` stays zero.

Global, per-lane, and per-shard `Run` fields expose:

- **`Count`**: Jobs that finished `Run` and contributed a run sample.
- **`TotalNanos`**: Sum of run durations.
- **`MaxNanos`**: Maximum observed run duration.

Use `AverageDuration()` / `MaxDuration()` helpers on `RunStatsGCPressure`.

**Queue wait vs run duration:**

- **High queue wait, low run duration** — scheduler pressure, lane backlog, hot shard, or too few workers.
- **Low queue wait, high run duration** — slow user code or downstream dependencies inside `Run`.
- **Both high** — overload plus slow work once admitted.

Run stats require `EnableRunTiming`. `SlowJobThreshold` only gates the `OnSlowJob` callback when `EnableHooks` is true.

---

## 6. Lane Throughput Counters (Stats v1)

Each lane tracks standard throughput counters since queue startup:
- **`SubmittedTotal`**: Total number of successfully enqueued jobs.
- **`CompletedTotal`**: Total number of jobs that finished with a `nil` error.
- **`FailedTotal`**: Total number of jobs that finished with a non-nil error.
- **`QueueFullTotal`**: Total number of times a submission failed because the lane's queue capacity was saturated.

```go
for _, shard := range stats.Shards {
    for _, lane := range shard.Lanes {
        fmt.Printf("Lane %s: Success=%d, Failed=%d, Rejected=%d\n", 
            lane.Lane, lane.CompletedTotal, lane.FailedTotal, lane.QueueFullTotal)
    }
}
```

---

## 7. Queue Wait Latency (Stats v1 opt-in)

To prevent unneeded epoch polling on hot execution paths, wait-time tracking is fully opt-in and disabled by default. Enable it in the configuration:

```go
cfg := keylane.Config{
    // ... basic config ...
    Observability: keylane.ObservabilityConfig{
        TrackQueueWait: true,
    },
}
```

When enabled, each job tracks its enqueue epoch timestamp. When popped by a worker, the scheduler accumulates wait times. You can query the average wait time for any lane:

```go
avgWait := stats.Shards[0].Lanes[0].AverageQueueWait()
fmt.Printf("Average queue wait: %v\n", avgWait)
```

---

## 8. Observability Hooks (Opt-in Callbacks)

Hooks run only when `EnableHooks` is true. In low-allocation mode, hooks are disabled even if `Hooks` is populated.

### OnJobTiming

Called after every completed job when `EnableHooks` is true and `OnJobTiming` is registered:

```go
Hooks: keylane.Hooks{
    OnJobTiming: func(ev keylane.JobTimingEvent) {
        log.Printf("lane=%s shard=%d wait=%v run=%v outcome=%v",
            ev.Lane, ev.ShardID, ev.QueueWait, ev.RunDuration, ev.Outcome)
    },
},
```

`JobTimingEvent` includes shard ID, lane ID/name, queue wait, run duration, and `JobOutcome` (`Completed`, `Failed`, `Canceled`).

### OnSlowJob

Called when run duration meets or exceeds `SlowJobThreshold`:

```go
Observability: keylane.ObservabilityConfig{
    SlowJobThreshold: 100 * time.Millisecond,
    Hooks: keylane.Hooks{
        OnSlowJob: func(ev keylane.SlowJobEvent) {
            log.Printf("[WARNING] Slow job lane=%s shard=%d wait=%v run=%v threshold=%v",
                ev.Lane, ev.ShardID, ev.QueueWait, ev.RunDuration, ev.Threshold)
        },
    },
},
```

### Design Commitments
- **Run stats configurable**: Cumulative run duration in `StatsGCPressure()` uses `EnableRunTiming` (on by default).
- **Slow detection gated**: `SlowJobThreshold <= 0` or `EnableHooks: false` disables `OnSlowJob`.
- **Lock Isolation**: Hooks execute **outside** shard and scheduler locks.
- **Nil Safety**: Nil hooks are skipped with a branch only.
- **Hook panics**: Hook panics are recovered so a bad observer cannot kill worker goroutines.
- **User panics**: User job panic recovery is not implemented; timing hooks are not guaranteed for panicking jobs.

---

## 9. Low-Allocation Observability Mode (KL-1207)

See [production-tuning.md](production-tuning.md) for when to enable `LowAllocationMode` or `LowAllocationObservabilityConfig()`, and how to benchmark visibility vs low-allocation overhead.

---

## 10. High-Cardinality Warning

> [!WARNING]
> **Do not use high-cardinality values as Lane names.**
>
> Lanes are intended for static job classes (e.g., `"payment"`, `"audit"`, `"webhook"`). 
> Internally, `go-keylane` registers and maps every unique `Lane` name to a compact, contiguous numeric ID. Bounded queue arrays, worker quotas, and counter registries are allocated per-lane. 
> 
> If you dynamically generate `Lane` names using high-cardinality labels (such as unique customer IDs, tenant IDs, order IDs, or timestamps):
> - Memory footprint will explode because internal structures are pre-allocated for every registered lane.
> - Snapshot performance of `Stats()` will degrade significantly due to scanning huge registries.
>
> Always use the **Job Key** (which natively routes to deterministic shards) for dynamic identifiers (tenant, user, customer), and keep **Lanes** bounded to a small, static set of job types.

---

## 11. Out-of-Scope Telemetry Integrations

To keep `go-keylane` lightweight and free from external dependencies:
- It **does not** bundle built-in Prometheus metric exporters.
- It **does not** depend on or package OpenTelemetry SDK/API integrations.
- It **does not** include custom structured loggers or tracing frameworks.

If you need to expose queue metrics to Prometheus or OpenTelemetry, you can easily pull stats on a timer via `q.Stats()` and publish them to your organization's telemetry system from your application's entrypoint.

