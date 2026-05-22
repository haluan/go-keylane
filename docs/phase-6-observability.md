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

## 2. Cumulative Lane Counters (StatsGCPressure)

`Queue.StatsGCPressure()` exposes per-lane `Counters` since queue start:

- **`Submitted`**: Every admission attempt after the lane is resolved.
- **`Accepted`**: Successfully enqueued jobs.
- **`Rejected`**: Failed admission (includes queue-full and stopped-queue cases).
- **`QueueFull`**: Rejections due to bounded lane capacity.
- **`Completed`**, **`Failed`**, **`Canceled`**: Terminal worker outcomes (`context.Canceled` counts as canceled, not failed).
- **`Panicked`**: Reserved; always zero until panic recovery exists.

Counters are best-effort under concurrency and are not durable audit logs.

---

## 3. Lane Throughput Counters (Stats v1)

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

## 4. Queue Wait Latency (Opt-in)

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

## 5. Slow Job Hook (Opt-in Callback)

You can register callback hooks to notify when job executions are unusually slow:

```go
cfg := keylane.Config{
    // ... basic config ...
    Observability: keylane.ObservabilityConfig{
        SlowJobThreshold: 100 * time.Millisecond,
        Hooks: keylane.Hooks{
            OnSlowJob: func(ev keylane.SlowJobEvent) {
                log.Printf("[WARNING] Slow job executed in lane %s (shard %d): took %v", 
                    ev.Lane, ev.ShardID, ev.Duration)
            },
        },
    },
}
```

### Design Commitments
- **Zerotiming Overhead**: If `SlowJobThreshold == 0`, no timing calculation takes place.
- **Lock Isolation**: Slow job hooks execute **strictly outside** shard and scheduler locks, meaning a slow hook cannot block other active workers or shard queues.
- **Nil Safety**: If `OnSlowJob` is `nil`, the timing is ignored gracefully.

---

## 6. High-Cardinality Warning

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

## 7. Out-of-Scope Telemetry Integrations

To keep `go-keylane` lightweight and free from external dependencies:
- It **does not** bundle built-in Prometheus metric exporters.
- It **does not** depend on or package OpenTelemetry SDK/API integrations.
- It **does not** include custom structured loggers or tracing frameworks.

If you need to expose queue metrics to Prometheus or OpenTelemetry, you can easily pull stats on a timer via `q.Stats()` and publish them to your organization's telemetry system from your application's entrypoint.

