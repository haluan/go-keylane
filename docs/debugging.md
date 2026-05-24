# Go-Keylane Troubleshooting & Debugging Guide

This document provides a comprehensive troubleshooting guide for resolving performance bottlenecks, queue saturation, and lifecycle issues. See [observability.md](observability.md) for API overview.

---

## 1. Symptom → signal map

| Symptom | Signal to inspect | Likely meaning |
|---------|-------------------|----------------|
| High latency | `QueueWait` + `Run` in `StatsGCPressure()` | Separate queueing delay from execution time |
| High queue wait | Per-lane depth, shard depth, `DebugSnapshot().HotLanes` / `HotShards` | Backlog, insufficient workers, or lane quota imbalance |
| High run duration | `Run` in `StatsGCPressure()`, `OnSlowJob` hook | Slow `Run` body or downstream dependency |
| Queue full | `Counters.QueueFull`, `Pressure()` | Capacity limit or admission policy; shed load earlier |
| One lane dominates | `DebugSnapshot().HotLanes` | Lane quota or workload class imbalance |
| One shard dominates | `DebugSnapshot().HotShards` | Shard key skew (hot tenant/customer on one key) |
| Localized hot key | `PressureSummary.MitigationRelevant`, `ShardPressure.Class == localized_key` | Single key dominates; scale-out may not help |
| Distributed overload | `PressureSummary.ScaleRelevant`, `Class == distributed` | Many shards hot; capacity or admission tuning |
| Worker saturation | `PressureSummary.WorkerBusyRatio`, `Class == worker_bound` | Workers busy while depth looks moderate |
| `Await()` timeout | Queue wait + run duration + `Pressure()` | Caller deadline shorter than queue/run behavior; job may still run |
| Quota changes too often | `AdaptiveDebugSnapshot()`, `OnQuotaChange` | Short cooldown, narrow hysteresis band, or aggressive pressure thresholds |
| Quota never changes | `AdaptiveDebugSnapshot().Running`, `LastDecision` | Warmup, insufficient samples, disabled increase/decrease, at min/max bound |
| Overload / shed storms | `OverloadRejected`, `OverloadShed`, `OnOverloadPolicyDecision` | Lane class too aggressive; best-effort shedding expected under load |
| Missing v0.4 hook events | `Observability.EnableHooks` | Low-allocation mode disables hooks; keep decisions do not emit overload events |

If p99 is high, inspect queue-wait and run-duration percentiles (or averages from cumulative stats) **separately**. High queue wait with normal run duration means scheduler backlog. Normal queue wait with high run duration means slow user code or I/O inside `Run`.

---

## 2. Quick Checklist

Follow these 10 steps to isolate scheduler performance problems:

1. **Verify if the queue scheduler is started:** Check if `q.Start(ctx)` was executed and returned nil.
2. **Review configuration settings:** Check `ShardCount`, `WorkerCount`, and `QueueSizePerLane` for capacity mismatches.
3. **Inspect queue-full rejections:** `StatsGCPressure().Lanes[].Counters.QueueFull` (cumulative) or v1 `Stats()` lane `QueueFullTotal`.
3b. **Inspect scheduler pressure and lane history:** Use `Pressure()` for a quick overload signal (`IsPressured`, `IsOverloaded`). Use `PressureSummary()` for KL-1503 pressure class and scale/mitigation flags. Use `DebugSnapshot()` for hot shard/lane rankings (`HotShards`, `HotLanes`). Use `StatsGCPressure()` for cumulative counters and queue-wait/run timing. High average or max queue wait usually means lane pressure, hot shards, or too few workers.
4. **Identify hot shards and lanes:** `DebugSnapshot().HotShards` and `HotLanes` list the top backlog by depth. Use job **keys** (not lane names) for per-tenant routing; lanes should stay a small static set.
5. **Identify the hot key:** Check if a single noisy key is routing heavy traffic to a single shard. With hot key tracking enabled, inspect `DebugSnapshot().Shards[].HotKeyCandidate` — see [hot-key-mitigation.md](hot-key-mitigation.md).
6. **Run the Go race detector:** Execute `go test -race ./...` to verify there are no active data races.
7. **Analyze active workers stack traces:** Collect a pprof goroutine dump (`go tool pprof`) to verify if worker goroutines are blocked.
8. **Examine queue wait times:** `StatsGCPressure().QueueWait` or per-lane `lane.QueueWait.AverageDuration()` (requires `EnableQueueWaitTiming`).
9. **Check context cancellation propagation:** Ensure jobs check `ctx.Done()` periodically during processing.
10. **Check for Await deadlocks:** Ensure you are not calling `Await()` from inside a worker on the same queue.
11. **Assess worker processing limits:** Increase `WorkerCount` if database calls or network requests are highly latent.

---

## 3. Request is Waiting Too Long

If jobs are experiencing high latency before execution:
- **Root Cause:** All worker threads are fully utilized, or a popped shard contains a massive backlog of high-priority lane jobs that starve lower lanes.
- **Remediation:** 
  - Calculate average queue wait delay via the observability counters.
  - Increase the global `WorkerCount` in configuration to process shards faster.
  - Lower the individual lane quotas so workers cycle through shards faster.

---

## 4. Lane is Full

When submissions fail with `ErrQueueFull` or `TrySubmit` returns `false`:
- **Root Cause:** Job enqueue rates are higher than worker processing capacity, causing the bounded `laneQueue` buffer to saturate.
- **Remediation:**
  - Increase `QueueSizePerLane` to handle transient load spikes.
  - Optimize the execution speed of the job callback function to increase processing throughput.
  - Apply client-side exponential backoff or rate-limiting to curb incoming request traffic.

---

## 5. Key is Noisy

A single tenant or user is sending a disproportionate volume of jobs:
- **Root Cause:** A hot key is bombarding the scheduler. The scheduler successfully isolates the key to its assigned shard, but the lane queues inside that shard are saturated.
- **Remediation:**
  - Standard keylane round-robin shard scheduling will prevent this shard from starving other shards.
  - Ensure that quiet tenants use distinct keys so they route to separate, healthy shards.
  - Apply client-side rate limits to the offending noisy key before submitting jobs to the queue.

---

## 6. Workers are Saturated

Workers are constantly active and CPU utilization is pinned at 100%:
- **Root Cause:** The worker thread pool is too small for the incoming CPU-bound computational load.
- **Remediation:**
  - Profile the process using `pprof` to identify CPU hotspots.
  - Set `WorkerCount` to match the exact number of logical CPU cores (`runtime.NumCPU()`).

---

## 7. Queue Wait vs. Run Time

Use `StatsGCPressure()` to separate scheduler delay from user execution time:

- **Queue wait** (`QueueWait`): time from admission to the lane queue until execution starts (before `Run()`).
- **Run duration** (`Run`): time spent inside `Run()` only.

```text
Average queue wait = QueueWait.TotalNanos / QueueWait.Count
Average run duration = Run.TotalNanos / Run.Count
```

Or use `QueueWait.AverageDuration()` and `Run.AverageDuration()`.

**Analysis:**

- **High queue wait, low run duration** — scheduler pressure, insufficient workers, hot lane/shard, or lane quota tuning.
- **Low queue wait, high run duration** — slow user code, DB/HTTP latency, or blocking inside `Run`.
- **Both high** — overload and slow work once admitted; consider backpressure, worker tuning, and profiling `Run`.

Optional hooks: `OnJobTiming` reports per-job queue wait and run duration; `OnSlowJob` fires when run duration exceeds `SlowJobThreshold`.

---

## 8. SubmitValue/Await Timeout

Your caller thread receives `context.DeadlineExceeded` when executing an `Await(ctx)` call:

> [!WARNING]
> **Await timeout means the caller stopped waiting. It does not necessarily mean the job was cancelled.**
>
> When the `Await` context times out, the calling thread unblocks immediately to maintain API responsiveness. However, the job remains in the scheduler's internal shard queue or may already be executing on a worker goroutine. It will continue running to completion unless the job's `Run` callback itself checks for context cancellation (`ctx.Done()`).

- **Remediation:** 
  - Ensure your job's `Run` function respects context cancellations.
  - Increase the timeout threshold on the `Await` context to match execution latencies.

---

## 9. Shutdown is Blocking

Calling `q.Stop(ctx, keylane.WithDrain(true))` blocks indefinitely or times out:
- **Root Cause:** Active workers are executing long-running or infinitely blocked jobs, or the enqueued backlog is too massive to drain within the shutdown context deadline.
- **Remediation:**
  - Ensure all job callbacks check `ctx.Done()` and exit promptly when cancelled.
  - Provide a reasonable timeout on the shutdown context (e.g., 5 or 10 seconds) so the process can exit even if some jobs remain active.

---

## 10. v0.4 adaptive and overload

For quota policy, overload, and adaptive controller issues:

| Symptom | Doc |
|---------|-----|
| Critical lane high queue wait | [lane-priority.md](lane-priority.md), [adaptive-quota.md](adaptive-quota.md) |
| Best-effort rejected too often | [lane-priority.md](lane-priority.md), [overload-policy.md](overload-policy.md) |
| Quota oscillation or no changes | [adaptive-tuning.md](adaptive-tuning.md), [adaptive-observability.md](adaptive-observability.md) |
| Missing overload events or Retry-After | [overload-policy.md](overload-policy.md), [adaptive-observability.md](adaptive-observability.md) |
| Controller not running after Start | [adaptive-quota.md](adaptive-quota.md) — `AdaptiveDebugSnapshot().Running` |
| Benchmark regression with adaptive on | [benchmarks/adaptive-quota.md](benchmarks/adaptive-quota.md) |

---

## 11. Race-Condition Debugging

If you encounter inconsistent behavior, memory corruption, or unexpected panics:
- **When to check:** Always run tests and local builds with Go's built-in race detector enabled.
- **Command:**
  ```bash
  go test -race ./...
  ```
- **Analysis:** If the detector reports a data race, trace the concurrent access back to shared memory states inside your job callback closures.
