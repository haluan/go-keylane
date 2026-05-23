# GC pressure shaping

`keylane` is a Go lane-sharded concurrency control library. It helps services **shape GC pressure** by bounding and smoothing execution under load. It does **not** replace the Go garbage collector or avoid Go GC pauses.

> **Invariant:** `keylane` does not avoid Go GC pauses. It helps prevent services from feeding the GC with uncontrolled concurrency, goroutine explosion, allocation bursts, and request pile-up.

For operational tuning and signals, see [production-tuning.md](production-tuning.md), [observability.md](observability.md), and [benchmarks.md](benchmarks.md).

---

## What happens without bounded concurrency

Under an incoming request burst, a typical failure mode looks like this:

```text
incoming request burst
  -> uncontrolled goroutine / job execution
  -> allocation burst
  -> heap growth
  -> more GC work and GC assist pressure
  -> worse p95/p99 under overload
```

The Go runtime still controls when collections run. You cannot turn that off from application code.

---

## What keylane changes

With admission, bounded queues, and a fixed worker pool:

```text
incoming request burst
  -> scheduler admission
  -> bounded queues
  -> lane and shard fairness
  -> bounded in-flight execution
  -> smoother allocation rate
  -> more stable p95/p99 under pressure
```

`keylane` does **not** replace the Go scheduler. It bounds **your** in-process work queueing and execution so overload shows up as queue wait, `ErrQueueFull`, or pressure signals instead of unbounded goroutines and memory growth.

---

## What keylane shapes

| Mechanism | Effect |
|-----------|--------|
| Bounded shards and lanes | Preallocated ring buffers; `ErrQueueFull` instead of unbounded slice growth on enqueue |
| Fixed worker pool | One goroutine per worker, not per job — limits goroutine explosion |
| Lane quotas | Fairness across workload classes on each shard pass |
| Optional `sync.Pool` batch reuse | Can reduce scheduler-layer allocations on worker paths (see benchmarks) |
| Map-free hot paths | Lane indexing by numeric ID, not per-job string maps in the worker loop |

These reduce **concurrency-driven** allocation bursts and queue chaos. They do not eliminate allocations inside your `Run` callbacks.

---

## What keylane cannot control

- **User closure allocations** — Captures, maps, slices, and JSON parsing in `Run` are your application's heap traffic.
- **Go runtime GC** — Pause timing, cycle length, and sweep behavior are controlled by the runtime and environment (`GOGC`, `GOMEMLIMIT`, etc.).
- **Downstream latency** — Slow databases or HTTP calls inside `Run` are visible as high **run duration**, not as something keylane can GC-tune away.

---

## How to evaluate claims

Use the [production benchmark suite](benchmarks.md) to compare allocation and fairness under load:

```bash
make bench-production
```

The suite measures whether keylane reduces concurrency-driven allocation bursts and queue unfairness. **It does not prove that Go GC pauses are eliminated.**

Optional GC investigation (environment-specific):

```bash
GODEBUG=gctrace=1 go test -bench=GCPressure -benchmem ./benchmarks
```

See also [benchmarks/README.md](../benchmarks/README.md).

---

## Application practices

1. **Avoid heavy captures in closures** — Prefer stable `Run` targets or pooled structs for per-job state.
2. **Reuse buffers** in job bodies where possible.
3. **Use stable shard keys** (tenant, customer) on `Job.Key`; keep **lanes** as a small static set of workload classes.
4. **Prefer early reject** via `Pressure()` and bounded capacity over unbounded queue growth that hides overload as latency.

---

## Related documentation

- [observability.md](observability.md) — `StatsGCPressure()`, `Pressure()`, `DebugSnapshot()`, hooks
- [debugging.md](debugging.md) — queue wait vs run duration, hot shard/lane
- [production-tuning.md](production-tuning.md) — workers, capacity, low-allocation mode
- [phase-7-gc-pressure-shaping.md](phase-7-gc-pressure-shaping.md) — implementation-era notes
