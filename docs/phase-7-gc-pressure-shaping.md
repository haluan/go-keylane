# Phase 7: GC Pressure Shaping

> See [gc-pressure-shaping.md](gc-pressure-shaping.md) for the v0.2 operator guide.

Go-Keylane is meticulously engineered to minimize GC pressure on high-frequency execution pipelines.

## Critical Invariant Statement

> [!IMPORTANT]
> `go-keylane` helps reduce GC pressure caused by uncontrolled concurrency, goroutine explosion, unbounded queues, and allocation bursts. It does not avoid Go GC pauses.

---

## 1. What Go-Keylane Shapes (Controls)

- **Bounded Shards & Lanes**: By preallocating backing ring buffers (`laneQueue`) at construct-time and rejecting submissions past capacity with `ErrQueueFull`, memory footprints are strictly upper-bounded. Slice growth or re-allocation never occurs on the enqueuing path.
- **Worker Batch Pooling**: `processShard` can optionally reuse batch slices using `sync.Pool`. Under baseline benchmarks, this has demonstrated the ability to eliminate scheduler-layer slice allocations on the worker loops (reducing them to 0 B/op and 0 allocs/op in baseline tests). Note that these benchmark numbers serve as baseline performance indicators rather than runtime guarantees, as actual heap allocations depend heavily on runtime environment state, Go compiler optimizations, and job closure captures.
- **Map-Free Execution**: The worker loop and scheduler use slice-based offset indexing via deterministic alphabetical `LaneID` mapping. String map lookups are completely bypassed, eliminating key-hashing allocations on active workers.
- **Goroutine Bounds**: Instead of spawning a goroutine per job, a fixed set of persistent worker goroutines process all shard lanes, completely preventing goroutine leaks and runtime thread explosions.

---

## 2. What Go-Keylane Cannot Control

- **User Closure Allocations**: If your job's `Run` closure captures variables, instantiates local maps/slices, or parses large JSON payloads, those allocations belong to your application context. Go-Keylane executes them outside scheduler locks, but the garbage collector will still need to collect them.
- **Go Runtime GC Pauses**: GC pauses, cycle durations, and sweep phases are controlled entirely by the Go runtime and environment configurations (such as the `GOGC` and `GOMEMLIMIT` variables).

---

## 3. Best Practices to Reduce App GC Pressure

1. **Avoid Capturing in Closures**: Define statically typed job run targets, or use pooled structs to hold closure variables.
2. **Re-use Buffers**: Pass preallocated structures or slices to your job runner.
3. **Use ValueJob Wisely**: `SubmitValue` uses a generic `Future[T]` to return values. When awaiting values, avoid allocating new intermediate pointers when primitive types or pre-allocated structures are sufficient.
