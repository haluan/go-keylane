# Go-Keylane Glossary of Terms

This glossary defines the essential terms, internal structs, and operational concepts used in `go-keylane`.

---

## Terms and Concepts

### Key
A string identifier representing a business context (such as tenant ID, customer ID, or order UUID). The key is FNV-1a hashed by the scheduler to assign the job to a specific isolated shard bucket.

### Lane
A label (string) classifying the type of work a job represents (e.g., `payment`, `audit`, `webhook`). Each lane possesses a bounded queue size and a custom execution quota configured during queue initialization.

### Shard
A concurrency isolation boundary. The scheduler partitions enqueued jobs into a fixed number of shards based on the job key's hash. Each shard maintains its own locks and lane queues to prevent high-traffic keys from starving quieter keys on other shards.

### Worker
An active goroutine managed by the scheduler. Workers continuously pop ready shards, acquire quota-limited batches of work, and process those jobs in parallel, bypassing global scheduler lock contention.

### Quota
An execution limit configured per Lane. It determines the maximum number of jobs from a specific lane queue that a single worker pass will execute before yielding the shard to other ready shards. At runtime, quotas can be updated safely via `UpdateQuotaPolicy` without interrupting in-flight work. See [adaptive-quota.md](adaptive-quota.md) and [production-tuning.md](production-tuning.md).

### QuotaVersion
Monotonic generation counter on the active `QuotaPolicySnapshot` (`CurrentQuotaPolicy().Version`). Used for compare-and-swap quota updates and correlating `QuotaChangeEvent` / adaptive decisions.

### LaneClass
Priority classification (`critical`, `normal`, `background`, `best_effort`) shared by admission, overload, and adaptive quota. See [lane-priority.md](lane-priority.md).

### Admission policy
Per-lane rules evaluated before enqueue: class, `RejectAboveRatio`, and `MaxQueueDepth`. See [admission-control.md](admission-control.md).

### Overload action
Pre-enqueue decision: `keep`, `reject`, `shed`, or `degrade`. May include advisory `RetryAfter` and `BackoffHint`. See [overload-policy.md](overload-policy.md).

### Adaptive quota
Optional periodic controller that adjusts lane drain quotas within configured min/max bounds using pressure and queue-wait signals. Disabled by default. See [adaptive-quota.md](adaptive-quota.md) and [adaptive-tuning.md](adaptive-tuning.md).

### Hot key
A logical job key that concentrates submissions on one shard. v0.5 detection reports **hot key candidates** using bounded per-shard tracking and `KeyHash` diagnostics. See [hot-key-detection.md](hot-key-detection.md).

### Hot key candidate
An approximate detection signal from v0.5.0 — not confirmed root cause. Candidates may false-positive during bursts or tracker eviction.

### Localized pressure
Overload concentrated on one hot key or one shard, as opposed to many shards simultaneously. Per-key mitigation often helps more than scale-out.

### Distributed backlog
Many shards pressured at once with no single dominant key. Scale-out may help. See [shard-pressure-diagnostics.md](shard-pressure-diagnostics.md).

### Scale signal
Advisory output from `Queue.ScaleSignal()` — not an autoscaler. Reports `Recommended`, `Reason`, and `Scope` for external platforms. See [autoscaling-signals.md](autoscaling-signals.md).

### Mitigation action
Per-key admission outcome: `allow`, `throttle`, `reject`, or `shed`. See [per-key-admission-policy.md](per-key-admission-policy.md).

### InternalJob
The scheduler's internal wrapper around a user-submitted `Job`. It records management timestamps (such as submission time) to calculate precise queue wait latency.

### LaneID
A compact, 0-indexed integer representation of a registered `Lane` string. Alphabetically indexed during configuration registry instantiation to replace runtime map queries with efficient slice lookups.

### laneQueue
The internal ring buffer data structure used inside shards to queue pending `InternalJob` objects per Lane. Built as a fixed-capacity ring buffer to ensure zero slice re-allocations and stable allocations.

### readyCh
The central scheduler channel (`chan int`) storing the IDs of shards that contain ready work. Workers listen on this channel to pull and process active shards in a clean round-robin loop.

### Future
A concurrency primitive returned by `SubmitValue`. It represents a pending asynchronous computation. Callers use `Await` to block the current thread until the worker completes the job and resolves the future with its return value.

### Drain
A shutdown lifecycle option (`WithDrain(true)`). It configures the queue `Stop()` operation to block and allow all currently enqueued jobs to be fully processed by active workers before exiting the program.
