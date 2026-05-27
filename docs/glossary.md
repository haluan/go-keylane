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

### FailureKind (v0.6)
Structured classification of handler errors (`retryable`, `permanent`, `timeout`, `cancelled`, `overloaded`, `rejected`, `deadline_exhausted`, etc.). See [failure-policy.md](failure-policy.md) and [v0.6.0 overview](v0.6-retry-deadline-failure-policy.md).

### RetrySafety (v0.6)
Duplicate-safety declaration on `Idempotency` (`safe`, `unsafe`, `requires_check`). See [idempotency.md](idempotency.md).

### Retry suppression (v0.6)
Runtime-health gate that blocks in-worker retries under pressure, overload, or hot keys. See [retry-suppression.md](retry-suppression.md).

### DeadlineBudget (v0.6)
Caller deadline visibility after queue wait and runtime. See [deadline-budget.md](deadline-budget.md).

### Pipeline (v0.7)
A typed multi-stage request submitted with `SubmitPipeline[S,O]`. Stages share one state type `S` and run sequentially in-worker. See [request-pipeline.md](request-pipeline.md).

### Stage / StageName (v0.7)
A low-cardinality label for one step in a pipeline (`validate`, `db_read`, custom stable names). Used in `StageObservation` and `StageFailure`, not as metric labels for raw keys or request IDs.

### StageExecutionContext (v0.7)
Immutable request/stage execution metadata attached to `context.Context` during `SubmitPipeline` stages and `SubmitRequest` handlers. Includes shard, lane, stage index, attempt, and deadline snapshot. See [stage-execution-context.md](stage-execution-context.md).

### Continuation (v0.7)
A handle returned from a `RunContinuation` stage when work continues outside the Keylane worker. Resolved by `ContinuationCompleter.Complete`, `Fail`, or `Cancel`. See [continuations.md](continuations.md).

### Continuation completer (v0.7)
The `ContinuationCompleter[S]` interface that drives continuation resolution. First call wins; later calls return `false` and may count as late completion. See [continuations.md](continuations.md).

### Late continuation completion (v0.7)
A `Complete`/`Fail`/`Cancel` after the continuation was already resolved (cancel, deadline, or earlier completer). Increments `DebugSnapshot.Continuation.LateCompletions` and may fire `OnContinuationLate`. See [continuations.md](continuations.md).

### Backend resource (v0.7)
A low-cardinality name for a downstream system (`primary-db`, `wallet-api`). Configured under `BackendResources` with per-lane capacity limits.

### Backend lane (v0.7)
A low-cardinality class of downstream usage (`db_read`, `db_write`, `external_api`, `cache_read`, `cache_write`). Distinct from request **Lane**.

### Backend lease (v0.7)
Permission to use one slot of backend capacity until `Release()` is called. See [backend-resource-coordination.md](backend-resource-coordination.md).

### Backend admission (v0.7)
The decision to grant or deny a backend lease for a resource/lane (`BackendAdmissionAccepted`, `BackendAdmissionSaturated`, etc.). Distinct from request-queue admission. See [backend-resource-coordination.md](backend-resource-coordination.md).

### Backend saturation (v0.7)
When `inflight >= MaxInFlight` for a resource/lane and admission mode is `reject`. Reported as `BackendAdmissionSaturated` in hooks and snapshots.

### DB/API pressure adapter (v0.7)
An implementation of `BackendPressureProvider` that maps external pool telemetry into `BackendPressureSnapshot`. Built-in adapters: `SQLDBPressureAdapter` (`database/sql` stats) and `APIClientPressureAdapter` (custom bounded API client / semaphore). Observational only — keylane does not reject requests from pool pressure unless the application gates on snapshots. See [backend-pressure-adapters.md](backend-pressure-adapters.md).

### BackendPressureProvider (v0.7)
Optional interface for one `backend_resource` + `backend_lane` pressure probe. Implemented by DB/API pressure adapters and custom providers. See [backend-pressure-adapters.md](backend-pressure-adapters.md).

### BackendPressureSnapshot (v0.7)
Low-cardinality pool pressure view: `InUse`, `Capacity`, `Idle`, `WaitCount`, `WaitTime`, `Saturated`, `Pressure`. Emitted via `Queue.BackendPressure` and `OnBackendPressure` hooks.
