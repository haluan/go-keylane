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
An execution limit configured per Lane. It determines the maximum number of jobs from a specific lane queue that a single worker pass will execute before yielding the shard to other ready shards. At runtime, quotas can be updated safely via `UpdateQuotaPolicy` without interrupting in-flight work (see production tuning guide).

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
