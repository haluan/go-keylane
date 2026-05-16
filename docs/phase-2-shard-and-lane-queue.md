# Phase 2: Shard and Lane Queue

Phase 2 implements the internal data structures that manage job queueing and routing.

## Architecture

### Hash-to-Shard Routing
Jobs are routed to a specific **Shard** based on their **Key**. This is a two-step process:
1.  **Hashing**: The string key is hashed using the FNV-1a algorithm (`internal/core/hash.go`).
2.  **Routing**: The hash is taken modulo the `ShardCount` to determine the `ShardID` (`internal/core/route.go`).

This ensures that all jobs with the same key always land in the same shard, providing a foundation for sequential execution within a key if needed (though Phase 2 does not yet implement the scheduler).

### Lane Queues
Inside each shard, jobs are separated into **Lanes**. Each lane has its own dedicated queue (`internal/core/lane_queue.go`).

- **Ring Buffer**: The queue is implemented as a fixed-capacity ring buffer.
- **Zero Allocations**: The backing slice is pre-allocated at startup. Pushing and popping jobs does not trigger any heap allocations in the steady state.
- **FIFO**: Each lane queue preserves First-In-First-Out order.

### Shards and Concurrency
A **Shard** (`internal/core/shard.go`) is the unit of concurrency isolation.
- Each shard has its own `sync.Mutex`.
- Different shards can be processed by different workers in parallel without contention.
- The `ready` flag indicates that a shard has at least one job and is eligible for scheduling.

## Enqueue Flow
When a job is submitted (`internal/core/enqueue.go`):
1.  The shard is locked.
2.  The job is pushed into the correct lane queue.
3.  If the shard was not already `ready`, it is marked as `ready`.
4.  The shard is unlocked.

Execution of the job function itself never happens while holding a shard lock. Phase 2 only covers the transition from a public `Job` to a queued `InternalJob`.
