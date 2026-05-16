# Phase 3: Worker Scheduler

Phase 3 introduces the `Scheduler` and worker goroutines that execute jobs with per-lane quota fairness.

## Architecture

### Submit Flow
When a job is submitted via `Queue.Submit`:
1.  The job is validated.
2.  The lane is looked up to get an internal `LaneID`.
3.  The job key is hashed to determine the `ShardID`.
4.  The job is converted to an `InternalJob` and enqueued into the correct shard/lane.
5.  If the shard was not already Ready, its `ShardID` is sent to the `ReadyCh`.

### Worker Loop
A set of worker goroutines (defined by `WorkerCount`) run a loop that:
1.  Waits for a `ShardID` from the `ReadyCh`.
2.  Processes the shard using `processShard`.

### Process Shard
`processShard` performs the following steps:
1.  Locks the shard.
2.  Pops a batch of jobs from each lane queue according to the configured `LaneQuotas`.
3.  Checks if the shard still has remaining work. If not, clears the `Ready` flag.
4.  Unlocks the shard.
5.  Executes each job in the batch outside the shard lock.
6.  If the shard still has more work, it is requeued into the `ReadyCh`.

### Quota Fairness
By popping jobs up to a specific quota per lane in each processing cycle, the scheduler ensures that a noisy lane (one with many pending jobs) does not starve other lanes in the same shard.

## Lifecycle Management
- `New(config)`: Validates configuration and initializes internal structures.
- `Start(ctx)`: Launches worker goroutines. It is idempotent via `sync.Once`.
- Jobs run as long as the context passed to `Start` is alive.

## Note on Future Phases
Phase 3 provides fire-and-forget job execution. Refinements such as `Future`, `SubmitValue`, `Stats`, and graceful shutdown/drain will be implemented in later phases.
