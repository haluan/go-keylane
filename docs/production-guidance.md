# Go-Keylane Production & Operational Guidance

This guide contains operational recommendations, capacity planning rules, and design choices for deploying `go-keylane` to production systems.

For shard keys, lanes, workers, queue capacity, pressure admission, and observability modes, see [production-tuning.md](production-tuning.md).

---

## 1. Capacity Tuning

### Worker Count Tuning
- **Initial Baseline:** Start with `worker_count <= GOMAXPROCS`. Having too many workers compared to available CPU threads increases context-switching overhead without increasing throughput.
- **Tuning rule:** If your job callbacks contain heavy blocking operations (like external HTTP requests or SQL queries), you can tune the worker count higher. If your jobs are strictly CPU-bound, match the CPU core count exactly.

### Shard Count Sizing
- Shards act as isolated lock buckets. 
- Higher shard counts (e.g. 32, 64, or 128) minimize lock contention but increase the static memory allocation footprint because each shard pre-allocates its own internal lane queues.
- A good rule of thumb is choosing a shard count that is at least 4 to 8 times the worker thread count.

---

## 2. Structural Partitioning

### Choosing Keys
- The **Key** is your scheduling isolation boundary. Choose keys representing stable, logical entities like `tenant_id`, `customer_id`, or `merchant_uuid`.
- **Warning:** Avoid high-churn, single-use keys (like random request IDs or timestamps) as they scatter work evenly across all shards, defeating the purpose of noisy neighbor key isolation.

### Choosing Lanes
- Group jobs into lanes based on their Service Level Objectives (SLOs) and execution urgency (e.g., `payment` vs `email_notification`).
- **Warning:** Avoid registering too many lanes (e.g., > 10) as each lane inside a shard allocates memory for its queue. Keep lanes compact and descriptive.

---

## 3. Graceful Error & Lifecycle Handling

### Handling `ErrQueueFull`
- When a shard-lane queue reaches its maximum capacity, `Submit` and `SubmitValue` return `ErrQueueFull` immediately to apply backpressure.
- Adopters must implement a concrete fallback strategy:
  - **Shedding:** Drop the request and return an HTTP `429 Too Many Requests` status code back to the client.
  - **Retrying:** Enqueue the request to an out-of-process dead-letter queue (DLQ) or apply exponential backoff.
  - **Sinking:** Siphon the payload to a persistent fallback database.

### Handling `ErrStopped`
- During graceful program shutdown, new job submissions return `ErrStopped`. 
- Critical shutdown-sensitive service layers must check this error and safely roll back transactions or route incoming work to redundant nodes.

---

## 4. Await Deadlock Risk

> [!CAUTION]
> **Never call `Await` inside a worker `Run` function on the same queue.**
>
> If a worker thread picks up a job that blocks on `Await` for another job submitted to the *same* queue, it consumes a worker slot. If all worker threads block on Await, the queue scheduler deadlocks forever (worker starvation).
>
> **Remediation:**
> - Submit dependent tasks sequentially from the initial client caller side.
> - Run jobs in separate, decoupled `Queue` instances.

---

## 5. Job Execution Rules

### Context-Aware Jobs
- All user-defined job callbacks must respect context cancellation:
  ```go
  Run: func(ctx context.Context) error {
      // In long loops or blocking steps, check for cancellation:
      select {
      case <-ctx.Done():
          return ctx.Err()
      default:
      }
      return nil
  }
  ```

### Avoid Long Blocking Jobs in Shared Lanes
- A job that blocks for a long time (e.g., an HTTP call without a timeout) holds up worker slots and stalls lane processing.
- Set aggressive timeouts on all database and external network calls.

---

## 6. Monitoring & Observability

### Key Metrics to Watch
Adopters should regularly query `Stats()` and export key values to Prometheus or Datadog:
- **`TotalDepth`**: Tracks total pending jobs. Sustained increases indicate that worker processing capacity is saturated.
- **`QueueFullTotal`**: Incremented when backpressure drops jobs. Alert immediately if this value climbs, indicating queue saturation.
- **`FailedTotal`**: Tracks jobs returning execution errors. High rates signal application bugs or database issues.
- **`QueueWaitTotalNanos` / `QueueWaitCount`**: Calculate the average queue wait latency per lane:
  $$\text{AvgWaitTime} = \frac{\text{QueueWaitTotalNanos}}{\text{QueueWaitCount}}$$
  Monitor the p95 and p99 queue wait latency to detect scheduler queue lag.

---

## 7. Decoupling Queues

### When to Use Separate Queues
While `go-keylane` supports multiple lanes, you should deploy separate, independent queue scheduler instances when:
1. **SLOs are highly divergent**: A high-speed API pathway and a bulk monthly report system should not share a worker pool.
2. **Failure isolation is critical**: An outage in the billing queue must never impact the health of the core authentication queue.
