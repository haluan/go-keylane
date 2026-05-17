# Phase 8 — Testing Strategy & Guidelines

This document outlines the testing strategy, design guidelines, and structure for the consolidated `go-keylane` test suite.

---

## 1. Test Design Rules

To ensure rapid, deterministic, and highly reliable execution on any host, all tests must follow these absolute rules:

1. **No `time.Sleep`**: Never use sleeps for synchronization or timing windows. Use channels, wait groups (`sync.WaitGroup`), or `context.WithTimeout` context limits.
2. **Standard test timeout**: Utilize the helper function `testTimeout(t *testing.T) context.Context` which applies a deterministic **2-second** timeout for all testing scopes, preventing deadlocks or infinite blocking.
3. **Deterministic routing**: Utilize `findKeyForShard` (internal) and `findKeyForShardPublic` (public) to discover key strings routing deterministically to specific shards instead of relying on hash luck.
4. **Worker tracking**: Never track thread counts with `runtime.NumGoroutine()`. Always use `workerWG` or standard wait group synchronization wrappers.

---

## 2. Test Suite Categories

The test suite is organized into logical, well-structured files covering both internal and public surface areas:

### 2.1. Helpers
- [test_helpers_test.go](file:///Users/haluan.irsad/Documents/go-work/code/go-keylane/test_helpers_test.go): Contains public testing configurations, standardized mock queue instantiation, and channel waiting tools (`waitForSignal`, `waitForN`).
- [internal/core/test_helpers_test.go](file:///Users/haluan.irsad/Documents/go-work/code/go-keylane/internal/core/test_helpers_test.go): Contains FNV key hashing target search algorithms to find keys mapping to exact shard indexes.

### 2.2. Routing Determinism
- [routing_test.go](file:///Users/haluan.irsad/Documents/go-work/code/go-keylane/routing_test.go): Verifies that identical keys deterministically map to the same shard, shard IDs lie within standard ranges, and single-shard config routes everything accurately.
- [internal/core/route_test.go](file:///Users/haluan.irsad/Documents/go-work/code/go-keylane/internal/core/route_test.go): Directly exercises FNV hash keys and internal route shard mapping functions.

### 2.3. Fairness & Noisy Isolation
- [fairness_test.go](file:///Users/haluan.irsad/Documents/go-work/code/go-keylane/fairness_test.go): Verifies that lane-level quotas are strictly respected and that quieter lanes get allocated executing slots in round-robin turns without starvation.
- [noisy_lane_test.go](file:///Users/haluan.irsad/Documents/go-work/code/go-keylane/noisy_lane_test.go): Asserts that a high-volume lane within a shard does not starve other quieter lanes sharing the same shard.
- [noisy_key_test.go](file:///Users/haluan.irsad/Documents/go-work/code/go-keylane/noisy_key_test.go): Verifies that high-activity keys routed to Shard A do not impact or slow down processing of distinct keys routed to Shard B.
- [internal/core/process_shard_test.go](file:///Users/haluan.irsad/Documents/go-work/code/go-keylane/internal/core/process_shard_test.go): Directly processes single worker runs on shard structures to prove exact pass fairness.

### 2.4. Value Submission correctness
- [submit_value_test.go](file:///Users/haluan.irsad/Documents/go-work/code/go-keylane/submit_value_test.go): Exercises public value submission paths including Sentinel error resolution, value returning, and zero-value assignments under failures.

### 2.5. Future Await Timeout
- [future_test.go](file:///Users/haluan.irsad/Documents/go-work/code/go-keylane/future_test.go): Validates that a short await context deadline generates `context.DeadlineExceeded` cleanly, without corrupting the future's state or preventing later successful resolving/awaiting.

### 2.6. Graceful Lifecycles
- [shutdown_test.go](file:///Users/haluan.irsad/Documents/go-work/code/go-keylane/shutdown_test.go): Confirms graceful lifecycles, ensuring stop commands block on worker termination, enqueued jobs completely drain before stop returns, and stop remains idempotent.
- [internal/core/shutdown_test.go](file:///Users/haluan.irsad/Documents/go-work/code/go-keylane/internal/core/shutdown_test.go): Internal unit testing of worker group teardown loops.

### 2.7. Concurrent Safety (Race detector)
- [race_test.go](file:///Users/haluan.irsad/Documents/go-work/code/go-keylane/race_test.go): Launches parallel goroutine loops submitting jobs, retrieving stats, stopping queues, and awaiting futures simultaneously under intense concurrency checks.

---

## 3. Running the Test Suite

Execute standard verification commands from the project root:

### All Core and Public Tests
```bash
go test -v ./...
```

### Targeted Categories
```bash
go test -v -run 'TestShardRouting|TestLaneQuota|TestNoisyLane|TestNoisyKey' ./...
```

### Lifecycles and Value Jobs
```bash
go test -v -run 'TestSubmitValue|TestAwait|TestGracefulShutdown|TestStop' ./...
```

### Complete Race Verification
```bash
go test -race -count=5 ./...
```
