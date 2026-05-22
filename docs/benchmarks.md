# Go-Keylane Performance Benchmarks

This document describes the commands and methodology for running and analyzing benchmarks to track memory allocation and performance.

> [!WARNING]
> **Environment-Specific Warning**:
> Benchmark metrics (`ns/op`, `B/op`, `allocs/op`) are highly environment-sensitive and depend on your machine's CPU architecture, active system workloads, operating system, and Go compiler version. 
> These numbers represent localized hardware-specific baseline performance — they are **not product guarantees** and should not be used as hard assertions in CI/CD pipeline pass/fail gates.

---

## 1. Running Benchmarks

### Full Benchmark Suite
To run all benchmarks (including public and internal core packages) showing memory allocation statistics:
```bash
go test -bench=. -benchmem ./...
```

### Public API Benchmarks
To only run public enqueuing and value submission benchmarks:
```bash
go test -bench='BenchmarkSubmit' -benchmem .
```

### GC Pressure Snapshot Guardrails
To benchmark `StatsGCPressure()` snapshot cost and verify Submit enqueue path allocations were not regressed by in-flight accounting (which runs only in `processShard`, not on Submit):
```bash
make bench-gc-pressure
# or:
go test -bench='BenchmarkStatsGCPressure|BenchmarkSubmit' -benchmem .
go test -bench='BenchmarkStatsGCPressure|BenchmarkProcessShard' -benchmem ./internal/core
```

Use `benchstat` to compare before and after KL-1201/KL-1202/KL-1203/KL-1204:
- **Submit path:** `BenchmarkSubmitSingleLane` and `BenchmarkSubmitHotPathAllocGuardrail` (queue never started; enqueue-only).
- **processShard path:** `BenchmarkProcessShardSingleLane` and `BenchmarkProcessShardSingleLaneInflightGuardrail` (same workload; documents shardInflight/laneInflight atomic cost).
- **Queue wait (KL-1203):** `BenchmarkRecordGCPressureQueueWait` (atomic update path); `BenchmarkStatsGCPressure` includes queue-wait fields. Successful enqueue stamps `AcceptedAt` (`time.Time`) on the queued job copy after `push`.
- **Run duration (KL-1204):** `BenchmarkRecordGCPressureRunDuration`; `StatsGCPressure` version `"4"` adds `Run` fields. Compare `BenchmarkProcessShardNoHooks`, `BenchmarkProcessShardNilHooks`, and `BenchmarkProcessShardLightweightHooks` for hook overhead.

### Core Scheduler Benchmarks
To run the internal lane queue and process shard loop benchmarks:
```bash
go test -bench='BenchmarkProcessShard|BenchmarkLaneQueue' -benchmem ./internal/core
```

---

## 2. Before/After Optimization Workflow

We recommend using the standard Go `benchstat` tool to compare performance before and after code changes.

### Step 1: Install benchstat
```bash
go install golang.org/x/perf/cmd/benchstat@latest
```

### Step 2: Record Baseline
1. Checkout your stable branch/commit.
2. Run benchmarks and save the output:
   ```bash
   go test -bench='BenchmarkProcessShard' -benchmem ./internal/core -count=10 > old.txt
   ```

### Step 3: Record New Changes
1. Apply your optimizations.
2. Run benchmarks again:
   ```bash
   go test -bench='BenchmarkProcessShard' -benchmem ./internal/core -count=10 > new.txt
   ```

### Step 4: Compare Performance
Analyze the improvements using `benchstat`:
```bash
benchstat old.txt new.txt
```
This will print a statistical comparison showing changes in execution duration (`ns/op`), bytes allocated (`B/op`), and allocs per operation (`allocs/op`).

---

## 3. sync.Pool Optimization Baseline Comparison

To verify the effectiveness of `sync.Pool` slice recycling on the scheduler's hot paths, comparison benchmarks can be run directly using the internal test suite:

```bash
go test -bench='Pool' -benchmem ./...
```

### 3.1. Internal Worker Loop comparison (`processShard`)
Under identical mock scheduler conditions, reusing the batch collections via `sync.Pool` eliminates worker scheduling allocations:

| Benchmark Target | Ops | Duration | Memory | Allocations |
| :--- | :--- | :--- | :--- | :--- |
| `BenchmarkProcessShardWithoutPool` | `5,266,510` | `220.9 ns/op` | `320 B/op` | `1 allocs/op` |
| `BenchmarkProcessShardWithPool` | `6,269,278` | `191.5 ns/op` | **`0 B/op`** | **`0 allocs/op`** |

### 3.2. Public Submit Value comparison (`SubmitValue`)
Under high-frequency public enqueuing where user closures are passed to return values, enqueuing paths are kept stable:

| Benchmark Target | Ops | Duration | Memory | Allocations |
| :--- | :--- | :--- | :--- | :--- |
| `BenchmarkSubmitValueWithoutPool` | `6,145,743` | `172.9 ns/op` | `240 B/op` | `3 allocs/op` |
| `BenchmarkSubmitValueWithPool` | `6,816,633` | `177.7 ns/op` | `240 B/op` | `3 allocs/op` |

> [!NOTE]
> These metrics serve as baseline performance indicators rather than absolute runtime guarantees, as actual heap allocations depend heavily on runtime environment state, active workloads, and user closures.

