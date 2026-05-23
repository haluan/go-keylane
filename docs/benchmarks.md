# Go-Keylane Performance Benchmarks

This document describes the commands and methodology for running and analyzing benchmarks to track memory allocation and performance.

> [!IMPORTANT]
> **GC pressure shaping, not GC pause elimination**
>
> - `keylane` does **not** avoid Go GC pauses.
> - `keylane` helps shape GC pressure caused by uncontrolled concurrency, goroutine explosion, allocation bursts, and request pile-up.
> - The benchmark suite measures whether keylane reduces concurrency-driven allocation bursts and queue unfairness. **It does not prove that Go GC pauses are eliminated.**

See [gc-pressure-shaping.md](gc-pressure-shaping.md) and [benchmarks/README.md](../benchmarks/README.md).

> [!WARNING]
> **Environment-Specific Warning**:
> Benchmark metrics (`ns/op`, `B/op`, `allocs/op`) are highly environment-sensitive and depend on your machine's CPU architecture, active system workloads, operating system, and Go compiler version. 
> These numbers represent localized hardware-specific baseline performance — they are **not product guarantees** and should not be used as hard assertions in CI/CD pipeline pass/fail gates.

---

## 1. Running Benchmarks

### Production benchmark suite (v02.)
Scenario and API benchmarks with stable names live under [`benchmarks/README.md`](../benchmarks/README.md).

```bash
make bench-production
# or:
go test -bench='Keylane|Fairness|GCPressure' -benchmem ./benchmarks
```

Regex: `Keylane|Fairness|GCPressure`. Fairness benches emit `wait_p50_ns`, `latency_p95_ns`, `completed_jobs/op`, etc. Use `benchstat` with `-count=5` for trends — no hard thresholds in CI.

### What to inspect

| Metric | Meaning |
|--------|---------|
| `ns/op` | Per-operation wall time |
| `B/op`, `allocs/op` | Heap allocation per benchmark iteration |
| `wait_p50_ns`, `wait_p95_ns` (fairness) | Queue wait distribution under load |
| `latency_p50_ns`, `latency_p95_ns` (fairness) | End-to-end latency under mixed workloads |
| `completed_jobs/op` | Throughput under fairness scenarios |

Scenarios include many keys vs one hot key, noisy lane vs latency-sensitive lane, global FIFO comparison, and GC pressure burst benches (`BenchmarkGCPressure*`).

### Optional GC trace

```bash
GODEBUG=gctrace=1 go test -bench=GCPressure -benchmem ./benchmarks
```

Output is environment-specific; use for local investigation only, not as a product guarantee.

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

Use `benchstat` to compare before and after v0.2:
- **Submit path:** `BenchmarkSubmitSingleLane` and `BenchmarkSubmitHotPathAllocGuardrail` (queue never started; enqueue-only).
- **processShard path:** `BenchmarkProcessShardSingleLane` and `BenchmarkProcessShardSingleLaneInflightGuardrail` (same workload; documents shardInflight/laneInflight atomic cost).
- **Queue wait:** `BenchmarkRecordGCPressureQueueWait` (atomic update path); `BenchmarkStatsGCPressure` includes queue-wait fields. Successful enqueue stamps `AcceptedAt` (`time.Time`) on the queued job copy after `push`.
- **Run duration:** `BenchmarkRecordGCPressureRunDuration`; `StatsGCPressure` version `"4"` adds `Run` fields. Compare `BenchmarkProcessShardNoHooks`, `BenchmarkProcessShardNilHooks`, and `BenchmarkProcessShardLightweightHooks` for hook overhead.
- **Debug snapshot:** `BenchmarkDebugSnapshot` and `BenchmarkPressure` measure caller-triggered diagnostic snapshot cost (not worker hot path).

```bash
go test -bench='BenchmarkDebugSnapshot|BenchmarkPressure' -benchmem .
```

### Overload Hot-Path Guardrails

```bash
go test -bench='BenchmarkEvaluateOverload|BenchmarkCheckOverload' -benchmem ./internal/core .
```

### Admission Hot-Path Guardrails

Two benchmarks verify that the successful-admit path (pressure below all thresholds, depth zero) allocates nothing:

- **`BenchmarkEvaluateAdmission`** (`./internal/core`) — tests `evaluateAdmission` in isolation; pure per-lane threshold comparison.
- **`BenchmarkEvaluateAdmissionForLane`** (`./internal/core`) — adds one atomic snapshot load (public `Scheduler` method).
- **`BenchmarkCheckAdmission`** (root `.`) — full public path including `Pressure()` and `LaneQueueDepth()`.

Expected result on the admit branch: **0 allocs/op** for all three benchmarks.

```bash
go test -bench='BenchmarkEvaluateAdmission|BenchmarkCheckAdmission' -benchmem ./internal/core .
```

### Core Scheduler Benchmarks
To run the internal lane queue and process shard loop benchmarks:
```bash
go test -bench='BenchmarkProcessShard|BenchmarkLaneQueue|BenchmarkKeylaneProcessShard' -benchmem ./internal/core
```

v0.2 adds `BenchmarkKeylaneProcessShardWithLaneQuota` and `BenchmarkKeylaneProcessShardRequeue` for unequal quotas and ReadyCh requeue paths.

### Low-allocation observability

Compare default vs low-allocation submit/worker overhead:

```bash
make bench-low-alloc
# or:
go test -bench='BenchmarkKeylaneSubmit.*Observability|BenchmarkKeylaneSubmitValue.*Observability' -benchmem ./benchmarks
go test -bench='BenchmarkKeylaneWorker.*Observability' -benchmem ./internal/core
```

See [production-tuning.md](production-tuning.md) for mode selection.

### Adaptive quota benchmarks

Compare fixed vs adaptive submit paths and diagnostic snapshot cost:

```bash
go test -bench='BenchmarkFixedQuota|BenchmarkAdaptiveQuota|BenchmarkSubmitWithAdaptiveQuota' -benchmem .
go test -bench='BenchmarkAdaptiveQuotaDecisionTick|BenchmarkAdaptiveQuotaWithOverloadSignals' -benchmem ./internal/core
```

| Benchmark | Purpose |
|-----------|---------|
| `BenchmarkSubmitWithAdaptiveQuotaDisabled` | Submit with controller off |
| `BenchmarkSubmitWithAdaptiveQuotaEnabled` | Submit with controller on (long eval interval) |
| `BenchmarkSubmitAdaptiveDisabled` / `BenchmarkSubmitAdaptiveEnabled` | KL-1405 spec aliases (delegate to the `WithAdaptiveQuota` names) |
| `BenchmarkFixedQuotaCriticalAndBackground` | Alternating critical/background submit, adaptive off |
| `BenchmarkAdaptiveQuotaCriticalAndBackground` | Same workload, adaptive on |
| `BenchmarkAdaptiveQuotaSnapshot` | `AdaptiveQuotaSnapshot()` read cost |
| `BenchmarkAdaptiveQuotaDecisionTick` | Pure evaluator tick (no scheduler) |
| `BenchmarkAdaptiveQuotaWithOverloadSignals` | Signal snapshot build + one eval tick |

See [adaptive-quota.md](adaptive-quota.md), [adaptive-tuning.md](adaptive-tuning.md), and [benchmarks/adaptive-quota.md](benchmarks/adaptive-quota.md).

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

