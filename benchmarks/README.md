# v0.8.0 production benchmark suite

Repeatable `go test -bench` coverage for Keylane v0.8.0 with stable `BenchmarkKeylane*`, `BenchmarkFairness*`, and `BenchmarkGCPressure*` names in this package, plus documented guardrails in the root module and `internal/core`.

See [docs/performance-regression.md](../docs/performance-regression.md) for baseline capture, `benchstat` comparison, and PR review thresholds.

**Principle:**

- Keylane does **not** avoid Go GC pauses.
- Keylane helps shape GC pressure caused by uncontrolled concurrency, goroutine explosion, allocation bursts, and request pile-up.

Bounded concurrency and pooling are mechanisms that contribute to shaping that pressure; they are not a guarantee against runtime GC pauses.

## v0.8.0 benchmark catalog (KL-1805)

| Area | Package | Bench regex / names | Comparison tier |
|------|---------|---------------------|-----------------|
| Scheduler submit | `./benchmarks` | `BenchmarkKeylaneSubmit*` | **Stable** |
| Submit edge paths | `./benchmarks` | `BenchmarkKeylaneSubmitContextCanceled`, `DeadlineExpired`, `QueueFull`, `DuringShutdown` | Stable (edge) |
| Future / Await | `./benchmarks` | `BenchmarkKeylaneSubmitValue*`, `BenchmarkKeylaneAwait*` | **Stable** |
| Lane fairness | `./benchmarks` | `BenchmarkFairnessKeylane*`, `BenchmarkFairnessNoisyLaneVsSensitiveLane` | Exploratory (p50/p95/p99) |
| Shard / hot key | `./benchmarks` | `BenchmarkKeylaneSubmitOneHotKey`, `BenchmarkGCPressureOneHotKeyBurst` | **Stable** / exploratory |
| GC pressure burst | `./benchmarks` | `BenchmarkGCPressureSubmitBurst` | **Stable** |
| Observability overhead | `./benchmarks` | `BenchmarkKeylaneSubmit*Observability`, `BenchmarkGCPressureObservability*` | Exploratory |
| Continuation opt-in | `./benchmarks` | `BenchmarkKeylaneContinuationDisabledOverhead` | Exploratory |
| Submit alloc guardrail | `.` (root) | `BenchmarkSubmitHotPathAllocGuardrail` | **Stable** |
| Pipeline overhead | `.` (root) | `BenchmarkPipelineSingleStage`, `BenchmarkPipelineStageHooks*` | **Stable** / exploratory |
| Backend resources | `.` (root) | `BenchmarkBackendAcquireRelease*` | **Stable** |
| Continuation lifecycle | `.` (root) | `BenchmarkPipelineContinuation*` | Exploratory |
| Scheduler hot path | `./internal/core` | `BenchmarkProcessShard*`, `BenchmarkKeylaneProcessShard*` | Exploratory |
| Full tree | `./...` | `go test ./... -bench .` | Exploratory only |

**Stable** benchmarks are listed in [baselines/v0.8.0.json](baselines/v0.8.0.json) and suitable for release-to-release `benchstat` on the same machine. **Exploratory** benches (fairness distributions, continuation storms, saturated backend reject) inform design but are not single-number SLAs.

## Baseline capture

```bash
# From repo root
make bench-baseline
# or:
./benchmarks/scripts/run-baseline.sh

# Compare two captured runs (requires benchstat: go install golang.org/x/perf/cmd/benchstat@latest)
make bench-compare OLD=/tmp/go-keylane-bench-v0.7.0.txt NEW=/tmp/go-keylane-bench-v0.8.0.txt
```

Artifacts: [baselines/v0.8.0.md](baselines/v0.8.0.md), [baselines/v0.8.0.json](baselines/v0.8.0.json).

## Workload shapes

| Constant | Value | Used by |
|----------|-------|---------|
| `benchShardCount` | 8 | fairness, GC burst, many-key submit |
| `benchWorkerCount` | 4 | fairness FIFO baseline, standard config |
| `benchQueueSizePerLane` | 64 | standard config |
| `benchFairnessJobCount` | 400 | fairness scenarios per iteration |
| Single-lane config | 16 shards, 4 workers, 10k capacity | submit/future microbenches |

Lane quotas (standard config): `default=2`, `noisy=4`, `sensitive=1`, `laneA/B/C=2`.

## Commands

```bash
# Production suite (this package)
make bench-production

# Or directly:
go test -bench='Keylane|Fairness|GCPressure' -benchmem ./benchmarks

# Root guardrails (alloc + pipeline + backend)
go test . -run '^$' -bench='BenchmarkSubmitHotPathAllocGuardrail|BenchmarkPipelineSingleStage|BenchmarkBackendAcquireReleaseDisabled' -benchmem

# Full repo (exploratory)
go test ./... -run '^$' -bench . -benchmem

# Repeat for benchstat
go test -bench='Keylane|Fairness|GCPressure' -benchmem -count=5 ./benchmarks

# GC trace (optional)
GODEBUG=gctrace=1 go test -bench=GCPressure -benchmem ./benchmarks

# CPU / heap profiles
go test -bench=BenchmarkKeylaneSubmit -cpuprofile=cpu.prof -memprofile=mem.prof ./benchmarks
```

## Custom metrics (fairness)

Fairness benchmarks report distribution metrics via `b.ReportMetric` (no hard thresholds in CI):

| Metric suffix | Meaning |
|---------------|---------|
| `wait_p50_ns` / `wait_p95_ns` / `wait_p99_ns` | Queue wait (job start − submit) |
| `latency_p50_ns` / … | End-to-end (job done − submit) |
| `completed_jobs/op` | Completed count per benchmark op |
| `failed_jobs/op` | Failed jobs per op |
| `rejected_jobs/op` | `ErrQueueFull` (or FIFO equivalent) per op |

Compare Keylane vs global FIFO with `benchstat` on the same machine; numbers are environment-specific, not product SLAs.

## Benchmark map (all packages)

| Location | Names | Role |
|----------|-------|------|
| `./benchmarks` | `BenchmarkKeylane*`, `BenchmarkFairness*`, `BenchmarkGCPressure*` | Production-oriented API + scenario suite |
| `./benchmarks` | `BenchmarkKeylaneSubmit*Observability`, `BenchmarkKeylaneDebugSnapshotOnDemand` | Visibility vs low-allocation |
| `.` (root) | `BenchmarkSubmit*`, `BenchmarkSubmitHotPathAllocGuardrail`, `BenchmarkStatsGCPressure`, `BenchmarkPipeline*`, `BenchmarkBackend*`, `BenchmarkPipelineContinuation*` | Guardrails + pipeline/continuation/backend |
| `./internal/core` | `BenchmarkProcessShard*`, `BenchmarkKeylaneProcessShard*`, `BenchmarkKeylaneWorker*Observability` | Scheduler hot path |

## Groups

1. **Submit** — `submit_benchmark_test.go` (`BenchmarkKeylaneSubmit*`, observability matrix, cancel/deadline/shutdown).
2. **Future / Await** — `future_benchmark_test.go`.
3. **Shard** — `./internal/core -bench='ProcessShard|KeylaneProcessShard'`.
4. **Fairness** — `fairness_benchmark_test.go` + in-package global FIFO baseline (`global_fifo_baseline_test.go`).
5. **GC pressure** — `gc_pressure_benchmark_test.go`.
6. **Continuation opt-in** — `continuation_benchmark_test.go`.
7. **Observability modes** — `observability_benchmark_test.go`. Worker compare: `./internal/core -bench=BenchmarkKeylaneWorker.*Observability`. Root `BenchmarkGCPressureLowAllocationMode` compares **sync.Pool** (`DisablePooling`), not observability config.

## Observability matrix

| Benchmark | Config |
|-----------|--------|
| `BenchmarkKeylaneSubmitObservabilityOff` | Default |
| `BenchmarkKeylaneSubmitStatsEnabled` | Same enqueue path as default (stats snapshot: `BenchmarkStatsGCPressure` in root) |
| `BenchmarkKeylaneSubmitTimingEnabled` | `TrackQueueWait: true` |
| `BenchmarkKeylaneSubmitNoopHookEnabled` | `OnJobTiming` no-op |
| `BenchmarkGCPressureObservabilityEnabled` vs `BenchmarkGCPressureObservabilityMinimal` | Full hooks vs minimal burst |

Snapshot cost remains `BenchmarkStatsGCPressure` / `BenchmarkDebugSnapshot` in the root package.
