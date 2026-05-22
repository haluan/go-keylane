# KL-1206 Production benchmark suite

Repeatable `go test -bench` coverage for Keylane v0.2 with stable `BenchmarkKeylane*`, `BenchmarkFairness*`, and `BenchmarkGCPressure*` names.

**Principle:**

- Keylane does **not** avoid Go GC pauses.
- Keylane helps shape GC pressure caused by uncontrolled concurrency, goroutine explosion, allocation bursts, and request pile-up.

Bounded concurrency and pooling are mechanisms that contribute to shaping that pressure; they are not a guarantee against runtime GC pauses.

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

# Full repo (includes KL-1201–1205 guardrails)
go test -bench=. -benchmem ./...

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

## Benchmark map

| Location | Names | Role |
|----------|-------|------|
| `./benchmarks` | `BenchmarkKeylane*`, `BenchmarkFairness*`, `BenchmarkGCPressure*` | Production-oriented API + scenario suite |
| `./benchmarks` | `BenchmarkKeylaneSubmit*Observability`, `BenchmarkKeylaneDebugSnapshotOnDemand` | KL-1207 visibility vs low-allocation |
| `.` (root) | `BenchmarkSubmit*`, `BenchmarkSubmitHotPathAllocGuardrail`, `BenchmarkStatsGCPressure`, `BenchmarkDebugSnapshot`, `BenchmarkGCPressureLowAllocationMode` | KL-1201–1205 guardrails; root low-alloc bench is **sync.Pool** only |
| `./internal/core` | `BenchmarkProcessShard*`, `BenchmarkKeylaneProcessShard*`, `BenchmarkKeylaneWorker*Observability` | Scheduler hot path |

## Groups

1. **Submit** — `submit_benchmark_test.go` (`BenchmarkKeylaneSubmit*`, observability matrix).
2. **Future / Await** — `future_benchmark_test.go`.
3. **Shard** — `./internal/core -bench='ProcessShard|KeylaneProcessShard'`.
4. **Fairness** — `fairness_benchmark_test.go` + in-package global FIFO baseline (`global_fifo_baseline_test.go`).
5. **GC pressure** — `gc_pressure_benchmark_test.go`.
6. **Observability modes (KL-1207)** — `observability_benchmark_test.go` (`BenchmarkKeylaneSubmit*Observability`, `BenchmarkKeylaneDebugSnapshotOnDemand`). Worker compare: `./internal/core -bench=BenchmarkKeylaneWorker.*Observability`. Root `BenchmarkGCPressureLowAllocationMode` compares **sync.Pool** (`DisablePooling`), not observability config.

## Observability matrix

| Benchmark | Config |
|-----------|--------|
| `BenchmarkKeylaneSubmitObservabilityOff` | Default |
| `BenchmarkKeylaneSubmitStatsEnabled` | Same enqueue path as default (stats snapshot: `BenchmarkStatsGCPressure` in root) |
| `BenchmarkKeylaneSubmitTimingEnabled` | `TrackQueueWait: true` |
| `BenchmarkKeylaneSubmitNoopHookEnabled` | `OnJobTiming` no-op |
| `BenchmarkGCPressureObservabilityEnabled` vs `BenchmarkGCPressureObservabilityMinimal` | Full hooks vs minimal burst |

Snapshot cost remains `BenchmarkStatsGCPressure` / `BenchmarkDebugSnapshot` in the root package.
