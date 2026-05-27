# Pipeline benchmarks (v0.7)

Part of [v0.7.0 — Advanced Request Pipeline & Backend Resource Coordination](../v0.7-advanced-request-pipeline-and-resource-coordination.md).

Measures SubmitPipeline overhead, hook cost, continuation yield/resume, and backend coordination. All benchmarks use in-memory fakes (no network or real database).

---

## Commands

```bash
# SubmitPipeline-centric benchmarks
go test -bench='BenchmarkPipeline' -benchmem .

# Continuation yield/resume (continuation_bench_test.go)
go test -bench='BenchmarkPipelineContinuation' -benchmem .

# Backend acquire/release and pressure
go test -bench='BenchmarkBackend' -benchmem .
```

Optional Makefile target: `make bench-pipeline` (if configured).

---

## Benchmark catalog

### `pipeline_bench_test.go`

| Benchmark | What it measures |
|-----------|------------------|
| `BenchmarkPipelineSingleStage` | One noop stage, hooks default (enabled in `DefaultObservabilityConfig`) |
| `BenchmarkPipelineMultiStage` | Three sync stages |
| `BenchmarkPipelineStageHooksDisabled` | `EnableHooks: false` |
| `BenchmarkPipelineStageHooksEnabled` | All stage hooks wired to no-op |
| `BenchmarkPipelineFullStack` | Multi-stage + backend coordination + static pressure provider |
| `BenchmarkPipelineBackendAcquireRelease` | Single stage `WithBackend` acquire/release via `SubmitPipeline` |
| `BenchmarkPipelineBackendSaturatedReject` | In-pipeline saturated second acquire (stage failure path) |
| `BenchmarkPipelineBackendPressureCollection` | `Queue.BackendPressure` with static provider |
| `BenchmarkPipelineSQLPressureAdapter` | `SQLDBPressureAdapter` mapping (wrapper; same as `BenchmarkSQLDBPressureAdapter`) |
| `BenchmarkPipelineAPIPressureAdapter` | `APIClientPressureAdapter` mapping (wrapper; same as `BenchmarkAPIClientPressureAdapter`) |
| `BenchmarkPipelineRetryDisabled` | Single stage, default retry off |
| `BenchmarkPipelineRetryEnabled` | Same pipeline with `Retry.Enabled` and one retryable failure |

### `continuation_bench_test.go`

| Benchmark | What it measures |
|-----------|------------------|
| `BenchmarkPipelineContinuationYieldResume` | Yield, complete, await |
| `BenchmarkPipelineContinuationLateCompletion` | Late completer after resolution |
| `BenchmarkPipelineContinuationObservabilityOverhead` | Continuation hooks on vs minimal |

### `backend_bench_test.go` / `backend_pressure_bench_test.go`

| Benchmark | What it measures |
|-----------|------------------|
| `BenchmarkBackendAcquireRelease*` | Direct `AcquireBackend` / `Release` (no pipeline) |
| `BenchmarkSQLDBPressureAdapter` | Alias target for `BenchmarkPipelineSQLPressureAdapter` |
| `BenchmarkAPIClientPressureAdapter` | Alias target for `BenchmarkPipelineAPIPressureAdapter` |
| `BenchmarkBackendPressureSnapshotCollection` | Queue collects all providers |

`make bench-pipeline` runs `BenchmarkPipeline*` (all pipeline and continuation benchmarks, including SQL/API wrappers) and `BenchmarkBackend*`. Use `make bench-continuation` to run only the continuation subset in isolation.

---

## Interpretation

- Compare `HooksDisabled` vs `HooksEnabled` for observability tax on the hot path.
- `BenchmarkPipelineFullStack` is an upper bound for a stage doing backend work with diagnostics enabled.
- Allocation counts appear with `-benchmem`; use for regression tracking, not absolute SLOs.

---

## Related

- [pipeline-observability.md](../pipeline-observability.md)
- [pipeline-testing.md](../pipeline-testing.md)
- [benchmarks.md](../benchmarks.md)
