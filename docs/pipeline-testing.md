# Pipeline testing (v0.7)

Part of [v0.7.0 — Advanced Request Pipeline & Backend Resource Coordination](v0.7-advanced-request-pipeline-and-resource-coordination.md).

How the v0.7.0 pipeline and backend coordination features are covered by tests and how to run them locally.

---

## Quick commands

```bash
# Pipeline-focused unit and integration tests
go test -race -run 'Pipeline|SubmitPipeline' .

# Continuation stack
go test -race -run 'Continuation' .

# Backend coordination and pressure
go test -race -run 'Backend' .

# Full race suite (CI)
go test -race ./...

# Benchmarks
go test -bench='BenchmarkPipeline' -benchmem .
go test -bench='BenchmarkBackend' -benchmem .
```

See [benchmarks/pipeline.md](benchmarks/pipeline.md) for benchmark catalog.

---

## Test matrix

| Category | Primary files |
|----------|----------------|
| Pipeline stages (order, validation, future) | `pipeline_test.go` |
| Stage hooks | `pipeline_observability_test.go` |
| Stage execution context | `pipeline_context_test.go` |
| Stage failure / deadline / panic recovery | `pipeline_failure_test.go`, `pipeline_stage_panic_test.go` |
| Retry + pipeline | `pipeline_retry_test.go`, `pipeline_retry_context_test.go` |
| Continuation lifecycle | `continuation_test.go`, `continuation_cancellation_test.go`, `continuation_deadline_test.go` |
| Continuation races | `continuation_race_test.go` |
| Continuation hooks | `continuation_observability_test.go`, `continuation_test.go` (disabled hooks) |
| Backend sync pipeline | `backend_pipeline_test.go`, `backend_continuation_test.go` |
| Backend admission | `backend_resource_test.go` |
| Backend hooks | `backend_observability_test.go` |
| Pool pressure adapters | `backend_pressure_test.go` (URL-like/overlong label rejection), `backend_sql_adapter_test.go`, `backend_api_adapter_test.go` |
| Cross-feature integration | `pipeline_integration_test.go` |
| Stress / goroutine leaks | `pipeline_stress_test.go` |
| DebugSnapshot v0.7.0 | `debug_snapshot_pipeline_test.go`, `backend_snapshot_test.go` (lane `Queued`, pressure `Resource`/`Lane`/`Pressure` ratio) |

---

## Stress tests

`pipeline_stress_test.go` uses `eventuallyNoGoroutineGrowth` and backend inflight checks to detect leaks after:

- Continuation cancellation while yielded
- Backend admission rejection (goroutines + `BackendLaneDBWrite` `InFlight == 0`)
- Continuation registry at capacity
- Stage failure after saturated backend reject (`TestStressBackendInFlightZeroAfterStageFailure`)
- Submitted pipeline stage panic with `WithBackend` (`TestStressBackendInFlightZeroAfterStagePanic`; `runPipelineStages` recovers panic)
- Continuation pending drain after deadline (`TestStressContinuationPendingZeroAfterDeadline`; also `continuation_deadline_test.go`)

These complement race tests in `continuation_race_test.go` and `backend_resource_test.go`.

---

## Related

- [pipeline-observability.md](pipeline-observability.md)
- [continuations.md](continuations.md)
