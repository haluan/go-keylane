# Runtime lifecycle hardening (KL-1806)

This document is the v0.8 contract for panic isolation, shutdown, goroutine lifecycle, race safety, and late completion behavior.

Related: [phase-5-backpressure-and-shutdown.md](phase-5-backpressure-and-shutdown.md), [production-hardening.md](production-hardening.md), [observability-contract.md](observability-contract.md).

## Principles

- Runtime-owned goroutines must not die silently because user code panicked.
- User hook failure must not corrupt scheduler state.
- Shutdown has deterministic observable behavior.
- Accepted work has a clear completion, cancellation, or rejection path.
- Awaiters must not block forever after shutdown.
- Continuations tolerate late completion safely.
- Backend leases are released on panic, cancellation, and shutdown paths.
- Race detector cleanliness is part of the production contract (`go test -race ./...`).

## Panic boundaries

| Boundary | Behavior |
|----------|----------|
| `Job.Run` / `ValueJob.Run` (worker) | Panic recovered → `ErrJobPanicked`; worker continues; lane `Panicked` counter incremented |
| Pipeline `Stage.Run` / `RunContinuation` | Panic recovered → permanent stage failure (`stage panic` message) |
| Observability hooks | `callHook` recovers; `HookPanicsRecovered()` increments |
| Backend pressure provider probe | Recovered → configuration error |
| Backend admission/release hooks | `callHook`; panic counted via `HookPanicsRecovered()` |
| Internal invariant panics | Not swallowed; fail tests |

### Job panic

```go
if errors.Is(err, keylane.ErrJobPanicked) { ... }
```

Classification: `FailurePanic` / `JobOutcomePanicked`. Job panics are **not** retried by default.

### Hook panic

Hooks must not block workers. A hook panic does not prevent required cleanup on the worker path. Use `keylane.HookPanicsRecovered()` for process-wide diagnostics.

## Stop and drain

`Queue.Stop(ctx, opts...)` transitions: running → stopping → stopped.

| Option | Semantics |
|--------|-----------|
| Default / `WithDrain(false)` | Reject new work; queued work may be canceled; workers exit without draining backlog |
| `WithDrain(true)` | Reject new work; allow queued jobs to run until empty or stop context expires |

After stop begins:

- `Submit`, `TrySubmit`, `SubmitValue` → `ErrStopped` (use `errors.Is`)
- `Stop` is idempotent (safe to call concurrently or repeatedly)

See [phase-5-backpressure-and-shutdown.md](phase-5-backpressure-and-shutdown.md) for the state machine diagram.

## Await after shutdown

- `SubmitValue` on a stopped queue returns `ErrStopped` and the future completes with `ErrStopped` on `Await`.
- In-flight work stopped without drain may leave futures canceled or failed per request context; awaiters must not block indefinitely.
- Leak tests: `TestAwaitAfterShutdownNoGoroutineLeak`, `TestSubmitValueStoppedQueue`.

## Continuation late completion

After cancellation, timeout, or shutdown, a late `ContinuationCompleter.Complete` must:

- Not panic
- Not double-complete the future
- Return `false` when the continuation slot is already resolved
- Increment `DebugSnapshot().Continuation.LateCompletions` when counted as late

Tests: `continuation_cancellation_test.go`, `continuation_deadline_test.go`, `TestPipelineContinuationLateCompleteAfterStop`.

## Backend resource leases

- `lease.Release()` is **idempotent** (double release is safe). See `TestBackendDoubleReleaseSafe`.
- `WithBackend` uses `defer lease.Release()`; panic inside `fn` still releases the lease.
- After stage panic with `WithBackend`, in-flight counts return to zero (`TestStressBackendInFlightZeroAfterStagePanic`, `TestBackendLeaseReleasedAfterJobPanic`).

## Diagnostics (low cardinality)

| Signal | Source |
|--------|--------|
| Hook panics | `HookPanicsRecovered()` |
| Job panics | `StatsGCPressure().Lanes[].Counters.Panicked` |
| Continuation late | `DebugSnapshot().Continuation.LateCompletions` |

Do not export panic messages or raw keys as metric labels (KL-1804).

## Race detector

Run before release on the same packages as CI:

```bash
go test -race ./...
cd httpkeylane && go test -race ./...
cd tracing/otel && go test -race ./...
```

Representative tests: `race_test.go`, `continuation_race_test.go`, `v05_runtime_race_test.go`, `lifecycle_leak_test.go`.

## Goroutine leak tests

Tests capture `runtime.NumGoroutine()` before a scenario, stop the queue, then use `eventuallyNoGoroutineGrowth` with a tolerance (typically 8–12) to allow unrelated runtime goroutines.

Coverage includes: idle start/stop, queue-full then stop, await-after-shutdown, pipeline continuation cancel, backend reject, v0.5 hot-key burst.

## Known limitations (pre-v1.0)

- No distributed shutdown or cross-pod drain
- No persistent workflow recovery
- No exactly-once execution guarantee
- Job panic messages are not exported as metric labels

## Test index

| Area | Files |
|------|-------|
| Job panic | `job_panic_test.go`, `internal/core/job_panic_test.go` |
| Shutdown | `stop_test.go`, `shutdown_test.go` |
| Leak | `lifecycle_leak_test.go`, `pipeline_stress_test.go`, `v05_goroutine_leak_test.go` |
| Race | `race_test.go`, `continuation_race_test.go` |
| Late / backend | `lifecycle_late_test.go`, `continuation_cancellation_test.go` |
| Hook panic | `backend_hook_panic_test.go`, `observability_contract_test.go` |
