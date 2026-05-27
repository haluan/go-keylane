# Public API inventory (KL-1801)

Exported symbol review for v0.8 / v1.0. **Categories** match [api-stability.md](api-stability.md).

Machine-readable export lists (559 + 38 + 5 + 3 symbols) live in:

```text
internal/apicheck/testdata/exports_keylane.txt
internal/apicheck/testdata/exports_httpkeylane.txt
internal/apicheck/testdata/exports_metrics_prometheus.txt
internal/apicheck/testdata/exports_tracing_otel.txt
```

Guard: `go test ./internal/apicheck/...`

---

## `github.com/haluan/go-keylane`

### Core queue lifecycle — Stable Candidate

| Symbol | Category | Notes |
|--------|----------|-------|
| `Queue`, `New`, `Start`, `Stop`, `Submit`, `TrySubmit` | Stable Candidate | Primary entry points |
| `Config`, `Config.Validate` | Stable Candidate | Zero values disable optional subsystems |
| `Job`, `Lane`, `WithDrain` | Stable Candidate | Fire-and-forget execution |
| `ErrQueueAlreadyStarted`, `ErrQueueNotStarted`, `ErrNilQueue` | Stable Candidate | Lifecycle errors |

### SubmitValue / Future — Stable Candidate

| Symbol | Category | Notes |
|--------|----------|-------|
| `SubmitValue`, `ValueJob` | Stable Candidate | Typed async work + result |
| `Future`, `Future.Await`, `Future.Done` | Stable Candidate | Single completion semantics |
| `FailureFromFuture`, `BudgetFromFuture`, `RetryTraceFromFuture` | Stable Candidate | Observability helpers |

### Request runtime (v0.3) — Stable Candidate

| Symbol | Category | Notes |
|--------|----------|-------|
| `SubmitRequest`, `Request`, `RequestMeta` | Stable Candidate | Single-handler requests |
| `RequestHooks`, `RequestObservation` | Stable Candidate | Hook contract |
| `Transport`, `RequestOutcome` | Stable Candidate | Low-cardinality labels |

### Admission / overload — Stable Candidate

| Symbol | Category | Notes |
|--------|----------|-------|
| `AdmissionConfig`, `CheckAdmission`, `AdmissionResult` | Stable Candidate | Lane admission |
| `OverloadConfig`, `CheckOverload` | Stable Candidate | Overload policy |
| `PerKeyAdmissionConfig`, `CheckPerKeyAdmission` | Stable Candidate | v0.5 hot key mitigation |
| `AdmissionRejectedError` | Stable Candidate | Use `errors.Is` |

### Failure / retry / deadline (v0.6) — Stable Candidate

| Symbol | Category | Notes |
|--------|----------|-------|
| `Failure`, `FailureKind`, `ClassifyFailure` | Stable Candidate | Classification |
| `RetryableFailure`, `PermanentFailure`, `CancelledFailure`, … | Stable Candidate | Constructors |
| `RetryPolicy`, `RunWithRetry`, `DecideRetry` | Stable Candidate | Bounded in-worker retry |
| `Idempotency`, `IdempotencyPolicy` | Stable Candidate | Retry safety |
| `RetrySuppressionPolicy` | Stable Candidate | Pressure-aware retry gate |
| `DeadlineBudget`, `DeadlineBudgetSnapshot` | Stable Candidate | Caller deadline visibility |

### Stats / DebugSnapshot — Stable Candidate

| Symbol | Category | Notes |
|--------|----------|-------|
| `Stats`, `StatsGCPressure` | Stable Candidate | Pull metrics |
| `DebugSnapshot`, `DebugSnapshotVersion` | Stable Candidate | Incident diagnostics |
| `Pressure`, `PressureSummary` | Stable Candidate | Queue pressure |
| `ScaleSignal`, `HotKeyConfig`, `ShardPressure` | Stable Candidate | v0.5 autoscaling signals |

### Adaptive quota — Deprecated / Stable mix

| Symbol | Category | Notes |
|--------|----------|-------|
| `AdaptiveDebugSnapshot`, `AdaptiveQuotaPolicy` | Stable Candidate | Preferred diagnostics |
| `AdaptiveQuotaSnapshot`, `AdaptiveControllerSnapshot` | Deprecated Candidate | Use `AdaptiveDebugSnapshot` |
| `Queue.AdaptiveQuotaSnapshot` | Deprecated Candidate | Method wrapper |

### Request pipeline (v0.7 KL-1701) — Experimental

| Symbol | Category | Notes |
|--------|----------|-------|
| `SubmitPipeline`, `Pipeline`, `PipelineStage` | Experimental | Multi-stage orchestration |
| `StageMeta`, `StageName`, `StageValidate`, … | Stable Candidate | Low-cardinality stage names |
| `StageFailure`, `AsStageFailure` | Experimental | Failure attribution |
| `ErrEmptyPipelineStages`, `ErrNilPipelineComplete` | Experimental | Validation errors |

### Stage execution context (KL-1702) — Experimental

| Symbol | Category | Notes |
|--------|----------|-------|
| `StageExecutionContext`, `StageExecutionFromContext` | Experimental | Context metadata |
| `StageMetaFromContext`, `RequestMetaFromExecution` | Experimental | Helpers |
| `ContextWithStageExecution` | Experimental | Attached per stage |

### Continuations (KL-1703) — Experimental

| Symbol | Category | Notes |
|--------|----------|-------|
| `Continuation`, `ContinuationCompleter` | Experimental | Opt-in via `ContinuationConfig.Enabled` |
| `ContinuationConfig`, `NewContinuation` | Experimental | `MaxPending` default applied at `New` when enabled |
| `ContinuationConfig.CompletionRetention` | Experimental | Reserved; unused |
| `ContinuationHooks`, `ContinuationObservation` | Experimental | Hook ordering: yielded → completed → resumed |
| `ErrContinuationDisabled`, `ErrContinuationRegistryFull` | Experimental | Capacity errors |

### Backend coordination (KL-1704) — Experimental

| Symbol | Category | Notes |
|--------|----------|-------|
| `AcquireBackend`, `WithBackend`, `BackendLease` | Experimental | In-process admission |
| `BackendResourceConfig`, `BackendLanePolicy` | Experimental | Zero value disables |
| `BackendAdmissionReject` | Experimental | Supported admission mode |
| `BackendAdmissionWait` | Remove Before v1.0 | Constant exists; `New` rejects config |
| `BackendLane`, `BackendResourceName` | Stable Candidate | Low-cardinality identifiers |
| `BackendOperationFromStage` | Experimental | Stage context integration |
| `BackendResourceHooks`, `BackendAdmissionDecision` | Experimental | Observability |

### Pool pressure adapters (KL-1705) — Experimental

| Symbol | Category | Notes |
|--------|----------|-------|
| `BackendPressureProvider`, `BackendPressureSnapshot` | Experimental | Observational; no auto-reject |
| `SQLDBPressureAdapter`, `APIClientPressureAdapter` | Experimental | Built-in adapters |
| `Queue.BackendPressure` | Experimental | Collects provider snapshots |

### Observability hooks — Stable Candidate

| Symbol | Category | Notes |
|--------|----------|-------|
| `ObservabilityConfig`, `DefaultObservabilityConfig` | Stable Candidate | Hooks recover panics |
| `RequestHooks`, `StageObservation` | Stable Candidate | See [pipeline-observability.md](pipeline-observability.md) |

### Package metadata

| Symbol | Category | Notes |
|--------|----------|-------|
| `Version` | Experimental | Development version string |

All other exported symbols in `exports_keylane.txt` inherit the category of their subsystem unless listed in testdata-only helpers; new exports require explicit classification in PR review.

---

## `github.com/haluan/go-keylane/httpkeylane`

| Symbol | Category | Notes |
|--------|----------|-------|
| `Middleware` | Stable Candidate | Primary HTTP integration |
| `Config`, `KeyFunc`, `LaneFunc`, `ErrorHandler` | Stable Candidate | Request routing |
| `AdmissionConfig`, `OverloadConfig` | Stable Candidate | Maps to core admission |
| `TransportHTTP` | Stable Candidate | Metric label transport |
| `CookieKey`, `HeaderKey`, `QueryKey`, … | Stable Candidate | Key extraction helpers |
| `DefaultErrorHandler`, `DegradeHandler` | Stable Candidate | Status mapping |

Full list: `exports_httpkeylane.txt` (38 symbols).

---

## `github.com/haluan/go-keylane/metrics/prometheus`

| Symbol | Category | Notes |
|--------|----------|-------|
| `Collector`, `NewCollector` | Stable Candidate | Optional adapter; label contract in [metrics.md](metrics.md) |
| `Register`, `MustRegister` | Stable Candidate | Prometheus registration helpers |

Full list: `exports_metrics_prometheus.txt` (5 symbols).

---

## `github.com/haluan/go-keylane/tracing/otel`

| Symbol | Category | Notes |
|--------|----------|-------|
| `HookAdapter`, `NewHookAdapter` | Stable Candidate | Optional OTel bridge |

Full list: `exports_tracing_otel.txt` (3 symbols).

---

## Review checklist (maintainers)

- [ ] `go test ./internal/apicheck/...` passes
- [ ] New exports classified in this document
- [ ] Experimental APIs have Go doc marker
- [ ] Deprecated APIs have `Deprecated:` line
- [ ] [migration/v0.7-to-v0.8.md](migration/v0.7-to-v0.8.md) updated for breaking changes
- [ ] Examples use Stable Candidate entry points where possible
