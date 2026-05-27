# Observability contract (v0.8)

This document defines production-safe observability expectations and **compatibility guarantees** for v0.8. It complements [config-validation.md](config-validation.md), [production-defaults.md](production-defaults.md), and KL-1804 contract tests.

Programmatic inventory: `keylane.StableMetricDescriptors()`, `keylane.ExperimentalMetricPatterns()`, `keylane.ForbiddenMetricLabelNames()`, and `otel.StableTraceAttributeKeys()`.

---

## Principles

- **Stable names** for stable signals scraped by official adapters.
- **Low-cardinality labels** by default (`lane`, `shard_id`, bounded `reason`/`action`).
- **Sensitive values redacted** in hook payloads unless `Observability.ExposeRawRequestIdentifiers` is true (`HotKey.ExposeRawKey` is separate, for debug snapshots).
- **Hook panic isolation** — user hooks run through `callHook`; panics do not crash workers.
- **Explicit opt-in** for hot-path timing, hooks, and verbose tracing.
- **Experimental** hook-adapter metrics are documented but not registered by core.

---

## Stability levels

| Level | Meaning |
|-------|---------|
| `stable` | Safe for dashboards and alerts within v0.8.x |
| `experimental` | May change before v1.0; implement in your metrics adapter |
| `internal` | Not part of the user-facing contract |

---

## Production defaults (KL-1803)

| Area | Production default |
|------|-------------------|
| Unset `Observability` | Resolves to `DefaultObservabilityConfig` at `New`; `ValidateConfig` warns with `KL_CONFIG_OBSERVABILITY_FULL_DEFAULTS_RESOLVED` |
| Hook payloads | `Key` and `RequestID` redacted by default; `KeyHash` set for correlation; opt in with `ExposeRawRequestIdentifiers` |
| `ProductionDefaults()` | Uses `LowAllocationObservabilityConfig()` — counters/stats on; hot-path hooks and timing off |
| Raw keys in metrics | Not exported by core or prometheus adapter |
| Request / idempotency keys as labels | Not exported |
| Debug snapshots | Pull API (`DebugSnapshot`); not invoked on every submit |

```go
cfg := keylane.ProductionDefaults()
```

See [production-defaults.md](production-defaults.md).

---

## Stable Prometheus metrics

Registered by [metrics/prometheus](../metrics/prometheus) when the collector is attached. Names are frozen for v0.8; no renames from v0.7.

| Metric | Labels | Description |
|--------|--------|-------------|
| `keylane_jobs_submitted_total` | `scheduler`, `lane` | Enqueue attempts |
| `keylane_jobs_completed_total` | `scheduler`, `lane` | Completed jobs |
| `keylane_jobs_failed_total` | `scheduler`, `lane` | Failed jobs |
| `keylane_queue_full_total` | `scheduler`, `lane` | Queue-full rejections |
| `keylane_admission_rejected_total` | `scheduler`, `lane` | Pressure admission rejects |
| `keylane_admission_shed_total` | `scheduler`, `lane` | Overload sheds |
| `keylane_lane_depth` | `scheduler`, `lane` | Queued jobs per lane |
| `keylane_shard_depth` | `scheduler`, `shard_id` | Queued jobs per shard |
| `keylane_shard_queue_depth` | `scheduler`, `shard_id` | Spec alias of `shard_depth` |
| `keylane_inflight_jobs` | `scheduler`, `shard_id`, `lane` | Running jobs |
| `keylane_queue_wait_seconds` | `scheduler`, `lane` | Cumulative queue wait (summary) |
| `keylane_run_duration_seconds` | `scheduler`, `lane` | Cumulative run duration (summary) |
| `keylane_pressure_ratio` | `scheduler` | Global depth ratio |
| `keylane_scale_pressure_ratio` | `scheduler` | Composite scale pressure |
| `keylane_scale_recommended` | `scheduler`, `reason`, `scope` | Scale-out signal |
| `keylane_queue_depth_ratio` | `scheduler` | Depth ratio component |
| `keylane_queue_wait_max_seconds` | `scheduler` | Max queue wait (aggregate) |
| `keylane_admission_throttled_total` | `scheduler` | Per-key throttle decisions |
| `keylane_worker_busy_ratio` | `scheduler` | In-flight / workers |
| `keylane_hot_shard_count` | `scheduler` | Hot shard count |
| `keylane_hot_key_candidate_count` | `scheduler` | Bounded hot key candidates |
| `keylane_localized_hot_key_ratio` | `scheduler` | Localized hot key ratio |
| `keylane_hot_key_pressure_ratio` | `scheduler` | Max localized hot key pressure |
| `keylane_hot_key_rejected_total` | `scheduler` | Hot key rejections |
| `keylane_per_key_admission_decisions_total` | `scheduler`, `action`, `reason` | Per-key decisions |
| `keylane_per_key_mitigation_actions_total` | `scheduler`, `action`, `reason` | Mitigation alias |
| `keylane_shard_pressure_ratio` | `scheduler` | Shard pressure composite |

Contract tests: `TestStableMetricDescriptors`, `TestMetricNamesFollowContract` (prometheus package).

---

## Experimental hook-adapter metrics

Not registered by core. Implement in `Hooks` callbacks; see [metrics.md](metrics.md).

Examples (stability `experimental`):

- Pipeline: `keylane_pipeline_stage_*`, `keylane_pipeline_continuation_*`
- Backend: `keylane_backend_admission_total`, `keylane_backend_pressure_ratio`, …

Use bounded `backend_resource`, `backend_lane`, `stage`, `transport`, `operation` — never raw paths or request IDs.

Inventory: `keylane.ExperimentalMetricPatterns()`.

---

## Label policy

### Allowed on stable adapter metrics

```text
scheduler
lane
shard_id
action
reason
scope
quantile   (Prometheus summary)
```

`AllowedDefaultMetricLabelNames()` returns this list.

### Forbidden by default

```text
key
raw_key
key_hash
request_id
idempotency_key
customer_id
user_id
tenant_id
backend_name
route_pattern
http_path
error_message
```

`ForbiddenMetricLabelNames()` returns this list. Contract tests assert stable descriptors never use forbidden names and prometheus gather does not expose raw job keys as label values.

### Validation warnings

| Code | When |
|------|------|
| `KL_CONFIG_RAW_KEY_EXPOSURE_ENABLED` | `HotKey.ExposeRawKey` — may expose raw keys in snapshots; must not label metrics/traces |
| `KL_CONFIG_HIGH_CARDINALITY_LABEL_RISK` | Hooks + debug snapshot + hot keys together |
| `KL_CONFIG_DEBUG_SNAPSHOT_HOT_PATH_HEAVY` | Debug snapshot + worker timing without low-allocation mode |
| `KL_CONFIG_OBSERVABILITY_FULL_DEFAULTS_RESOLVED` | Unset observability resolves to full defaults |

---

## Hook event contract

Hooks are **synchronous**, invoked on worker or cold paths after state updates (unless noted). Disabled when `Observability.EnableHooks` is false or the callback is nil. **Panics are recovered** via `callHook` and do not stop job execution.

| Hook | When | Stable fields (low cardinality) |
|------|------|--------------------------------|
| `OnJobTiming` | After job `Run` finishes | `ShardID`, `Lane`, durations, `Outcome` |
| `OnSlowJob` | Run duration ≥ threshold | Same as timing + `Threshold` |
| `Request.OnQueued` | Request accepted to queue | Redacted `RequestMeta` (`Key`/`RequestID` empty; use `KeyHash` on observations) |
| `Request.OnStarted` / `OnCompleted` / `OnRejected` | Request lifecycle | `RequestObservation` with `KeyHash`, redacted `Key`/`RequestID` |
| `Request.OnFailure` | Classified failure | `FailureEvent` |
| `Request.OnStageStarted` / `OnStageCompleted` / `OnStageFailed` | Pipeline stage | `StageObservation` (`StageMeta.Name` bounded) |
| `Request.Continuation.*` | Continuation lifecycle | `ContinuationObservation` |
| `OnPerKeyAdmissionDecision` | Per-key throttle/reject | `action`, `reason` (no raw key) |
| `OnOverloadPolicyDecision` | Overload policy | Policy enums |
| `OnAdaptiveQuotaDecision` | Adaptive quota | Decision metadata |
| `OnHotKeyCandidate` | Hot key candidate | `key_hash` unless `ExposeRawKey` |
| `OnShardPressureSummary` | After pressure summary | Aggregates only |
| `OnScaleSignal` | When diagnostics enabled | Signal snapshot |
| `Retry.OnRetryEvent` | Retry lifecycle | Attempt metadata |
| `Backend.*` | Backend coordination | Resource/lane enums |

**Contract:** Hooks must not block indefinitely. Do not perform heavy I/O or metrics export on the hot path without sampling.

Panic isolation is tested for job timing, pipeline stages, continuations, backend admission/release, backend pressure, retry hooks, OTEL timing hooks, and debug snapshot paths. Recovered panics increment `HookPanicsRecovered()`.

---

## HTTP adapter (`httpkeylane`)

`httpkeylane.ObserveFunc` receives `keylane.RequestObservation` snapshots from `ObservationForError(queue, meta, err)` on every HTTP request completion (including success). The same redaction rules apply: `Key` and `RequestID` are empty by default; `KeyHash` is set for correlation.

`HTTPRequestMetadata.Path` is the raw URL path and may be high-cardinality. Use it for logging only, not as a metric label. Prefer `RequestObservation.Operation` (from `OperationFunc`) for dashboards.

---

## OpenTelemetry attribute contract

Stable span attributes (package `tracing/otel`):

| Key | Use |
|-----|-----|
| `keylane.shard_id` | Shard id |
| `keylane.lane` | Lane name |
| `keylane.queue_wait_ms` | Queue wait (when enabled) |
| `keylane.run_ms` | Run duration (when enabled) |
| `keylane.queue_depth` | Pressure snapshot |
| `keylane.inflight_jobs` | In-flight count |
| `keylane.pressure_ratio` | Depth ratio |
| `keylane.slow_job_threshold_ms` | Slow job threshold |
| `keylane.outcome` | `completed`, `failed`, `canceled`, `panicked` |

Do not add `keylane.raw_key`, `keylane.request_id`, or unbounded path attributes. See [tracing-opentelemetry.md](tracing-opentelemetry.md).

---

## Debug snapshot stability

- **Version:** `DebugSnapshotVersion` (currently `"6"`). Bump is release-noted when JSON/programmatic consumers depend on fields.
- **Stable top-level fields:** `Version`, `GeneratedAt`, `ShardCount`, `LaneCount`, `WorkerCount`, depth/capacity totals, `Pressure`, `Shards`, `Lanes`.
- **Experimental / conditional:** `HotKeys` (raw key only with `ExposeRawKey`), `Continuation*`, `BackendResources`, `BackendPressure`.
- **Collection:** `Queue.DebugSnapshot()` is a pull API; not called per submit by default.

---

## Safe production dashboards

```text
ALERT KeylaneQueueDepthHigh
  IF keylane_queue_depth_ratio > 0.80 FOR 5m

ALERT KeylaneWorkerSaturated
  IF keylane_worker_busy_ratio > 0.85 FOR 5m
```

Avoid alerting on `hot_key_candidate_count` alone; combine with `PressureSummary` / `ScaleSignal`.

---

## Related

- [metrics.md](metrics.md) — full metric reference and hook adapter patterns
- [metrics-prometheus.md](metrics-prometheus.md) — collector quick start
- [production-hardening.md](production-hardening.md) — cross-cutting v0.8 governance
- [compatibility-rules.md](compatibility-rules.md)
- [releases/v0.8.0.md](releases/v0.8.0.md)
