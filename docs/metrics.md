# Metrics (v0.5+)

Part of [v0.7.0 — Advanced Request Pipeline & Backend Resource Coordination](v0.7-advanced-request-pipeline-and-resource-coordination.md) for pipeline, continuation, and backend metric families.

Platform-neutral reference for go-keylane observability metrics. Metric names are stable across adapters; this document describes semantics and label safety.

For Prometheus wiring, see [metrics-prometheus.md](metrics-prometheus.md). For autoscaling interpretation, see [autoscaling-signals.md](autoscaling-signals.md).

go-keylane does **not** implement an autoscaler. Metrics are inputs for Prometheus, OpenTelemetry, custom control loops, HPA external metrics, KEDA, or other platforms.

---

## Core metrics (all versions)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `keylane_jobs_submitted_total` | Counter | `scheduler`, `lane` | Jobs submitted |
| `keylane_jobs_completed_total` | Counter | `scheduler`, `lane` | Jobs completed |
| `keylane_jobs_failed_total` | Counter | `scheduler`, `lane` | Jobs failed |
| `keylane_queue_full_total` | Counter | `scheduler`, `lane` | Queue full rejections |
| `keylane_lane_depth` | Gauge | `scheduler`, `lane` | Queued jobs per lane |
| `keylane_shard_depth` | Gauge | `scheduler`, `shard_id` | Queued jobs per shard |
| `keylane_inflight_jobs` | Gauge | `scheduler`, `shard_id`, `lane` | Running jobs |
| `keylane_queue_wait_seconds` | Summary | `scheduler`, `lane` | Cumulative queue wait stats |
| `keylane_run_duration_seconds` | Summary | `scheduler`, `lane` | Cumulative run duration stats |
| `keylane_pressure_ratio` | Gauge | `scheduler` | Global depth ratio |

---

## v0.7.0 pipeline stage metrics (adapter hooks)

The library does not register these counters directly; implement them in `OnStageStarted` / `OnStageCompleted` / `OnStageFailed` hooks from [request-observability.md](request-observability.md).

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `keylane_pipeline_stage_started_total` | Counter | `transport`, `operation`, `lane`, `stage` | Stage execution started |
| `keylane_pipeline_stage_completed_total` | Counter | `transport`, `operation`, `lane`, `stage`, `outcome` | Stage finished without error |
| `keylane_pipeline_stage_failed_total` | Counter | `transport`, `operation`, `lane`, `stage`, `failure_kind` | Stage returned classified error |
| `keylane_pipeline_stage_duration_seconds` | Histogram | `transport`, `operation`, `lane`, `stage` | `StageObservation.StageDuration` |

Do not add `key`, `request_id`, URL path, tenant id, or raw error strings as labels.

See [pipeline-observability.md](pipeline-observability.md) for hook lifecycle ordering.

---

## v0.7.0 continuation metrics (hook adapters)

Implement in `Hooks.Request.Continuation` callbacks. See [continuations.md](continuations.md).

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `keylane_pipeline_continuation_yielded_total` | Counter | `transport`, `operation`, `lane`, `stage` | Stage returned a continuation |
| `keylane_pipeline_continuation_resumed_total` | Counter | `transport`, `operation`, `lane`, `stage` | Resume job started after completion |
| `keylane_pipeline_continuation_completed_total` | Counter | `transport`, `operation`, `lane`, `stage` | Continuation resolved successfully |
| `keylane_pipeline_continuation_failed_total` | Counter | `transport`, `operation`, `lane`, `stage`, `failure_kind` | `Completer.Fail` |
| `keylane_pipeline_continuation_cancelled_total` | Counter | `transport`, `operation`, `lane`, `stage` | Cancel or deadline while pending |
| `keylane_pipeline_continuation_late_total` | Counter | `transport`, `operation`, `lane`, `stage` | Resolution after continuation already terminal |

---

## v0.7.0 backend resource metrics (hook adapters)

v0.7.0 provides in-process backend admission and `DebugSnapshot.BackendResources` pressure. The library does **not** register Prometheus counters for backend coordination; implement them in `Hooks.Backend.OnBackendAdmission` and `OnBackendReleased` from [request-observability.md](request-observability.md). See [backend-resource-coordination.md](backend-resource-coordination.md).

v0.7.0 pool pressure metrics are implemented via `Hooks.Backend.OnBackendPressure`; see [backend-pressure-adapters.md](backend-pressure-adapters.md).

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `keylane_backend_admission_total` | Counter | `backend_resource`, `backend_lane`, `stage`, `backend_reason` | Admission attempts (`BackendAdmissionDecision.Reason`) |
| `keylane_backend_released_total` | Counter | `backend_resource`, `backend_lane`, `stage` | Releases (`OnBackendReleased`) |
| `keylane_backend_inflight` | Gauge | `backend_resource`, `backend_lane` | `InFlight` after admission or release |
| `keylane_backend_capacity` | Gauge | `backend_resource`, `backend_lane` | Configured `MaxInFlight` |
| `keylane_backend_held_duration_seconds` | Histogram | `backend_resource`, `backend_lane`, `stage` | `BackendReleaseEvent.HeldFor` |

Optional adapter decomposition of `keylane_backend_admission_total`:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `keylane_backend_admission_accepted_total` | Counter | `backend_resource`, `backend_lane`, `stage` | Accepted acquisitions |
| `keylane_backend_admission_rejected_total` | Counter | `backend_resource`, `backend_lane`, `stage`, `backend_reason` | Rejected acquisitions (`saturated`, `unknown_resource`, …) |

Use **backend lane** (`db_read`, `external_api`, …), not request `lane`, for downstream classification. `backend_resource` must be a small static set (`primary-db`, `wallet-api`). Do not label with SQL text, URLs, or request IDs.

For pull diagnostics without a metrics adapter, use `Queue.DebugSnapshot().BackendResources` when `BackendResources.Enabled` and `EnableDebugSnapshot` are true.

### v0.7.0 backend pool pressure metrics (v0.7)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `keylane_backend_pressure_ratio` | Gauge | `backend_resource`, `backend_lane` | `BackendPressureSnapshot.Pressure` (0..1) |
| `keylane_backend_in_use` | Gauge | `backend_resource`, `backend_lane` | External pool in-use count |
| `keylane_backend_capacity` | Gauge | `backend_resource`, `backend_lane` | External pool capacity |
| `keylane_backend_wait_total` | Counter | `backend_resource`, `backend_lane` | Cumulative wait events |
| `keylane_backend_wait_duration_seconds` | Counter | `backend_resource`, `backend_lane` | Cumulative wait time |
| `keylane_backend_saturated` | Gauge | `backend_resource`, `backend_lane` | 1 when pool saturated |

Use `Queue.BackendPressure` or `DebugSnapshot.BackendPressure` as the data source.

**Autoscaling relevance:** v0.5 `keylane_scale_pressure_ratio` reflects **keylane queue/shard pressure** (worker-bound, hot keys). v0.7.0 `keylane_backend_pressure_ratio` reflects **downstream pool saturation** (DB/API). Use both when diagnosing tail latency — flat CPU/memory does not imply healthy downstream pools. See [v0.7.0 overview](v0.7-advanced-request-pipeline-and-resource-coordination.md) and [autoscaling-signals.md](autoscaling-signals.md).

### v0.7.0 pipeline production alerts

Recommended continuation, backend, and stage alerts (pseudocode) are in [pipeline-observability.md](pipeline-observability.md#recommended-production-alerts-v07).

---

## v0.5 scale and pressure metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `keylane_queue_depth_ratio` | Gauge | `scheduler` | Total queued / total capacity |
| `keylane_queue_wait_max_seconds` | Gauge | `scheduler` | Max observed queue wait (aggregate) |
| `keylane_worker_busy_ratio` | Gauge | `scheduler` | In-flight jobs / worker count |
| `keylane_scale_pressure_ratio` | Gauge | `scheduler` | Composite scale pressure ratio |
| `keylane_scale_recommended` | Gauge | `scheduler`, `reason`, `scope` | 0/1 recommendation |
| `keylane_admission_rejected_total` | Counter | `scheduler`, `lane` | Cumulative rejects (`lane="_all"` for aggregate) |
| `keylane_admission_shed_total` | Counter | `scheduler`, `lane` | Cumulative sheds |
| `keylane_admission_throttled_total` | Counter | `scheduler` | Per-key throttle decisions |
| `keylane_hot_shard_count` | Gauge | `scheduler` | Hot shard count |
| `keylane_hot_key_candidate_count` | Gauge | `scheduler` | Bounded hot key candidate count |
| `keylane_hot_key_pressure_ratio` | Gauge | `scheduler` | Max localized hot key depth ratio |
| `keylane_hot_key_rejected_total` | Counter | `scheduler` | Hot key admission rejections |
| `keylane_localized_hot_key_ratio` | Gauge | `scheduler` | Localized key dominance ratio |
| `keylane_shard_pressure_ratio` | Gauge | `scheduler` | Global shard pressure composite |
| `keylane_shard_queue_depth` | Gauge | `scheduler`, `shard_id` | Spec alias of `keylane_shard_depth` |
| `keylane_per_key_admission_decisions_total` | Counter | `scheduler`, `action`, `reason` | Per-key decisions |
| `keylane_per_key_mitigation_actions_total` | Counter | `scheduler`, `action`, `reason` | Alias for per-key mitigation totals |

Ratios are unitless (0..1+). Time values are seconds.

---

## Safe labels

Use low-cardinality, static labels:

```text
scheduler
lane
shard_id
action
reason
scope
backend_resource
backend_lane
backend_reason
stage
```

`scheduler` is your deployment name (one value per process). `lane` names must be a small static set configured at queue creation. `backend_lane` and `backend_resource` are separate low-cardinality label sets for v0.7.0 backend coordination.

---

## Labels that must not be exposed by default

High-cardinality or sensitive labels can damage metrics systems and leak data:

```text
raw_key
customer_id
tenant_name
email
path_with_dynamic_id
authorization_token
session_id
```

**Never** use job `Key`, request IDs, tenant IDs, or unbounded route segments as metric labels.

If you need per-tenant visibility, aggregate in your application layer or use bounded key hashes in logs — not Prometheus labels.

---

## Example alerts (pseudocode)

Safe alerts combine context, not single counters alone:

```text
ALERT KeylaneQueueDepthHigh
  IF keylane_queue_depth_ratio > 0.80 FOR 5m

ALERT KeylaneScaleRecommendedGlobal
  IF keylane_scale_recommended{scope="global"} == 1 FOR 2m

ALERT KeylaneWorkerSaturated
  IF keylane_worker_busy_ratio > 0.85 FOR 5m

ALERT KeylaneAdmissionRejectRate
  IF rate(keylane_admission_rejected_total[5m]) > threshold
```

### Anti-patterns

- Alerting on `hot_key_candidate_count` alone without `PressureSummary` / `ScaleSignal` context
- Using raw key labels for per-tenant dashboards
- Treating `scale_recommended=1` with `scope=hot_key` as a scale-out signal

---

## OpenTelemetry naming (reference)

| Prometheus | OTEL-style name |
|------------|-----------------|
| `keylane_scale_pressure_ratio` | `keylane.scale.pressure_ratio` |
| `keylane_scale_recommended` | `keylane.scale.recommended` |
| `keylane_queue_depth_ratio` | `keylane.queue.depth_ratio` |
| `keylane_admission_rejected_total` | `keylane.admission.rejected` |
| `keylane_admission_shed_total` | `keylane.admission.shed` |
| `keylane_admission_throttled_total` | `keylane.admission.throttled` |

See [tracing-opentelemetry.md](tracing-opentelemetry.md) for span integration.

---

## Related docs

- [metrics-prometheus.md](metrics-prometheus.md) — adapter quick start
- [backend-resource-coordination.md](backend-resource-coordination.md) — backend lanes, leases, hooks (v0.7)
- [autoscaling-signals.md](autoscaling-signals.md) — interpreting scale metrics
- [runbooks/hot-key-and-scale-pressure.md](runbooks/hot-key-and-scale-pressure.md) — operator alerts
