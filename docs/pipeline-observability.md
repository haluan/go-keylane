# Pipeline observability (v0.7)

Consolidates KL-1701 (pipeline stages) through KL-1705 (pool pressure) into one hook and snapshot model. The library does not register Prometheus counters directly; implement metrics in hooks. Canonical v0.7 metric names are in [metrics.md](metrics.md#v07-backend-resource-metrics-hook-adapters-kl-1704).

---

## Hook surfaces

| Area | Config path | Events |
|------|-------------|--------|
| Request lifecycle | `Hooks.Request` | `OnQueued`, `OnStarted`, `OnCompleted`, `OnRejected` |
| Pipeline stages | `Hooks.Request` | `OnStageStarted`, `OnStageCompleted`, `OnStageFailed` |
| Continuations | `Hooks.Request.Continuation` | `OnContinuationYielded`, `OnContinuationCompleted`, `OnContinuationResumed`, `OnContinuationFailed`, `OnContinuationCancelled`, `OnContinuationLate` |
| Backend coordination | `Hooks.Backend` | `OnBackendAdmission`, `OnBackendReleased`, `OnBackendPressure` |

Enable with `Observability.EnableHooks = true`. Hooks recover from panics and must not block workers.

---

## Lifecycle ordering

### Synchronous multi-stage pipeline

```text
OnQueued
OnStarted
OnStageStarted (validate)
OnStageCompleted (validate)
OnStageStarted (db_read)
OnBackendAdmission (accepted)
OnBackendReleased
OnStageCompleted (db_read)
OnStageStarted (response)
OnStageCompleted (response)
OnCompleted
```

### Continuation stage

```text
OnStageStarted
OnContinuationYielded
(worker released)
OnContinuationCompleted   # completer accepted; resolution goroutine
OnContinuationResumed     # resume shard job starts (may follow queue wait)
OnStageStarted (next stage …)
OnStageCompleted / OnStageFailed (remaining stages)
OnCompleted
```

`OnContinuationCompleted` fires when `Complete`/`Fail`/`Cancel` is accepted, before the resume job runs. `OnContinuationResumed` fires inside the enqueued resume job. The yielding stage does not emit `OnStageCompleted` at yield time; resume runs from the next stage index.

### Backend admission rejected

```text
OnStageStarted
OnBackendAdmission (rejected)
OnStageFailed
OnRejected or OnCompleted with failure
```

---

## Low-cardinality metadata

Safe for metric labels (bounded sets per KL-1706):

- `lane`, `stage`, `operation`, `transport`
- `outcome`, `failure_kind`
- `backend_resource`, `backend_lane`, `backend_reason`

Present on observations for debugging and snapshots but **must not** be used as metric labels:

- `stage_index`, `stage_count`, `attempt`, `shard_id`
- `pressure`, `saturated`, `deadline_remaining` (bucket in logs only)
- `Key`, `RequestID`

- `Key`, `RequestID`
- Raw error strings (`Err` on continuation observations)
- URLs, SQL, tenant/user identifiers

Backend hooks use `KeyHash` only (not raw routing keys). See [backend-pressure-adapters.md](backend-pressure-adapters.md).

---

## Event types

- **`StageObservation`** — `Stage`, `Operation`, `StageDuration`, `Execution` (stage context), `FailureKind`
- **`ContinuationObservation`** — `Stage`, `YieldedFor`, `ResumeQueueWait`, `Outcome`, `FailureKind`
- **`BackendAdmissionDecision`** / **`BackendReleaseEvent`** — backend resource/lane, stage metadata when stage context is present
- **`BackendPressureEvent`** — external pool snapshot (`Pressure`, `InUse`, `Capacity`, `Saturated`)

---

## DebugSnapshot (pull diagnostics)

When `EnableDebugSnapshot` is true:

- `Continuation` — pending count, capacity, late completions
- `BackendResources` — in-process KL-1704 inflight per lane
- `BackendPressure` — KL-1705 external pool snapshots

See [debug-snapshot.md](debug-snapshot.md).

---

## Troubleshooting

| Symptom | Signals |
|---------|---------|
| Pipeline stall | Rising `Continuation.Pending`; shard queue depth in `DebugSnapshot` |
| Backend saturation | `BackendResources` lane `Saturated`; `BackendPressure.Saturated`; admission `reason=saturated` |
| Continuation leak | `Continuation.Pending` not returning to zero; goroutine growth (see [pipeline-testing.md](pipeline-testing.md)) |
| Queue vs pool pressure | `Pressure` (scheduler) vs `BackendPressure` (downstream pool) — different layers |

---

## Recommended production alerts (v0.7)

Pseudocode; tune thresholds per deployment. Combine signals — do not alert on a single counter in isolation. Use the canonical label names from [metrics.md](metrics.md) (`backend_resource`, `backend_lane`, `backend_reason`).

```text
ALERT KeylaneContinuationPendingHigh
  IF debug_snapshot_continuation_pending > 0 FOR 10m
     AND rate(keylane_pipeline_continuation_yielded_total[10m])
         > rate(keylane_pipeline_continuation_resumed_total[10m]) * 1.1

ALERT KeylaneContinuationLateCompletions
  IF rate(keylane_pipeline_continuation_late_total[5m]) > 0

ALERT KeylaneBackendLaneSaturated
  IF keylane_backend_saturated{backend_resource="primary-db",backend_lane="db_read"} == 1 FOR 5m
     OR rate(keylane_backend_admission_rejected_total{backend_resource="primary-db",backend_lane="db_read",backend_reason="saturated"}[5m]) > threshold

ALERT KeylaneBackendInFlightStuck
  IF keylane_backend_inflight{backend_resource="primary-db",backend_lane="db_write"}
       / keylane_backend_capacity{backend_resource="primary-db",backend_lane="db_write"} > 0.95 FOR 5m
     AND rate(keylane_backend_admission_rejected_total{backend_resource="primary-db",backend_lane="db_write"}[5m]) > 0

ALERT KeylanePipelineStageFailureSpike
  IF rate(keylane_pipeline_stage_failed_total[5m]) by (stage, failure_kind) > baseline
```

For pull-only diagnostics without Prometheus, poll `Queue.DebugSnapshot()` and alert on `Continuation.Pending`, `BackendResources` lane `Saturated`, and `LateCompletions`.

---

## Related

- [request-pipeline.md](request-pipeline.md)
- [request-observability.md](request-observability.md)
- [continuations.md](continuations.md)
- [backend-resource-coordination.md](backend-resource-coordination.md)
- [pipeline-testing.md](pipeline-testing.md)
