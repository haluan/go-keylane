# Autoscaling Signals (KL-1504)

Backpressure protects the process from overload, but it can also keep CPU and memory flat while requests are waiting, throttled, or rejected. Use Keylane **scale signals** to detect demand that is being held back by the scheduler.

## Quick API

```go
sig := q.ScaleSignal()
if sig.DiagnosticsEnabled && sig.Recommended && sig.Scope == keylane.ScaleScopeGlobal {
    // sustained global pressure — consider scaling capacity
}

snap := q.DebugSnapshot()
_ = snap.ScaleSignal
```

`ScaleSignal()` is the lightweight read path for frequent polling (for example Prometheus scrapes). `DebugSnapshot()` includes the same signal plus full shard/per-key context when you need a one-shot diagnostic bundle.

`ScaleSignal` is **not** a scaling controller. It does not recommend replica counts, talk to Kubernetes, or trigger autoscaling. External tools consume the signal.

## Configuration

```go
AutoscalingSignal: keylane.DefaultAutoscalingSignalConfig(),
```

Zero value disables scale signal calculation. `Queue.Pressure()` and admission control continue to work.

| Field | Role |
|-------|------|
| `Enabled` | Master switch for scale signal |
| `Window` | Observation window for rate deltas and consecutive unhealthy counting |
| `ConsecutiveWindows` | Unhealthy windows required before `Recommended=true` |
| `QueueDepthRatioThreshold` | Queue depth ratio trigger |
| `QueueWaitMaxThreshold` | Max observed queue wait trigger (not a histogram P95) |
| `AdmissionRejectRateThreshold` | Reject rate over window |
| `AdmissionShedRateThreshold` | Shed rate over window |
| `WorkerBusyRatioThreshold` | In-flight / worker saturation trigger |
| `HotShardRatioThreshold` | Fraction of shards hot |
| `ManyHotShardsThreshold` | Absolute hot shard count trigger |
| `LocalizedHotKeyRatioThreshold` | Hot key dominance guardrail |

## ScaleSignal fields

| Field | Meaning |
|-------|---------|
| `DiagnosticsEnabled` | `false` when autoscaling signal is disabled; `true` when enabled |
| `Recommended` | `true` after sustained global pressure (consecutive unhealthy windows) |
| `PressureRatio` | Composite max of normalized pressure components |
| `Reason` | Primary trigger (see below) |
| `Scope` | `global`, `shard`, `hot_key`, etc. |
| `QueueDepthRatio` | Total queued / total capacity |
| `QueueWaitMax` | Max observed queue wait across lanes (not histogram P95) |
| `AdmissionRejectedRate` | Rejects / submitted over window |
| `AdmissionShedRate` | Sheds / submitted over window |
| `AdmissionThrottledRate` | Per-key throttle events / submitted over window |
| `WorkerBusyRatio` | In-flight jobs / worker count |
| `HotShardCount`, `HotShardRatio` | From KL-1503 aggregates when enabled |
| `HotKeyCandidateCount`, `LocalizedHotKeyRatio` | From KL-1501/KL-1503 |

## ScaleReason values

| Reason | Meaning |
|--------|---------|
| `none` | Healthy when `DiagnosticsEnabled=true`; also returned when disabled |
| `queue_depth_high` | Queue depth ratio above threshold |
| `queue_wait_high` | Max queue wait above threshold |
| `admission_reject_high` | Reject rate above threshold |
| `admission_shed_high` | Shed rate above threshold |
| `worker_saturated` | Workers busy above threshold |
| `many_hot_shards` | Many shards hot simultaneously |
| `distributed_pressure` | KL-1503 global `distributed` or `worker_bound` class |
| `localized_hot_key` | One key dominates — **not** a global scale signal |
| `insufficient_data` | Zero capacity or no submissions yet |

When `DiagnosticsEnabled=false`, treat `Reason=none` as “signal disabled”, not healthy.

## ScaleScope values

| Scope | Meaning |
|-------|---------|
| `none` | No pressure scope |
| `global` | Scale-out may help (distributed backlog) |
| `shard` | Single-shard pressure (one hot shard, non-distributed) |
| `hot_key` | Per-key mitigation preferred over global scale |
| `lane` | Reserved for future finer-grained signals |
| `unknown` | Insufficient data |

## Localized hot key guardrail

When `LocalizedHotKeyRatio >= LocalizedHotKeyRatioThreshold` and few shards are hot, the signal sets:

```text
Recommended = false
Reason      = localized_hot_key
Scope       = hot_key
```

The consecutive-unhealthy window counter is **not** reset when localized pressure is detected alongside distributed pressure. Do **not** scale the whole service on a single-key storm. Use [per-key-admission.md](per-key-admission.md) first.

## Three backlog shapes

| Pattern | Signals | Scale-out helps? |
|---------|---------|------------------|
| **Localized hot key** | `Reason=localized_hot_key`, low `HotShardCount` | Often **no** |
| **Hot shard** | High `HotShardCount` with mixed keys | Sometimes |
| **Distributed backlog** | `Reason=distributed_pressure` or `many_hot_shards` | **Yes** |

See [shard-pressure-balancing.md](shard-pressure-balancing.md) and [pressure-diagnostics.md](pressure-diagnostics.md).

## Prometheus metrics

The optional [metrics/prometheus](../metrics/prometheus/) collector exposes low-cardinality metrics. Scale-specific outputs keep the `keylane_scale_*` prefix; runtime components use the flat namespace per KL-1504 spec.

| Metric | Type | Description |
|--------|------|-------------|
| `keylane_scale_pressure_ratio` | Gauge | Composite pressure ratio |
| `keylane_scale_recommended{reason,scope}` | Gauge | 0/1 recommendation |
| `keylane_queue_depth_ratio` | Gauge | Depth component |
| `keylane_queue_wait_max_seconds` | Gauge | Max observed wait (scheduler aggregate) |
| `keylane_admission_rejected_total{lane="_all"}` | Counter | Cumulative rejects (scheduler aggregate) |
| `keylane_admission_shed_total{lane="_all"}` | Counter | Cumulative sheds (scheduler aggregate) |
| `keylane_admission_throttled_total` | Counter | Cumulative per-key throttle decisions |
| `keylane_worker_busy_ratio` | Gauge | Worker saturation |
| `keylane_hot_shard_count` | Gauge | Hot shard count |
| `keylane_hot_key_candidate_count` | Gauge | Bounded hot key count |
| `keylane_localized_hot_key_ratio` | Gauge | Localized key ratio |

Per-lane breakdowns for `admission_rejected_total` and `admission_shed_total` use real lane names. Scheduler aggregates use the bounded sentinel label `lane="_all"`.

**Never** use raw keys, tenant IDs, or unbounded route labels on autoscaling metrics. Allowed labels on `keylane_scale_recommended` are `scheduler`, `reason`, and `scope` only.

## OpenTelemetry naming (reference)

| Prometheus | OTEL-style name |
|------------|-----------------|
| `keylane_scale_pressure_ratio` | `keylane.scale.pressure_ratio` |
| `keylane_scale_recommended` | `keylane.scale.recommended` |
| `keylane_queue_depth_ratio` | `keylane.queue.depth_ratio` |
| `keylane_admission_rejected_total` | `keylane.admission.rejected` (counter) |
| `keylane_admission_shed_total` | `keylane.admission.shed` (counter) |
| `keylane_admission_throttled_total` | `keylane.admission.throttled` (counter) |

Units: ratios are unitless 0..1+; time values are seconds.

## Example alert (pseudocode)

```text
ALERT KeylaneScaleRecommended
  IF keylane_scale_recommended{scope="global"} == 1
     FOR 2m
  ANNOTATE reason from keylane_scale_recommended{reason=...}
```

## Source files

| Spec name | Actual file |
|-----------|-------------|
| `autoscaling.go` | [`autoscaling.go`](../autoscaling.go) |
| `internal/scale_signal.go` | [`internal/core/scale_signal.go`](../internal/core/scale_signal.go) |

## Related

- [pressure-diagnostics.md](pressure-diagnostics.md) — KL-1503 shard pressure
- [per-key-admission.md](per-key-admission.md) — KL-1502 mitigation
- [v0.5-runtime-signals.md](v0.5-runtime-signals.md) — milestone overview
- [production-tuning.md](production-tuning.md) — worker and queue sizing
