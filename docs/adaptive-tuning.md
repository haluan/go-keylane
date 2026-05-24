# Adaptive Quota Tuning

## Overview

This guide covers practical tuning for the adaptive quota controller. For API reference, snapshots, and lifecycle, see [adaptive-quota.md](adaptive-quota.md).

> **Adaptive quota is not magic auto-tuning.** It is a bounded policy controller that can help shape pressure when configured correctly. Keylane helps shape concurrency and pressure; actual latency impact depends on workload, configuration, and downstream bottlenecks.

---

## Start with static quota

Run with fixed `LaneQuotas` first. Establish baseline queue wait, run duration, and `Pressure()` before enabling adaptive changes. Use `CurrentQuotaPolicy()` to record starting quotas and versions.

---

## Enable adaptive quota conservatively

1. Run with static quotas in production or load tests.
2. Enable observability hooks and `AdaptiveDebugSnapshot` in staging.
3. Enable adaptive quota in staging with conservative min/max bounds.
4. Use small `IncreaseStep` / `DecreaseStep` (default `1`).
5. Use `CooldownDuration` to avoid oscillation (default `5s`).
6. Compare fixed vs adaptive benchmarks — see [benchmarks/adaptive-quota.md](benchmarks/adaptive-quota.md).
7. Roll out gradually to production with alerts on quota change rate.

---

## Input signals

The controller evaluates these signals each tick:

| Signal | Source / use |
|--------|----------------|
| Global pressure ratio | `Pressure()` — increase/decrease hysteresis |
| Per-lane queue depth | Backlog per lane |
| Per-lane queue wait | P50/P95/P99 when timing enabled |
| Per-lane run duration | P50/P95/P99 when timing enabled |
| Queue full count | Blocks automatic increase on that lane |
| Rejection / shed count | Overload counters; localized decrease for background/best-effort |
| Quota change count | Per-lane adaptive stats; cooldown pacing |

Require `EnableQueueWaitTiming` / `EnableRunTiming` (default visibility mode) for wait/run percentiles in decision events.

---

## Pressure thresholds

| Field | Role |
|-------|------|
| `PressureLow` | Global pressure at or below this level allows **increase** evaluation (when other signals agree). |
| `PressureHigh` | Global pressure at or above this level allows **decrease** evaluation for all eligible lanes. |

Keep a gap between `PressureLow` and `PressureHigh` (defaults `0.60` / `0.85`) so the controller spends time in the **hold** band and avoids quota flapping.

| Field | Default (`DefaultAdaptiveQuotaConfig`) |
|-------|----------------------------------------|
| `PressureLow` | `0.60` |
| `PressureHigh` | `0.85` |
| `QueueWaitHigh` | `25ms` |
| `RunTimeHigh` | `250ms` |
| `EvaluationInterval` | `1s` |
| `WarmupDuration` | `5s` |
| `CooldownDuration` | `5s` |
| `IncreaseStep` / `DecreaseStep` | `1` |
| `MaxAdjustmentsPerTick` | `1` |

---

## Evaluation interval

`EvaluationInterval` controls how often the controller runs. Shorter intervals react faster but increase CPU and change churn. Start at `1s`; only decrease after validating stability.

---

## Increase/decrease step

Small steps (`1`) are recommended. Large steps cause visible quota jumps and harder-to-debug latency shifts.

---

## Warmup

`WarmupDuration` — no adjustments until the queue has collected meaningful samples (`warmup_active` reason). Default `5s`; extend for low-traffic services.

---

## Cooldown

`CooldownDuration` — per-lane minimum time between **successful** quota changes (`cooldown_active`). Increase cooldown when quotas oscillate; shorten only after load-test validation.

---

## Min/max quota

- **`MinQuota >= 1`** — avoid zero drain quotas
- **`MaxQuota`** — cap growth; size from peak sustainable throughput per lane
- Pin lanes with both `AllowIncrease` and `AllowDecrease` false

---

## Preventing oscillation

1. **Hysteresis band** — keep `PressureLow` well below `PressureHigh`
2. **Cooldown** — default `5s` between successful changes per lane
3. **`MaxAdjustmentsPerTick`** — default `1` limits changes per evaluation
4. **Fixed lanes** — pin quotas that must not move (billing, compliance)
5. **Shed best-effort first** — use [lane-priority.md](lane-priority.md) and [overload-policy.md](overload-policy.md) so adaptive quota is not the only relief valve

---

## Per-lane bounds

Each `LaneAdaptivePolicy` can set:

- `MinQuota` / `MaxQuota` — hard clamps after every decision
- `AllowIncrease` / `AllowDecrease` — merged with class defaults

Set **both** allow flags to `false` for a **fixed lane**.

---

## Class defaults

When a lane has no explicit adaptive entry, its class comes from the active **admission policy** lane class. Resolved policies are fixed at queue construction; admission policy updates after `New` do not refresh adaptive class until a future rebuild API.

| Class | Increase | Decrease |
|-------|----------|----------|
| critical | yes | no |
| normal | yes | yes |
| background / best-effort | no | yes |

Critical lanes are protected from automatic shrink; background and best-effort lanes are protected from automatic growth.

### Critical lanes under pressure

Even when a critical lane has `AllowIncrease: true`, the controller does not increase quotas during global overload. Hysteresis, queue-full counters, cooldown, and `MaxAdjustmentsPerTick` still apply.

---

## Localized overload decrease

Even when global pressure is below `PressureHigh`, **background** and **best-effort** lanes may decrease quota when overload shed, reject, or degrade counters on that lane are elevated.

---

## Queue-full guard

Automatic quota **increase** is blocked for a lane while that lane's queue-full counter is non-zero.

---

## Concurrent manual quota updates

Adaptive apply uses `QuotaVersion` from the evaluation snapshot. A concurrent manual update causes the adaptive change to be skipped (`quota_update_failed`). Prefer `UpdateLaneQuotaIfVersion` when coordinating.

---

## Recommended defaults

Use `DefaultAdaptiveQuotaConfig()` as a baseline (controller **disabled**). When enabling:

```go
config := keylane.AdaptiveQuotaConfig{
    Enabled:               true,
    EvaluationInterval:    time.Second,
    WarmupDuration:        5 * time.Second,
    CooldownDuration:      5 * time.Second,
    PressureHigh:          0.85,
    PressureLow:           0.60,
    QueueWaitHigh:         25 * time.Millisecond,
    IncreaseStep:          1,
    DecreaseStep:          1,
    MaxAdjustmentsPerTick: 1,
}
```

Normalize unset zero fields with `NormalizeAdaptiveQuotaConfig` after validation when building custom configs.

---

## Tuning examples

### Conservative production starter

```go
AdaptiveQuota: keylane.AdaptiveQuotaPolicy{
    Config: keylane.AdaptiveQuotaConfig{
        Enabled:            true,
        EvaluationInterval: 2 * time.Second,
        WarmupDuration:     30 * time.Second,
        CooldownDuration:   10 * time.Second,
        PressureLow:        0.50,
        PressureHigh:       0.90,
        IncreaseStep:       1,
        DecreaseStep:       1,
    },
    Lanes: []keylane.LaneAdaptivePolicy{
        {Lane: "api", Class: keylane.LaneNormal, MinQuota: 2, MaxQuota: 6},
        {Lane: "batch", Class: keylane.LaneBestEffort, MinQuota: 1, MaxQuota: 2,
            AllowIncrease: false, AllowDecrease: true},
    },
},
```

### Observing quota changes during tuning

```go
hooks := keylane.Hooks{
    OnQuotaChange: func(event keylane.QuotaChangeEvent) {
        // Export to logs, metrics, or tracing — keep fast and non-blocking.
    },
}
```

Do not perform slow network calls directly inside hooks.

---

## When to disable adaptive quota

Disable (`Enabled: false`) when:

- Quotas are owned entirely by an external control plane
- You need deterministic capacity for benchmarks or compliance tests
- Warmup/cooldown cannot be tuned safely for your traffic mix

Manual `UpdateLaneQuota` / `UpdateQuotaPolicy` remain available when adaptive quota is off.

---

## Policy and quota versions on events

`QuotaVersion` on each decision reflects the quota policy snapshot at evaluation time. `PolicyVersion` on `AdaptiveQuotaDecisionEvent` is the controller config generation (currently `1`). On `QuotaChangeEvent`, `PolicyVersion` is `0` when `source=manual` and the adaptive generation when `source=adaptive`. See [adaptive-observability.md](adaptive-observability.md).

---

## LaneAdaptiveStats.LastDecision

`LastDecision` in `AdaptiveDebugSnapshot` reflects the most recent adaptive evaluation reason, including hold. `AdaptiveHoldTotal` increments only when `EnableAdaptiveDecisionTracing` is on.

---

## Troubleshooting

| Symptom | Guidance |
|---------|----------|
| Quota oscillates | Widen pressure gap, increase cooldown, reduce steps, pin sensitive lanes |
| Quota never changes | See [adaptive-observability.md](adaptive-observability.md) checklist (warmup, samples, bounds, disabled flags) |
| Benchmark results worse with adaptive | Expected for some workloads; compare with `benchstat`, hooks off; see [benchmarks/adaptive-quota.md](benchmarks/adaptive-quota.md) |
| Critical lane still slow | Tune workers and max quota; adaptive does not replace capacity planning |

---

## Related docs

- [adaptive-quota.md](adaptive-quota.md) — configuration and hooks
- [adaptive-observability.md](adaptive-observability.md) — events and debugging
- [lane-priority.md](lane-priority.md) — `LaneClass` semantics
- [production-tuning.md](production-tuning.md) — broader queue tuning
- [benchmarks/adaptive-quota.md](benchmarks/adaptive-quota.md) — benchmark interpretation
