# Adaptive Quota

## Overview

Keylane v0.4 adds optional **adaptive quota**: a bounded controller that periodically evaluates runtime pressure and per-lane signals, then adjusts **lane drain quotas** through the same safe publish path as manual updates.

Adaptive quota is **disabled by default** (`DefaultAdaptiveQuotaConfig().Enabled == false`). It:

- Does **not** change `WorkerCount`, `ShardCount`, or queue capacity
- Does **not** drop queued work or cancel running jobs
- Does **not** run on the request submit hot path (evaluation runs on a timer)

Keylane helps shape concurrency and pressure; adaptive quota can improve stability for suitable workloads when configured conservatively. It is not magic auto-tuning — see [adaptive-tuning.md](adaptive-tuning.md).

### Mental model

| Concept | What it controls |
|---------|------------------|
| **Worker count** | How many goroutines drain shards |
| **Lane quota** | How many jobs per lane a worker drains per shard pass |
| **Queue capacity** | How many jobs may wait per lane (`QueueSizePerLane`) |
| **Admission policy** | Whether a request is accepted before enqueue |
| **Overload policy** | keep / reject / shed / degrade before enqueue |
| **Adaptive quota** | Bounded runtime adjustment of lane drain quotas |

---

## Static vs runtime quota

**Static (at `New`):** `Config.LaneQuotas` sets initial per-lane drain quotas when the queue is constructed.

**Runtime:** `UpdateQuotaPolicy` and `UpdateLaneQuota` publish new quotas without restarting workers. Workers load the current snapshot at the start of each drain cycle; in-flight work in that cycle keeps the snapshot it started with.

Start production with stable static quotas, observe behavior, then enable adaptive quota in staging.

---

## Quota snapshot

Inspect the active policy at any time:

```go
snap := q.CurrentQuotaPolicy()
fmt.Printf("version=%d default=%d\n", snap.Version, snap.DefaultQuota)
for lane, quota := range snap.LaneQuotas {
    fmt.Printf("lane=%s quota=%d\n", lane, quota)
}
```

`QuotaPolicySnapshot` includes a monotonic `Version` (`QuotaVersion` on events) and a defensive copy of `LaneQuotas`. Lanes are fixed at construction; only quotas for registered lanes may change.

---

## Safe quota update

Quota updates use **immutable snapshot publish**:

- Queued jobs are not dropped
- Running jobs are not interrupted
- The next worker drain cycle observes the new policy
- Each successful publish increments `Version` and may emit `OnQuotaChange` when hooks are enabled

> **Do not use quota updates as a per-request control path.** Quota changes are runtime policy changes, not request metadata.

Worked example: [Manual quota update example](#manual-quota-update-example).

---

## Min/max bounds

Per-lane clamps live in `LaneAdaptivePolicy`:

```go
{Lane: "report", MinQuota: 1, MaxQuota: 4, AllowDecrease: true}
```

- Use **`MinQuota >= 1`** so a lane never stops draining entirely by accident
- **`MaxQuota`** caps growth under pressure relief
- Setting **both** `AllowIncrease` and `AllowDecrease` to `false` pins a **fixed lane** (no automatic adjustments)

Lanes without an explicit `LaneAdaptivePolicy` inherit class from the **admission policy**, then class defaults:

| Class | Increase | Decrease |
|-------|----------|----------|
| critical | allowed | disabled |
| normal | allowed | allowed |
| background / best-effort | disabled | allowed |

See [lane-priority.md](lane-priority.md).

---

## Adaptive quota controller overview

The adaptive controller is a **periodic evaluator** (not part of the submit hot path). On each tick it:

1. Reads signals: global pressure, per-lane queue depth, queue-wait and run-duration percentiles (when timing is enabled), queue-full count, overload reject/shed counters
2. Applies hysteresis (`PressureLow` / `PressureHigh`), warmup, cooldown, and per-lane min/max bounds
3. Publishes at most `MaxAdjustmentsPerTick` quota changes via the same safe path as manual updates (CAS on `QuotaVersion`)

It does **not**:

- Resize `WorkerCount` or `ShardCount`
- Replace admission or overload policy (those run before enqueue)
- Drop queued work or cancel running jobs

Decision model and tuning: [Hysteresis](#hysteresis), [adaptive-tuning.md](adaptive-tuning.md). Observability: [adaptive-observability.md](adaptive-observability.md).

---

## Manual quota update example

Typical operator flow:

```go
before := q.CurrentQuotaPolicy()
log.Printf("payment quota=%d version=%d",
    before.LaneQuotas["payment"], before.Version)

ver, err := q.UpdateLaneQuota("payment", 3)
if err != nil {
    // invalid lane, invalid quota, version mismatch, or stopped queue
    return err
}

after := q.CurrentQuotaPolicy()
log.Printf("updated to quota=%d version=%d", after.LaneQuotas["payment"], after.Version)
```

With hooks enabled, `OnQuotaChange` fires with `source=manual` and `PolicyVersion=0`.

Coordinate with the adaptive controller using compare-and-swap:

```go
current := q.CurrentQuotaPolicy()
ver, err := q.UpdateLaneQuotaIfVersion("payment", 4, current.Version)
```

If adaptive apply races and bumps `QuotaVersion`, your update fails safely instead of overwriting an unseen change.

---

## Adaptive controller enable/disable

Enable in config:

```go
AdaptiveQuota: keylane.AdaptiveQuotaPolicy{
    Config: keylane.AdaptiveQuotaConfig{Enabled: true, /* ... */},
    Lanes:  []keylane.LaneAdaptivePolicy{ /* ... */ },
},
```

The controller goroutine starts with `q.Start(ctx)` and stops with `q.Stop`. When disabled or stopped, quotas remain at their last published values; no further automatic adjustments occur.

To turn adaptive behavior off in production, set `Enabled: false` (or omit `AdaptiveQuota`) and rely on static quotas plus manual `UpdateLaneQuota` if needed.

---

## Configuration example

```go
q, err := keylane.New(keylane.Config{
    LaneQuotas: map[keylane.Lane]int{
        "payment":    2,
        "background": 1,
    },
    AdaptiveQuota: keylane.AdaptiveQuotaPolicy{
        Config: keylane.AdaptiveQuotaConfig{
            Enabled:               true,
            EvaluationInterval:    time.Second,
            WarmupDuration:        5 * time.Second,
            CooldownDuration:      5 * time.Second,
            PressureLow:           0.60,
            PressureHigh:          0.85,
            QueueWaitHigh:         25 * time.Millisecond,
            IncreaseStep:          1,
            DecreaseStep:          1,
            MaxAdjustmentsPerTick: 1,
        },
        Lanes: []keylane.LaneAdaptivePolicy{
            {
                Lane: "payment", Class: keylane.LaneCritical,
                MinQuota: 1, MaxQuota: 8,
                AllowIncrease: true, AllowDecrease: false,
            },
        },
    },
})
```

Tuning guidance: [adaptive-tuning.md](adaptive-tuning.md).

---

## Hysteresis

- `pressure <= PressureLow` — increases may be considered
- `pressure >= PressureHigh` — decreases may be considered
- between thresholds — **hold** (reduces quota flapping)

**Background** and **best-effort** lanes may still **decrease** when that lane's overload shed/reject counters are elevated, even if global pressure is below `PressureHigh`.

Per-lane **cooldown** applies after each successful change.

---

## Safety rules

- No unbounded quota growth — `MaxQuota` and `MaxAdjustmentsPerTick` cap each tick
- Cooldown and hysteresis reduce oscillation
- Adaptive apply uses **CAS on `QuotaVersion`** — concurrent manual updates win; adaptive skips that tick (`quota_update_failed`)
- Critical class blocks automatic **decrease** by default, not unlimited **increase** under all conditions

---

## Common mistakes

1. **Enabling adaptive without static baseline** — Run with fixed quotas first; understand queue wait and pressure before automating changes.
2. **Treating critical as unlimited** — Critical lanes still hit admission depth caps and overload thresholds; class only shifts defaults.
3. **Marking every lane critical** — If every lane is critical, no lane is protected relative to others. See [lane-priority.md](lane-priority.md).
4. **Expecting adaptive on every submit** — Evaluation is periodic; submit path cost should stay low when hooks are off.
5. **Slow hooks** — `OnQuotaChange` and `OnAdaptiveQuotaDecision` must be fast; do not block on network calls inside hooks.

---

## Observability

```go
snap := q.AdaptiveDebugSnapshot()
// Enabled, Running, LastDecisions, per-lane LaneAdaptiveStats, PolicyVersion, QuotaVersion
```

```go
hooks.OnQuotaChange = func(e keylane.QuotaChangeEvent) { /* ... */ }
hooks.OnAdaptiveQuotaDecision = func(e keylane.AdaptiveQuotaDecisionEvent) { /* ... */ }
```

Full event reference: [adaptive-observability.md](adaptive-observability.md).

`AdaptiveQuotaSnapshot()` is deprecated; use `AdaptiveDebugSnapshot()`.

---

## Manual updates while adaptive is on

`UpdateLaneQuota` and `UpdateQuotaPolicy` remain available. All updates use the same validation and snapshot publish path.

The adaptive controller applies changes with compare-and-swap on `QuotaVersion`. If a manual update bumps the version between evaluation and apply, the adaptive adjustment is skipped for that tick. Prefer `UpdateLaneQuotaIfVersion` when coordinating explicitly.

---

## Limits and expectations

- **GC pauses** — Adaptive quota does not prevent Go GC pauses. It may reduce concurrency-driven allocation bursts indirectly by bounding lane drain quotas; see [gc-pressure-shaping.md](gc-pressure-shaping.md).
- **Indirect shaping** — Quota changes shape how much work drains per tick; they are not a GC tuner or memory limiter.
- **Critical lanes under overload** — Hysteresis, queue-full guards, cooldown, and per-tick caps still gate increases.

---

## Troubleshooting

| Symptom | Things to check |
|---------|-----------------|
| Quota never changes | `Enabled`, `Running`, warmup (`warmup_active`), insufficient samples, hold band (`neutral_pressure`), increase/decrease disabled, at min/max bound |
| Quota changes too often | Increase `CooldownDuration`, widen `PressureLow`/`PressureHigh` gap, reduce `MaxAdjustmentsPerTick` |
| Controller does not start | `Start` error, `Enabled: false`, validation failure at `New` |
| Controller stops adjusting | Queue stopped; check `AdaptiveDebugSnapshot().Running` |
| Manual update ignored adaptive tick | Expected CAS behavior; check `quota_update_failed` on decision hook |

See [adaptive-observability.md](adaptive-observability.md) and [debugging.md](debugging.md).

---

## Related documentation

- [adaptive-tuning.md](adaptive-tuning.md) — rollout and tuning
- [adaptive-observability.md](adaptive-observability.md) — events and snapshots
- [lane-priority.md](lane-priority.md) — `LaneClass`
- [overload-policy.md](overload-policy.md) — overload before enqueue
- [admission-control.md](admission-control.md) — admission policy
- [production-tuning.md](production-tuning.md) — capacity and observability modes
- [benchmarks/adaptive-quota.md](benchmarks/adaptive-quota.md) — benchmarks
