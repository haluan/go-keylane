# Adaptive Quota

KL-1404 adds an optional adaptive quota controller that periodically evaluates runtime pressure and per-lane queue-wait signals, then adjusts lane drain quotas through the same safe [`UpdateQuotaPolicy`](quota_policy.go) path as manual updates.

Adaptive quota is **disabled by default**. It does not resize workers or shards, does not drop queued work, and does not run on the request submit hot path.

## Limits and expectations

- **GC pauses** — Adaptive quota does not prevent Go GC pauses. It may reduce concurrency-driven allocation bursts indirectly by bounding lane drain quotas; see [gc-pressure-shaping.md](gc-pressure-shaping.md).
- **Indirect shaping** — Quota changes shape how much work drains per tick; they are not a GC tuner or memory limiter.
- **Critical lanes under overload** — `AllowIncrease` on critical lanes does not mean unbounded growth during global overload. Hysteresis (`PressureLow` / `PressureHigh`), queue-full guards, cooldown, and per-tick caps still gate increases. Critical class protects against automatic **decrease**, not automatic increase in all conditions.

## Configuration

```go
q, err := keylane.New(keylane.Config{
    LaneQuotas: map[keylane.Lane]int{
        "payment":    2,
        "background": 1,
    },
    AdaptiveQuota: keylane.AdaptiveQuotaPolicy{
        Config: keylane.AdaptiveQuotaConfig{
            Enabled:            true,
            EvaluationInterval: time.Second,
            WarmupDuration:     5 * time.Second,
            CooldownDuration:     5 * time.Second,
            PressureLow:        0.60,
            PressureHigh:       0.85,
            QueueWaitHigh:      25 * time.Millisecond,
            IncreaseStep:       1,
            DecreaseStep:       1,
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

Set **both** `AllowIncrease` and `AllowDecrease` to `false` on a lane to pin it as a **fixed lane** (no automatic adjustments regardless of class defaults).

Lanes without an explicit `LaneAdaptivePolicy` entry inherit the lane class from the **admission policy** (KL-1402), then receive class-based adaptive defaults:

| Class | Increase | Decrease |
|-------|----------|----------|
| critical | allowed | disabled |
| normal | allowed | allowed |
| background / best-effort | disabled | allowed |

Explicit `LaneAdaptivePolicy` entries override class and bounds.

## Hysteresis

- `pressure <= PressureLow` — increases may be considered
- `pressure >= PressureHigh` — decreases may be considered
- between thresholds — hold (reduces quota flapping)

**Background** and **best-effort** lanes may still **decrease** when that lane's overload shed/reject counters are elevated, even if global pressure is below `PressureHigh`. See [adaptive-tuning.md](adaptive-tuning.md).

Per-lane cooldown applies after each successful change.

## Observability

```go
snap := q.AdaptiveQuotaSnapshot()
```

Hook on quota change:

```go
obs.Hooks.OnAdaptiveQuotaDecision = func(e keylane.AdaptiveQuotaEvent) { ... }
```

Events include lane, old/new quota, action, reason, `PolicyVersion`, and `QuotaVersion`. The hook also fires on apply failure with reason `quota_update_failed` (quota unchanged).

`PolicyVersion` is reserved for future adaptive-policy snapshot generations. It is currently always `1` for the lifetime of a controller (no runtime `UpdateAdaptivePolicy` API yet) and will increment when that API exists. `QuotaVersion` is the active quota policy snapshot version from KL-1401.

## Manual updates

`UpdateLaneQuota` and `UpdateQuotaPolicy` remain available while adaptive quota is enabled. All updates use the same validation and snapshot publish path.

The adaptive controller applies changes with **compare-and-swap** on `QuotaVersion`: if a manual update bumps the quota policy version between evaluation and apply, the adaptive adjustment is skipped for that tick (hook reports `quota_update_failed`). Use `UpdateLaneQuotaIfVersion` when you need explicit version checks.

See also [adaptive-tuning.md](adaptive-tuning.md), [lane-priority.md](lane-priority.md), [production-tuning.md](production-tuning.md), [overload-policy.md](overload-policy.md), and [admission-control.md](admission-control.md).
