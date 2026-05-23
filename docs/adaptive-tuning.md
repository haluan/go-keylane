# Adaptive Quota Tuning

This guide covers practical tuning for the KL-1404 adaptive quota controller. For API reference and lifecycle, see [adaptive-quota.md](adaptive-quota.md).

## Pressure thresholds

| Field | Role |
|-------|------|
| `PressureLow` | Global pressure at or below this level allows **increase** evaluation (when other signals agree). |
| `PressureHigh` | Global pressure at or above this level allows **decrease** evaluation for all eligible lanes. |

Keep a gap between `PressureLow` and `PressureHigh` (defaults `0.60` / `0.85`) so the controller spends time in the **hold** band and avoids quota flapping.

## Cooldown and warmup

- **`WarmupDuration`** — no adjustments until the queue has been running long enough to collect meaningful queue-wait and run-time samples.
- **`CooldownDuration`** — per-lane minimum time between successful quota changes.

Increase cooldown when you see oscillation; shorten it only when you need faster reaction and have validated stability under load tests.

## Per-lane bounds

Each `LaneAdaptivePolicy` can set:

- `MinQuota` / `MaxQuota` — hard clamps applied after every decision.
- `AllowIncrease` / `AllowDecrease` — per-lane capability flags merged with [lane class](lane-priority.md) defaults.

Set **both** `AllowIncrease` and `AllowDecrease` to `false` for a **fixed lane**; the resolver treats that as an explicit fixed quota (not class defaults).

## Class defaults

When a lane has no explicit adaptive entry, its class comes from the active **admission policy** lane class (not a hard-coded normal default). Resolved policies are fixed at queue construction; admission policy updates after `New` do not refresh adaptive class until a future rebuild API.

Class-based adaptive defaults:

| Class | Increase | Decrease |
|-------|----------|----------|
| critical | yes | no |
| normal | yes | yes |
| background / best-effort | no | yes |

Critical lanes are protected from automatic shrink; background and best-effort lanes are protected from automatic growth.

### Critical lanes under pressure

Even when a critical lane has `AllowIncrease: true`, the controller does not increase quotas during global overload. `PressureLow` / `PressureHigh` hysteresis, queue-full counters, cooldown, and `MaxAdjustmentsPerTick` still apply. Critical class means “do not shrink automatically,” not “grow whenever queue wait is high.”

## Localized overload decrease

Even when global pressure is below `PressureHigh`, **background** and **best-effort** lanes may decrease quota when overload shed, reject, or **degrade** counters on that lane are elevated. This ties adaptive shrink to localized overload storms without punishing critical traffic globally.

See [overload-policy.md](overload-policy.md) for counter semantics.

## Queue-full guard

Automatic quota **increase** is blocked for a lane while that lane's queue-full counter is non-zero (spec: queue full must not trigger increase).

## Concurrent manual quota updates

Adaptive apply uses `QuotaVersion` from the evaluation snapshot. A concurrent `UpdateQuotaPolicy` / `UpdateLaneQuota` that bumps the version causes the adaptive change to be skipped for that tick (`quota_update_failed` hook reason). Prefer `UpdateLaneQuotaIfVersion` when coordinating with the controller.

## When to disable adaptive quota

Disable (`Enabled: false`) when:

- Quotas are owned entirely by an external control plane.
- You need deterministic capacity for benchmarks or compliance tests.
- Warmup/cooldown cannot be tuned safely for your traffic mix.

Manual `UpdateLaneQuota` / `UpdateQuotaPolicy` remain available when adaptive quota is off.

## Policy and quota versions on events

`QuotaVersion` on each decision reflects the quota policy snapshot the controller observed at evaluation time. `PolicyVersion` is reserved for future adaptive-policy updates and is currently always `1` for the controller lifetime.

## Related docs

- [adaptive-quota.md](adaptive-quota.md) — configuration and hooks
- [lane-priority.md](lane-priority.md) — `LaneClass` semantics
- [production-tuning.md](production-tuning.md) — broader queue tuning
