# Adaptive Observability (v0.4)

This guide explains how to observe and debug v0.4 adaptive quota, overload policy, and quota policy changes. For general hooks, timing, and adapters, see [observability.md](observability.md).

---

## Overview

v0.4 adds three event types and a diagnostic snapshot for operators:

| Mechanism | Use when |
|-----------|----------|
| `OnQuotaChange` | Every successful quota publish (manual or adaptive) |
| `OnAdaptiveQuotaDecision` | Adaptive evaluation outcome (change, apply failure, optional hold tracing) |
| `OnOverloadPolicyDecision` | Overload reject, shed, or degrade (not on keep) |
| `AdaptiveDebugSnapshot()` | Point-in-time controller state and per-lane adaptive counters |
| `StatsGCPressure()` | Cumulative overload and queue counters per lane |

Enable hooks with `Observability.EnableHooks` (off in low-allocation mode). Hooks run outside scheduler and quota locks. Keep callbacks fast and non-blocking — do not perform slow network I/O inside hooks.

```go
cfg := keylane.Config{
    Observability: keylane.ObservabilityConfig{
        EnableHooks: true,
        Hooks: keylane.Hooks{
            OnQuotaChange: func(e keylane.QuotaChangeEvent) {
                // log or export metrics
            },
            OnAdaptiveQuotaDecision: func(e keylane.AdaptiveQuotaDecisionEvent) {
                // adaptive evaluation outcome
            },
            OnOverloadPolicyDecision: func(e keylane.OverloadPolicyEvent) {
                // reject / shed / degrade
            },
        },
    },
}
```

Set `EnableAdaptiveDecisionTracing` to receive **hold** decisions on `OnAdaptiveQuotaDecision` (noisy). Hold decisions always update `LaneAdaptiveStats.LastDecision` in `AdaptiveDebugSnapshot`; the `AdaptiveHoldTotal` counter increments only when tracing is enabled.

---

## Quota change events

`QuotaChangeEvent` fires after every **successful** quota policy publish.

| Field | Meaning |
|-------|---------|
| `Lane` | Lane whose quota changed |
| `OldQuota`, `NewQuota` | Drain quota before and after |
| `Source` | `manual` (`UpdateQuotaPolicy` / `UpdateLaneQuota`) or `adaptive` |
| `Reason` | Human-readable detail (adaptive may set a reason string) |
| `QuotaVersion` | Monotonic generation of the active quota snapshot (`CurrentQuotaPolicy().Version`) |
| `PolicyVersion` | Adaptive controller config generation when `source=adaptive`; `0` for `source=manual` |

```go
hooks.OnQuotaChange = func(e keylane.QuotaChangeEvent) {
    log.Printf("quota lane=%s %d->%d source=%s ver=%d",
        e.Lane, e.OldQuota, e.NewQuota, e.Source, e.QuotaVersion)
}
```

Manual updates emit one event per lane that changed. Adaptive updates emit through the same path after a successful apply.

---

## Adaptive quota decision events

`AdaptiveQuotaDecisionEvent` is the KL-1405 spec name; `AdaptiveQuotaEvent` is a type alias for the same struct.

The hook fires when:

- A quota change is **applied** successfully
- Apply **fails** (e.g. `quota_update_failed` — version mismatch with a concurrent manual update)
- A **hold** decision is traced (`EnableAdaptiveDecisionTracing`)

| Field | Meaning |
|-------|---------|
| `Action` | `hold`, `increase`, or `decrease` |
| `Reason` | Stable code, e.g. `global_pressure_high`, `cooldown_active`, `at_min_bound` |
| `GlobalPressure`, `QueueDepth`, `InFlight` | Signals at evaluation time |
| `QueueWaitP50` / `P95` / `P99`, `RunP50` / `P95` / `P99` | Per-lane timing samples when available |
| `PolicyVersion` | Adaptive config generation (currently `1` until runtime policy reload exists) |
| `QuotaVersion` | Quota snapshot version used for CAS apply |

See [Reading event reason codes](#reading-event-reason-codes) for the full reason table.

---

## Reading event reason codes

`AdaptiveQuotaDecisionEvent.Reason` and `LaneAdaptiveStats.LastDecision` use stable `QuotaAdjustmentReason` codes:

| Reason | Typical meaning |
|--------|-----------------|
| `neutral_pressure` | Pressure between `PressureLow` and `PressureHigh` (hold band) |
| `global_pressure_high` | Global pressure at or above `PressureHigh` (decrease eligible) |
| `cooldown_active` | Per-lane cooldown after last successful change |
| `warmup_active` | Controller still in warmup |
| `insufficient_samples` | Not enough queue-wait/run samples yet |
| `at_min_bound` / `at_max_bound` | Clamped by `MinQuota` / `MaxQuota` |
| `increase_disabled` / `decrease_disabled` | Lane or class disallows direction |
| `quota_update_failed` | CAS apply failed (quota unchanged) |
| `queue_full` | Queue-full guard blocked increase |

**Hold decisions:** `LastDecision` always reflects the latest reason in `AdaptiveDebugSnapshot`. The `AdaptiveHoldTotal` counter increments only when `EnableAdaptiveDecisionTracing` is on; use `LastDecision` to debug holds without noisy hooks.

Overload events use separate `OverloadReason` codes on `OverloadPolicyEvent` — see [overload-policy.md](overload-policy.md).

---

## Overload policy events

`OverloadPolicyEvent` fires on **reject**, **shed**, or **degrade**. **Keep** decisions do not emit events (no hook allocation on the happy path).

| Field | Meaning |
|-------|---------|
| `Action` | `reject`, `shed`, or `degrade` |
| `Reason` | Stable overload reason code |
| `RetryAfter` | Suggested retry delay (advisory) |
| `BackoffHint` | Structured backoff guidance for callers |
| `GlobalPressure`, `LanePressure` | Pressure at decision time |
| `QueueDepth`, `MaxQueueDepth` | Lane depth vs policy cap |
| `PolicyVersion` | Active overload policy generation |

HTTP middleware can map `RetryAfter` to a `Retry-After` header when `EnableRetryAfter` is set. See [overload-policy.md](overload-policy.md).

Per-lane cumulative counters are also in `StatsGCPressure()`:

| Counter | When incremented |
|---------|------------------|
| `OverloadRejected` | `OverloadActionReject` |
| `OverloadShed` | `OverloadActionShed` |
| `OverloadDegrade` | `OverloadActionDegrade` |

---

## Per-lane counters

**Overload (all integrations):** use `StatsGCPressure().Lanes[].Counters` for `OverloadRejected`, `OverloadShed`, `OverloadDegrade`, plus admission and queue-full totals.

**Adaptive (when controller enabled):** `AdaptiveDebugSnapshot().Lanes[]` exposes `LaneAdaptiveStats`:

| Field | Meaning |
|-------|---------|
| `QuotaChangeTotal` | Successful quota publishes affecting this lane |
| `AdaptiveIncreaseTotal` / `AdaptiveDecreaseTotal` / `AdaptiveHoldTotal` | Adaptive decisions by action (hold counter needs tracing) |
| `LastQuotaChange` | Time of last quota change on this lane |
| `LastDecision` | Most recent `QuotaAdjustmentReason` (including hold) |

`LaneAdaptiveStats` also mirrors overload-style totals (`KeepTotal`, `RejectTotal`, etc.) for adaptive evaluation context.

---

## Adaptive debug snapshot

Prefer `AdaptiveDebugSnapshot()` over deprecated `AdaptiveQuotaSnapshot()`.

```go
snap := q.AdaptiveDebugSnapshot()
fmt.Printf("enabled=%v running=%v ticks=%d quotaVer=%d\n",
    snap.Enabled, snap.Running, snap.TickCount, snap.QuotaVersion)
for _, lane := range snap.Lanes {
    fmt.Printf("lane=%s last=%s increases=%d decreases=%d\n",
        lane.Lane, lane.LastDecision, lane.AdaptiveIncreaseTotal, lane.AdaptiveDecreaseTotal)
}
```

| Field | Meaning |
|-------|---------|
| `Enabled` | Adaptive config has `Enabled: true` |
| `Running` | Controller goroutine is active (requires `Start`) |
| `LastEvaluation` | Time of last tick |
| `TickCount` | Evaluation ticks since start |
| `LastDecisions` | Recent decisions across lanes (bounded) |
| `Lanes` | Per-lane `LaneAdaptiveStats` (defensive copy) |
| `PolicyVersion`, `QuotaVersion` | Active policy generations |

Call on a timer or admin path, not on every `Submit`.

---

## Example event flow

Typical sequence when a background lane is overloaded:

1. Report lane queue depth rises; global pressure increases.
2. Overload policy sheds best-effort report requests → `OnOverloadPolicyDecision` (shed).
3. Adaptive controller sees high global pressure on the next tick.
4. Controller decreases report lane quota within `MinQuota` / `MaxQuota` → successful apply → `OnQuotaChange` (`source=adaptive`) and `OnAdaptiveQuotaDecision` (decrease).
5. Pressure eases; controller enters hold band (`neutral_pressure`) or cooldown.
6. During cooldown, evaluations may hold → `LastDecision=cooldown_active`; hold hook only if tracing is on.

Correlate with `CurrentQuotaPolicy()`, `CurrentOverloadPolicy()`, and `Pressure()`.

---

## Debugging rejected requests

1. **Overload vs admission** — If both are enabled, overload runs first. Check `OverloadPolicyEvent` or `StatsGCPressure` overload counters vs `AdmissionRejected`.
2. **Lane class** — Was the lane classified correctly? Best-effort sheds earlier than critical. See [lane-priority.md](lane-priority.md).
3. **Depth caps** — Did `MaxQueueDepth` or `RejectAboveRatio` trigger? Compare `QueueDepth` / `MaxQueueDepth` on overload events.
4. **HTTP** — Confirm overload middleware is enabled and status mapping matches expectations (reject → 503, shed → 429 by default).

---

## Debugging quota changes

1. **No changes** — Is adaptive enabled and `Running`? Still in `warmup_active`? `insufficient_samples`? Pressure in hold band? `increase_disabled` / `decrease_disabled` on critical/background lanes?
2. **Too frequent** — Shorten evaluation interval only after validating stability; usually **increase** `CooldownDuration` and keep a gap between `PressureLow` and `PressureHigh`.
3. **Unexpected manual wins** — Concurrent `UpdateLaneQuota` bumps `QuotaVersion`; adaptive apply skips with `quota_update_failed`.
4. **Hooks silent** — `EnableHooks` false or nil callbacks; low-allocation observability mode disables hooks.

---

## Debugging controller shutdown

### Adaptive controller does not start

| Symptom | Check |
|---------|--------|
| Never runs | `AdaptiveQuotaConfig.Enabled`, `q.Start(ctx)` returned nil |
| `Running` false while queue active | Start failed or queue already stopped |

### Adaptive controller does not stop

| Symptom | Check |
|---------|--------|
| `Stop` returns timeout | Drain backlog with `WithDrain(true)` or shorten in-flight work; jobs must respect `ctx.Done()` |
| `Running` true briefly after `Stop` | Controller goroutine may finish one tick; recheck `AdaptiveDebugSnapshot()` after `Stop` returns |
| Process hangs on shutdown | Slow or blocking hook callbacks — hooks must be non-blocking; no network I/O in `OnQuotaChange` / `OnAdaptiveQuotaDecision` |
| Goroutine leak suspected | After full `Stop`, confirm `AdaptiveDebugSnapshot().Running == false` and `Enabled` matches config |

### After stop

Quota frozen after stop is expected — no further adaptive ticks. Events stop when hooks are disabled or the queue is stopped; in-flight work may still complete. Use `AdaptiveDebugSnapshot()` for final `TickCount` and `LastDecisions`.

---

## Troubleshooting checklist

When behavior looks wrong, answer:

- Was the lane classified correctly (`LaneClass` on admission/overload/adaptive policy)?
- Did the lane hit `MaxQueueDepth`?
- Did global pressure exceed `PressureHigh` or stay below `PressureLow`?
- Did cooldown prevent a quota change (`cooldown_active`)?
- Did min/max bounds block a change (`at_min_bound`, `at_max_bound`)?
- Was the adaptive controller disabled or not running?
- Were hooks configured (`EnableHooks` and non-nil callbacks)?
- Is `EnableAdaptiveDecisionTracing` required to see hold events in hooks?
- Does the controller fail to stop after `Stop` (blocked hooks, drain timeout, `Running` stuck true)?

See also [debugging.md](debugging.md) (v0.4 section) and [adaptive-tuning.md](adaptive-tuning.md).

---

## Related documentation

- [observability.md](observability.md) — hooks, timing, adapters
- [adaptive-quota.md](adaptive-quota.md) — controller configuration
- [adaptive-tuning.md](adaptive-tuning.md) — safe rollout and tuning
- [overload-policy.md](overload-policy.md) — overload actions and HTTP mapping
- [benchmarks/adaptive-quota.md](benchmarks/adaptive-quota.md) — benchmark interpretation
