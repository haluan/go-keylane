# Shard Pressure Diagnostics

This document explains how to read shard pressure diagnostics and distinguish pressure **shape** — hot key, hot lane, hot shard, worker saturation, distributed backlog, and burst vs sustained overload.

Detailed API reference: [pressure-diagnostics.md](pressure-diagnostics.md). Balancing patterns: [shard-pressure-balancing.md](shard-pressure-balancing.md).

---

## Pull APIs

| API | Use when |
|-----|----------|
| `Queue.PressureSummary()` | Global class, `ScaleRelevant`, `MitigationRelevant`, hot shard list |
| `Queue.ShardPressure(shardID)` | One shard's lane breakdown and hot key rows |
| `Queue.HotShards()` | Ranked hot shards by depth |
| `Queue.DebugSnapshot().ShardPressure` | Flat slice of per-shard snapshots |

Always check `DiagnosticsEnabled` before reading `Class` or ratios. Disabled config returns neutral snapshots, not `unknown`.

---

## Interpretation matrix

| What you see | Likely meaning | Scale-out helps? | First response |
|--------------|----------------|------------------|----------------|
| **One hot key, one shard hot** | Localized overload | Often **no** | Per-key mitigation; split or rate-limit the key |
| **Many shards hot, queue wait rising** | Distributed demand | **Yes** | More workers; global admission tuning |
| **One lane hot across many shards** | Lane quota or workload class imbalance | Sometimes | Lane quota / admission policy |
| **Workers saturated, depth moderate** | Execution capacity bound | **Yes** | More workers or faster `Run` |
| **One shard hot, many keys share load** | Hot shard (skew) | Sometimes | Reshard; spread key space |
| **Spike then recovery** | Temporary burst | Usually **no** | Wait; avoid reactive scale on short spikes |
| **Sustained high depth + wait** | Sustained overload | **Yes** | Scale + admission review |

### Spec examples (plain language)

```text
One hot key, one shard hot:
  likely localized overload; scale-out may not help much.

Many shards hot, queue wait rising:
  likely distributed demand; scale-out may help.

One lane hot across many shards:
  lane quota or workload class may need adjustment.

Workers saturated with rising queue depth:
  service likely needs more execution capacity.
```

---

## Pressure classes

| Class | Meaning |
|-------|---------|
| `healthy` | Below hot thresholds |
| `localized_key` | Bounded hot key explains most shard pressure |
| `lane_dominant` | One lane dominates shard depth |
| `shard_hot` | Shard at or above hot threshold |
| `distributed` | Many shards hot at once |
| `worker_bound` | Workers busy, wait rising, low shard skew |
| `unknown` | Enabled but insufficient data |

---

## Scale vs mitigation flags

`PressureSummarySnapshot` includes:

- **`ScaleRelevant`** — many hot shards, high global depth/wait, or worker saturation
- **`MitigationRelevant`** — localized hot-key pressure; per-key admission may help more than scale-out

Pair with `ScaleSignal.Recommended`, `Reason`, and `Scope` from [autoscaling-signals.md](autoscaling-signals.md).

---

## Reading snapshots together

```go
snap := q.DebugSnapshot()
// Coarse depth: snap.Pressure
// Global class: snap.PressureSummary
// Per shard: snap.Shards[i].ShardPressure, HotKeyCandidate
// Per-key mitigation: snap.Mitigations
// Flat shard list: snap.ShardPressure
```

Decision order:

1. If `PressureSummary.ScaleRelevant` → consider capacity (see autoscaling signals).
2. If `PressureSummary.MitigationRelevant` → inspect `HotKeys` and per-key policy.
3. If `DominantLane` is set → tune lane policy before resharding.

---

## Privacy

Hot key rows in pressure snapshots use **`key_hash` only**. Raw keys are never included in shard pressure snapshots even when `HotKey.ExposeRawKey` is enabled for detection snapshots.

---

## Prometheus metrics

See [metrics.md](metrics.md) for the full v0.5 metric list. Shard-pressure related names:

- `keylane_shard_pressure_ratio` — global composite ratio
- `keylane_shard_depth` / `keylane_shard_queue_depth` — per-shard queued depth (same value; spec alias)
- `keylane_hot_shard_count` — count of hot shards

---

## Related documentation

- [pressure-diagnostics.md](pressure-diagnostics.md) — configuration and formulas
- [shard-pressure-balancing.md](shard-pressure-balancing.md) — pattern table
- [hot-key-detection.md](hot-key-detection.md)
- [per-key-admission-policy.md](per-key-admission-policy.md)
- [v0.5-hot-key-autoscaling-signals.md](v0.5-hot-key-autoscaling-signals.md) — milestone overview
- [observability.md](observability.md) — hooks and DebugSnapshot
- [runbooks/hot-key-and-scale-pressure.md](runbooks/hot-key-and-scale-pressure.md) — operator guide
