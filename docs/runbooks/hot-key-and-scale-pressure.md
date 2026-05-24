# Runbook: Hot Key and Scale Pressure

Operator guide for v0.5 runtime signals. go-keylane exposes **advisory** pressure metrics and snapshots — it does not scale replicas or fix hot keys automatically.

> CPU tells you how busy the process is. Keylane tells you how much demand the process is holding back.

---

## What should I check when queue wait rises?

1. **`StatsGCPressure().QueueWait`** — is wait actually high vs run duration?
2. **`PressureSummary()`** — check `Class`, `ScaleRelevant`, `MitigationRelevant`
3. **`DebugSnapshot().HotShards` / `HotLanes`** — one shard or one lane dominating?
4. **`DebugSnapshot().HotKeys`** — one key hash dominating a shard?
5. **`WorkerBusyRatio`** — workers saturated vs queue buildup?
6. **Worker count and lane quotas** — see [production-tuning.md](../production-tuning.md)

| Finding | Likely cause | Action |
|---------|--------------|--------|
| One dominant `KeyHash` | Hot key | Per-key mitigation; client rate limit |
| One lane across shards | Lane imbalance | Lane quota / admission |
| Many hot shards | Distributed backlog | Scale workers; review global admission |
| High wait, moderate depth | Worker-bound | More workers or faster `Run` |

---

## What should I check when CPU is flat but latency rises?

Backpressure holds work in queues. CPU and memory may stay flat while demand accumulates.

1. **`ScaleSignal()`** — `Recommended`, `Reason`, `Scope`, `DiagnosticsEnabled`
2. **`queue_depth_ratio` / `Pressure.DepthRatio`** — queues filling?
3. **`queue_wait_max_seconds` / queue wait stats** — clients waiting?
4. **`admission_rejected_total` / `admission_throttled_total`** — explicit rejections?
5. **`worker_busy_ratio`** — workers busy while CPU looks idle (I/O-bound)?

Do **not** conclude "healthy" from CPU alone when queue wait or reject rates are elevated.

---

## What should I check when admission rejects increase?

1. **Separate overload vs lane admission vs per-key:**

   | Error / signal | Source |
   |----------------|--------|
   | `OverloadError` | Global/lane overload policy |
   | `ErrAdmissionRejected` | Lane admission policy |
   | `ErrPerKeyAdmissionThrottled/Rejected/Shed` | Per-key mitigation |

2. **`PressureSummary.Class`** — localized vs distributed
3. **`OnOverloadPolicyDecision` / `OnPerKeyAdmissionDecision` hooks** (if enabled)
4. **Recent config changes** — admission thresholds, per-key `DefaultAction`

Per-key rejects protect unrelated keys. Global rejects may need capacity or overload policy tuning.

---

## How do I know if scale-out may help?

Check **`ScaleSignal.Scope`** and supporting metrics:

| Signal | Scale-out likely helps? |
|--------|----------------------|
| `Scope=global`, `Reason=distributed_pressure` or `many_hot_shards` | **Yes** |
| `Scope=global`, high `HotShardCount`, rising queue wait | **Yes** |
| `Reason=worker_saturated`, high `WorkerBusyRatio` | **Yes** |
| `Reason=localized_hot_key`, `Scope=hot_key` | **No** — mitigate the key |
| `Scope=shard`, one hot shard, mixed keys | **Maybe** — resharding first |

Use sustained signals (multiple consecutive windows), not single scrapes.

---

## How do I know if one hot key is the real problem?

1. **`DebugSnapshot().HotKeys`** — one hash with high `DepthRatio` / `dominant` status
2. **`PressureSummary.MitigationRelevant == true`**
3. **`ScaleSignal.LocalizedHotKey == true`** or `Reason=localized_hot_key`
4. **`HotShardCount` low** (one or few shards hot)
5. **Cross-check:** many unique keys on the hot shard → hot shard, not hot key

Response: enable or tighten [per-key-admission-policy.md](../per-key-admission-policy.md); rate-limit the client; split key space. Do not scale the whole service for a single-key storm.

---

## Safe metrics to alert on

| Alert | Metric / API | Notes |
|-------|--------------|-------|
| Sustained queue pressure | `keylane_queue_depth_ratio > 0.80` for 5m | Combine with wait |
| Scale recommendation | `keylane_scale_recommended{scope="global"} == 1` for 2m | Check `reason` label |
| Worker saturation | `keylane_worker_busy_ratio > 0.85` for 5m | |
| Rising rejects | `rate(keylane_admission_rejected_total[5m])` | Per-lane or `_all` |
| Queue wait | `keylane_queue_wait_max_seconds` or queue wait summary | Not CPU |

---

## Metrics that should not be used alone

- **`hot_key_candidate_count`** without `PressureSummary` / `ScaleSignal` context — candidates are approximate
- **`scale_recommended`** without checking `scope` — `hot_key` scope means do not scale globally
- **Raw hot key count spikes** during bursts — distinguish burst vs sustained overload
- **Process CPU** — flat CPU does not mean healthy under backpressure

See [metrics.md](../metrics.md) for label safety.

---

## Quick reference checklist

```text
[ ] Queue wait vs run duration separated (StatsGCPressure)
[ ] PressureSummary.DiagnosticsEnabled == true
[ ] ScaleSignal.DiagnosticsEnabled == true
[ ] HotKeys inspected (hash only — no raw keys in dashboards)
[ ] ScaleSignal.Scope checked before scale-out
[ ] Per-key mitigation considered before global scale
[ ] Alerts use low-cardinality labels only
```

---

## Related docs

- [v0.5-hot-key-autoscaling-signals.md](../v0.5-hot-key-autoscaling-signals.md)
- [debugging.md](../debugging.md) — symptom table
- [autoscaling-signals.md](../autoscaling-signals.md)
- [shard-pressure-diagnostics.md](../shard-pressure-diagnostics.md)
