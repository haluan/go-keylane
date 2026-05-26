# DebugSnapshot (v0.5)

`Queue.DebugSnapshot()` returns a near-time diagnostic view of scheduler queue state. Version `"5"` adds backend resource pressure (KL-1704) on top of v0.5 hot key, shard pressure, and autoscaling fields.

Safe for concurrent reads while workers run. Does not guarantee a globally atomic view across all shards.

---

## When to use which API

| Need | API |
|------|-----|
| Full incident bundle (shards, lanes, hot keys, mitigation) | `DebugSnapshot()` |
| Frequent autoscaler polling | `ScaleSignal()` — lower allocation |
| Mitigation vs scale classification only | `PressureSummary()` |
| Single shard drill-down | `ShardPressure(shardID)` |

Do not call `DebugSnapshot()` on every submit. Sample on a timer or admin endpoint.

---

## v0.5 top-level fields

| Field | Purpose |
|-------|---------|
| `PressureSummary` | Global pressure class, `ScaleRelevant`, `MitigationRelevant` |
| `ScaleSignal` | Autoscaling recommendation with reason and scope |
| `PerKeyAdmissionSnapshots` | Per-key mitigation state (bounded) |
| `HotKeys` | Flat list of `HotKeyCandidateSnapshot` (stable-sorted) |
| `Mitigations` | Flat list of `PerKeyMitigationSnapshot` |
| `ShardPressure` | Flat slice of per-shard `ShardPressureSnapshot` |
| `HotShards` / `HotLanes` | Legacy depth rankings (backward compatible) |
| `Shards[]` | Per-shard depth, hot key candidates, nested `ShardPressure` |

Legacy fields (`Pressure`, lane/shard counters, policy versions) remain unchanged from earlier versions.

---

## Example JSON shape

Illustrative output using actual exported field names (values are examples):

```json
{
  "Version": "5",
  "GeneratedAt": "2026-05-23T12:00:00Z",
  "ShardCount": 4,
  "WorkerCount": 2,
  "TotalDepth": 820,
  "TotalCapacity": 1000,
  "Pressure": {
    "DepthRatio": 0.82,
    "IsPressured": true
  },
  "PressureSummary": {
    "DiagnosticsEnabled": true,
    "Class": "distributed",
    "ScaleRelevant": true,
    "MitigationRelevant": false,
    "WorkerBusyRatio": 0.91,
    "HotShardCount": 3
  },
  "ScaleSignal": {
    "DiagnosticsEnabled": true,
    "Recommended": true,
    "PressureRatio": 0.87,
    "Reason": "queue_wait_high",
    "Scope": "global",
    "QueueDepthRatio": 0.82,
    "WorkerBusyRatio": 0.91,
    "HotShardCount": 3,
    "HotKeyCandidateCount": 1,
    "LocalizedHotKey": false
  },
  "HotKeys": [
    {
      "ShardID": 2,
      "KeyHash": 2847593021,
      "DepthRatio": 0.65,
      "Status": "dominant",
      "Reason": "depth_ratio"
    }
  ],
  "Mitigations": [],
  "ShardPressure": [
    {
      "DiagnosticsEnabled": true,
      "ShardID": 2,
      "Class": "localized_key",
      "PressureRatio": 0.78
    }
  ]
}
```

Raw keys appear in snapshots **only** when `HotKey.ExposeRawKey` is enabled (not recommended in production).

---

## Privacy

Default observability uses **`KeyHash` only** in hot key and mitigation snapshots. Hooks follow the same rule unless `ExposeRawKey` is explicitly enabled.

---

## Optional v0.5 hooks

Require `Observability.EnableHooks` (and snapshot collection for hot key hooks):

| Hook | When it fires |
|------|----------------|
| `OnHotKeyCandidate` | After `DebugSnapshot()` observes a hot key candidate |
| `OnShardPressureSummary` | After `PressureSummary()` completes |
| `OnScaleSignal` | After `ScaleSignal()` with `DiagnosticsEnabled=true` |
| `OnPerKeyAdmissionDecision` | Per-key throttle/reject/shed at admission |

Hooks recover from panics and must not block.

---

## Related docs

- [v0.5-hot-key-autoscaling-signals.md](v0.5-hot-key-autoscaling-signals.md) — reading signals together
- [observability.md](observability.md) — full observability API
- [configuration.md](configuration.md) — enabling v0.5 features
- [debugging.md](debugging.md) — symptom → signal map
