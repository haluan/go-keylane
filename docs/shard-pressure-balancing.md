# Shard Pressure Balancing

KL-1503 provides **diagnostic-first** shard pressure balancing: explain whether pressure is localized, lane-dominated, shard-skewed, distributed, or worker-bound—without automatic rebalancing.

See [pressure-diagnostics.md](pressure-diagnostics.md) for API and configuration.

## Pattern table

| Pattern | Pressure class | Signals | Typical response |
|---------|----------------|---------|------------------|
| **Hot key** | `localized_key` | `HotKeyCandidates`, high contribution ratio | Per-key admission (KL-1502) |
| **Hot lane** | `lane_dominant` | `DominantLane`, `LaneBreakdown` | Lane quota / admission tuning |
| **Hot shard** | `shard_hot` | High `SkewRatio`, one shard in `HotShards` | Resharding or key spread |
| **Distributed backlog** | `distributed` | Many shards hot, `ScaleRelevant` | Scale workers / global admission |
| **Worker saturation** | `worker_bound` | High `WorkerBusyRatio`, rising wait | More workers or shorter jobs |

## APIs

```go
summary := q.PressureSummary()
hot := q.HotShards()
shard, ok := q.ShardPressure(shardID)

// Reuse backing array when polling hot shards:
hot = q.AppendHotShards(hot[:0])

snap := q.DebugSnapshot()
if snap.PressureSummary.DiagnosticsEnabled {
    _ = snap.PressureSummary.Class
}
_ = snap.Shards[i].ShardPressure.DiagnosticsEnabled
```

Legacy depth rankings remain on `snap.HotShards` and `snap.HotLanes` for backward compatibility.

## Reading snapshots together

```go
snap := q.DebugSnapshot()
// Coarse depth: snap.Pressure
// KL-1503: snap.PressureSummary
// Per shard: snap.Shards[i].ShardPressure, HotKeyCandidate
// Per-key mitigation: snap.PerKeyAdmissionSnapshots
```

1. If `PressureSummary.ScaleRelevant` → consider capacity (see [autoscaling-signals.md](autoscaling-signals.md)).
2. If `PressureSummary.MitigationRelevant` → inspect `HotKeyCandidates` and enable per-key admission.
3. If `DominantLane` is set → tune lane policy before resharding.

## Not in scope (KL-1503)

- Automatic key migration or shard splitting
- Dynamic shard count changes
- Cross-replica coordination

## Related

- [pressure-diagnostics.md](pressure-diagnostics.md) — KL-1503 guide
- [hot-key-tuning.md](hot-key-tuning.md) — detection tuning
- [autoscaling-signals.md](autoscaling-signals.md) — scale vs mitigate (KL-1504 stub)
- [debugging.md](debugging.md) — symptom tables
- [v0.5-runtime-signals.md](v0.5-runtime-signals.md) — milestone overview
