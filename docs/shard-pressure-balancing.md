# Shard Pressure Balancing

> **Status:** KL-1503 (planned) — automated shard pressure **balancing actions** are not implemented yet. KL-1501 diagnostics are available today.

## Diagnostics today (KL-1501)

Use `Queue.DebugSnapshot()` to distinguish concentration patterns before changing quotas, shards, or admission policy.

| Pattern | Hot shard? | Hot lane? | Hot key candidate? | Typical cause |
|---------|------------|-----------|-------------------|---------------|
| **Hot key** | Often one shard | May be one lane | Yes — one `KeyHash` dominates depth/submit/wait ratios | Single tenant, account, or cache key |
| **Hot shard** | Yes | Mixed lanes | No strong candidate | Shard skew without one dominant key |
| **Hot lane** | Many shards | Yes on `HotLanes` | Mixed | Lane quota or class imbalance |
| **Distributed backlog** | Many shards pressured | Several lanes | No dominant key | Global overload; scale workers or admission |

See [hot-key-mitigation.md](hot-key-mitigation.md) for hot key fields, privacy, and rejection accounting limits.

### Reading snapshots together

```go
snap := q.DebugSnapshot()
// Global: snap.Pressure, snap.HotShards, snap.HotLanes
// Per shard: snap.Shards[i].TotalDepth, HotKeyCandidate, HotKeyCandidates
```

1. If `Pressure.TotalDepthRatio` is high but no hot shard → check lane quotas and worker count.
2. If one shard is in `HotShards` and `HotKeyCandidate` points at one hash → likely hot key (mitigation is KL-1502+).
3. If `HotLanes` names a lane across shards → tune lane policy or adaptive quota before resharding.

`RecordHotKeyReject` increments `RejectedApprox` only for keys already in the bounded tracker (e.g. admission/overload reject after prior submits). Rejects before routing (invalid lane) are not attributed.

## Planned (KL-1503)

Shard pressure balancing will build on KL-1501 candidates and existing hot shard / hot lane signals to suggest or apply cross-shard balancing. Until then:

- [hot-key-tuning.md](hot-key-tuning.md) — detection tuning
- [debugging.md](debugging.md) — symptom tables
- [admission-control.md](admission-control.md) — reject paths that feed hot key reject counters when enabled
