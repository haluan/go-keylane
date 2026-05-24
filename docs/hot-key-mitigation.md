# Hot Key Mitigation (Detection)

KL-1501 adds **bounded hot key accounting and candidate detection**. Targeted throttle/reject/shed is implemented in [per-key-admission.md](per-key-admission.md) (KL-1502).

## What is a hot key?

A **hot key** is a logical identity (`tenant:big-customer`, `account:42`, …) that concentrates submissions on one shard. Keylane already exposes:

| Signal | Answers |
|--------|---------|
| Hot shard | Which shard has high depth? |
| Hot lane | Which lane is backlogged? |
| Pressure | Is total queue capacity filling? |

Hot key detection adds:

| Signal | Answers |
|--------|---------|
| Hot key candidate | Which **key hash** dominates depth or submissions on a shard? |

## Hot key vs hot shard vs distributed backlog

| Pattern | Shard depth | Key concentration |
|---------|-------------|-------------------|
| Hot key | One shard hot | One key hash dominates that shard |
| Hot shard (many keys) | One shard hot | Many keys share load |
| Distributed backlog | Many shards pressured | No single dominant key |

Use `DebugSnapshot().Shards[].HotKeyCandidate` together with `PressureSummary`, `HotShards`, and `HotLanes`.

## Why tracking must be bounded

Tracking every unique key in an unbounded `map[string]…` would:

- Grow with tenant cardinality
- Increase allocation and GC work on the submit path
- Worsen tail latency under load

KL-1501 uses a **fixed-size tracker per shard** (`MaxTrackedKeysPerShard`). Keys beyond the cap evict by LRU. Counts are **approximate** and intended for diagnostics, not billing.

## Configuration

```go
HotKey: keylane.HotKeyConfig{
    Enabled:                true,
    MaxTrackedKeysPerShard: 64,
    DetectionWindow:        30 * time.Second,
    HotKeyDepthRatio:       0.40,
    HotKeyWaitRatio:        0.40,
    ExposeRawKey:           false,
},
```

| Field | Role |
|-------|------|
| `Enabled` | Master switch (`true` in `DefaultHotKeyConfig()`; zero `HotKeyConfig{}` disables) |
| `MaxTrackedKeysPerShard` | Hard cap on tracked key hashes per shard (`0` = enabled no-op, no slots) |
| `DetectionWindow` | Decay window; stale entries expire on observe and at snapshot time (approximate, not a precise histogram) |
| `HotKeyDepthRatio` | Flag candidate when key queued depth share ≥ ratio |
| `HotKeyWaitRatio` | Flag candidate when key queue-wait share ≥ ratio |
| `MaxCandidatesPerSnapshot` | Ranked candidates per shard in `DebugSnapshot` (default 5) |
| `ExposeRawKey` | Include raw key in snapshots (sensitive; off by default) |

## Reading snapshots

```go
snap := q.DebugSnapshot()
for _, sh := range snap.Shards {
    if c := sh.HotKeyCandidate; c != nil {
        log.Printf("shard=%d key_hash=%x depth_ratio=%.2f status=%s",
            sh.ShardID, c.KeyHash, c.DepthRatio, c.Status)
    }
}
```

`HotKeyCandidates` lists additional ranked candidates (up to `MaxCandidatesPerSnapshot`). Status is `candidate` or `dominant` (approximate). `Reason` is one of `depth_ratio`, `submit_ratio`, `wait_ratio`, or combined `depth_and_submit_ratio`.

## Rejection accounting

When hot key tracking is enabled, `CheckAdmission` and `CheckOverload` call `RecordHotKeyReject` for the request key hash after a reject, but only if that hash already occupies a tracker slot (from a prior submit). This does not cover rejects before shard routing (e.g. `ErrInvalidLane`).

## Privacy

Default observability uses **`key_hash` only**. Do not enable `ExposeRawKey` in production unless you have a clear data-classification review. Prefer correlating hashes in your own systems.

## Disabling

Set `HotKey: HotKeyConfig{}` or `HotKey.Enabled: false`. No tracker memory is used and the submit path skips accounting.

## Related docs

- [hot-key-tuning.md](hot-key-tuning.md) — ratio and capacity tuning
- [shard-pressure-balancing.md](shard-pressure-balancing.md) — hot key vs hot shard vs hot lane
- [pressure-diagnostics.md](pressure-diagnostics.md) — KL-1503 pressure classes and API
- [debugging.md](debugging.md) — symptom table
