# Hot Key Detection

v0.5.0 adds **bounded hot key accounting and candidate detection**. Targeted throttle/reject/shed is implemented in [per-key-admission-policy.md](per-key-admission-policy.md).

## What is a hot key?

A **hot key** is a logical identity (for example `tenant:demo-7`, `account:42`) that concentrates submissions on one shard. Keylane already exposes:

| Signal | Answers |
|--------|---------|
| Hot shard | Which shard has high depth? |
| Hot lane | Which lane is backlogged? |
| Pressure | Is total queue capacity filling? |

Hot key detection adds:

| Signal | Answers |
|--------|---------|
| Hot key **candidate** | Which **key hash** dominates depth or submissions on a shard? |

A candidate is **approximate diagnostic signal**, not confirmed root cause. Treat it as a hint for investigation and mitigation — not billing or enforcement without your own validation.

---

## Hot key vs hot shard vs distributed backlog

| Pattern | Shard depth | Key concentration |
|---------|-------------|-------------------|
| Hot key | One shard hot | One key hash dominates that shard |
| Hot shard (many keys) | One shard hot | Many keys share load |
| Distributed backlog | Many shards pressured | No single dominant key |

Use `DebugSnapshot().HotKeys` (flat list) or `DebugSnapshot().Shards[].HotKeyCandidates` together with `PressureSummary`, `HotShards`, and `HotLanes`.

---

## Why tracking must be bounded

Tracking every unique key in an unbounded map would:

- Grow with tenant cardinality
- Increase allocation and GC work on the submit path
- Worsen tail latency under load

v0.5.0 uses a **fixed-size tracker per shard** (`MaxTrackedKeysPerShard`). Keys beyond the cap evict by LRU. Counts are **approximate** and intended for diagnostics.

---

## Why raw keys must not be exposed by default

Raw keys may contain PII or tenant identifiers. Default observability uses **`KeyHash` only** in snapshots, hooks, and Prometheus metrics.

Enable `HotKey.ExposeRawKey` only after a data-classification review. Prefer correlating hashes in your own systems.

---

## How candidate status is determined

Candidates are flagged when a tracked key hash exceeds ratio thresholds within the detection window:

| Field | Role |
|-------|------|
| `HotKeyDepthRatio` | Key queued depth share ≥ ratio |
| `HotKeyWaitRatio` | Key queue-wait share ≥ ratio |
| `DetectionWindow` | Decay window; stale entries expire |

Status values: `candidate` or `dominant` (approximate). Reason: `depth_ratio`, `submit_ratio`, `wait_ratio`, or combined `depth_and_submit_ratio`.

### Wait ratio visibility

`WaitRatio` and shard-pressure `WaitContributionRatio` use cumulative shard queue wait as the denominator. They are often **zero** when:

- The queue is not started (jobs only enqueue), or
- Workers drain quickly and cumulative wait has not accumulated.

For meaningful wait ratios in tests or dashboards, use a **blocked-worker** scenario or poll after sustained backlog.

---

## False positives and limits

Expect occasional false positives:

| Cause | What you see | What to do |
|-------|--------------|------------|
| LRU eviction | Key briefly spikes then disappears from tracker | Confirm sustained pattern over multiple windows |
| Cap overflow | Noisy keys beyond `MaxTrackedKeysPerShard` evict quieter keys | Increase cap cautiously; do not unbound |
| Short burst | Candidate during traffic spike, clears quickly | Distinguish burst vs sustained overload |
| Co-located keys | Hash collision is rare; skew is usually real concentration | Correlate with your key routing |

**Candidate ≠ confirmed root cause.** Cross-check with `PressureSummary.MitigationRelevant`, per-shard depth, and application metrics before aggressive reject policies.

---

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

See [configuration.md](configuration.md) for full field reference.

---

## Reading snapshots

```go
snap := q.DebugSnapshot()
for _, hk := range snap.HotKeys {
    log.Printf("shard=%d key_hash=%x depth_ratio=%.2f status=%s",
        hk.ShardID, hk.KeyHash, hk.DepthRatio, hk.Status)
}
```

`HotKeyCandidates` on each shard lists additional ranked candidates (up to `MaxCandidatesPerSnapshot`).

Public snapshot shape (hash-only by default):

```go
type HotKeyCandidateSnapshot struct {
    ShardID          int
    LaneID           uint16
    KeyHash          uint64
    DepthRatio       float64
    WaitRatio        float64
    Status           HotKeyStatus
    Reason           string
    LastSeenUnixNano int64
}
```

---

## Observe-only mode (detection without mitigation)

To **observe** pressure without throttling or rejecting:

- Enable `HotKey` with defaults, and
- Leave `PerKeyAdmission` disabled (`PerKeyAdmissionConfig{}`), or set `DefaultAction: PerKeyMitigationAllow`

This records candidates in snapshots and metrics without admission changes.

---

## Disabling

Set `HotKey: HotKeyConfig{}` or `HotKey.Enabled: false`. No tracker memory is used and the submit path skips accounting.

---

## Related docs

- [per-key-admission-policy.md](per-key-admission-policy.md) — mitigation
- [hot-key-tuning.md](hot-key-tuning.md) — ratio and capacity tuning
- [shard-pressure-diagnostics.md](shard-pressure-diagnostics.md) — pressure classes
- [debug-snapshot.md](debug-snapshot.md) — snapshot fields
- [debugging.md](debugging.md) — symptom table
