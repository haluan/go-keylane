# Pressure Diagnostics

Adds **shard pressure balancing diagnostics**: bounded snapshots that explain whether pressure is localized to one key, concentrated in one lane, skewed to one shard, distributed globally, or worker-bound.

Diagnostics are **read-only**. They do not move jobs, change hashing, or rebalance shards.

## Quick API

```go
summary := q.PressureSummary()
shard, ok := q.ShardPressure(0)
hot := q.HotShards()

snap := q.DebugSnapshot()
_ = snap.PressureSummary
_ = snap.Shards[0].ShardPressure
```

Coarse depth signal for admission remains on `Queue.Pressure()` and `DebugSnapshot.Pressure` (unchanged).

## Configuration

```go
ShardPressure: keylane.DefaultShardPressureConfig(),
```

Zero value disables rich snapshots (`DiagnosticsEnabled: false`; no pressure class). `Queue.Pressure()` still works.

| Field | Role |
|-------|------|
| `Enabled` | Master switch for rich pressure snapshots |
| `Window` | Accounting window for queue-wait normalization |
| `HotShardPressureRatio` | Shard considered hot at or above this pressure ratio |
| `DominantLaneRatio` | Lane contribution threshold for `lane_dominant` |
| `LocalizedHotKeyRatio` | Hot key contribution threshold for `localized_key` |
| `DistributedShardRatio` | Fraction of hot shards for global `distributed` |
| `WorkerBusyRatio` | In-flight / worker threshold for `worker_bound` |
| `MaxHotShards` | Cap hot shard entries in summary |
| `MaxLaneBreakdownPerShard` | Cap lane rows per shard |
| `MaxHotKeyCandidatesPerShard` | Cap hot key rows per shard |

## Pressure classes

| Class | Meaning |
|-------|---------|
| `healthy` | Below hot thresholds |
| `localized_key` | Bounded hot key explains most shard pressure |
| `lane_dominant` | One lane dominates shard depth (no dominant key) |
| `shard_hot` | Shard at or above hot threshold (per-shard view); high skew vs peers reinforces this class |
| `distributed` | Many shards hot at once |
| `worker_bound` | Workers busy, wait rising, low shard skew |
| `unknown` | Enabled but insufficient data (e.g. zero shards) |

When diagnostics are disabled, `PressureSummary()` returns `{DiagnosticsEnabled: false, UpdatedAt: …}` with an empty `Class` — not `unknown`. **Always check `DiagnosticsEnabled` before reading `Class`.**

Per-shard `ShardPressure(shardID)` uses the same contract: when diagnostics are disabled but the shard ID is valid, it returns `(ShardPressureSnapshot{DiagnosticsEnabled: false, ShardID: shardID}, true)`. Invalid shard IDs return `(zero, false)`.

## Source file layout

The spec uses generic `pressure_*` names; this repo uses `shard_pressure_*`:

| Spec name | Actual file |
|-----------|-------------|
| `pressure.go` | [`shard_pressure.go`](../shard_pressure.go) |
| `pressure_config.go` | config section of [`shard_pressure.go`](../shard_pressure.go) |
| `pressure_snapshot.go` | [`internal/core/shard_pressure_snapshot.go`](../internal/core/shard_pressure_snapshot.go) |
| `pressure_classification.go` | [`internal/core/shard_pressure_classification.go`](../internal/core/shard_pressure_classification.go) |
| `pressure_test.go` | [`shard_pressure_test.go`](../shard_pressure_test.go) |
| `pressure_benchmark_test.go` | [`shard_pressure_bench_test.go`](../shard_pressure_bench_test.go) |

## Pressure ratio formula

Per-shard `PressureRatio` is the maximum of:

- queue depth ratio (queued / capacity)
- queue wait ratio (normalized cumulative wait vs window × workers)
- admission pressure ratio (rejects + throttles + sheds / submitted, attributed from **global lane counters** proportionally by shard lane depth share)
- worker contribution ratio (in-flight / worker count, capped at 1)

Per-shard lane fields such as `CompletedApprox`, `RejectedApprox`, `QueueWaitApproxNanos`, and `InflightJobs` use the same depth-proportional attribution until per-shard lane counters exist. Treat them as estimates for diagnostics, not exact accounting.

Documented and deterministic; conservative by design for v0.5.

## Scale vs mitigation flags

`PressureSummarySnapshot` includes:

- **`ScaleRelevant`** — many hot shards, high global depth/wait, or worker saturation (scale-out may help)
- **`MitigationRelevant`** — localized hot-key pressure (per-key admission / key splitting may help)

See [autoscaling-signals.md](autoscaling-signals.md) for interpretation.

## Privacy

Hot key rows use **`key_hash` only** in pressure snapshots. Raw keys are never included even when `HotKey.ExposeRawKey` is enabled for detection snapshots.

## Integration with v0.5.0

— `HotKeyCandidates` in shard pressure; depth/wait/submit ratios
— `ActiveMitigation` and `MitigationReason` on hot key rows (from tracker + per-key snapshots)

## Related

- [shard-pressure-balancing.md](shard-pressure-balancing.md) — pattern table
- [hot-key-detection.md](hot-key-detection.md) — detection
- [per-key-admission-policy.md](per-key-admission-policy.md) — mitigation
- [v0.5-hot-key-autoscaling-signals.md](v0.5-hot-key-autoscaling-signals.md) — milestone overview
- [debugging.md](debugging.md) — symptom tables

## Benchmarks

```bash
go test -bench=Pressure -benchmem .
go test -bench=Pressure -benchmem ./internal/core
```
