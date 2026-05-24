# Shard pressure diagnostics (KL-1503 / KL-1505)

This document is the spec-listed entry point for shard pressure observability. Detailed balancing behavior and tuning live in companion docs.

## Related documentation

- [shard-pressure-balancing.md](shard-pressure-balancing.md) — shard pressure classes, hot shard ranking, tuning
- [pressure-diagnostics.md](pressure-diagnostics.md) — `PressureSummary`, per-shard snapshots, classification
- [observability.md](observability.md) — v0.5 `DebugSnapshot`, hooks, and metrics

## Pull APIs

| API | Use when |
|-----|----------|
| `Queue.PressureSummary()` | Global class (`distributed`, `localized_key`, `lane_dominant`, …), hot shard list |
| `Queue.ShardPressure(shardID)` | One shard's lane breakdown and hot key pressure snapshots |
| `Queue.DebugSnapshot().ShardPressure` | Flat slice of per-shard `ShardPressureSnapshot` (KL-1505) |

## Prometheus

- `keylane_shard_pressure_ratio` — global composite ratio
- `keylane_shard_depth` / `keylane_shard_queue_depth` — per-shard queued depth (same value; spec alias)
