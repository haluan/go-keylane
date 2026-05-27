# Observability contract (production defaults)

This document defines production-safe observability expectations for v0.8. It complements [config-validation.md](config-validation.md) and KL-1803 default inspection.

---

## Principles

- **Low-cardinality labels** on exported metrics (lane, shard, resource name enums—not raw keys).
- **Redacted identifiers** in debug snapshots unless explicitly opted in (`HotKey.ExposeRawKey`).
- **Stable metric and event names** across minors within v0.8 (see [metrics/prometheus](../metrics/prometheus)).
- **Hook panic isolation** — user hooks run through `callHook`; panics do not crash workers.
- **Explicit opt-in** for hot-path timing, hooks, and verbose tracing.

---

## Default behavior

| Area | Production default |
|------|-------------------|
| Unset `Observability` | Resolves to `DefaultObservabilityConfig` at `New` (full visibility); `ValidateConfig` warns with `KL_CONFIG_OBSERVABILITY_FULL_DEFAULTS_RESOLVED` |
| `ProductionDefaults()` | Uses `LowAllocationObservabilityConfig()` — counters/stats on; hot-path hooks and timing off |
| Raw keys in metrics | Not exported by core or prometheus adapter |
| Request / idempotency keys as labels | Not exported |
| Debug snapshots | Pull API (`DebugSnapshot`); not invoked on every submit |
| Autoscaling | Signal export only; no autoscaler control loop |

---

## Avoid by default

- Raw request keys, idempotency keys, or unbounded per-request label values on the scheduler hot path.
- Logging or tracing every queue operation without sampling or explicit configuration.
- Combining `EnableHooks`, `EnableDebugSnapshot`, and hot key tracking without reviewing `KL_CONFIG_HIGH_CARDINALITY_LABEL_RISK`.

When `EnableDebugSnapshot`, `EnableQueueWaitTiming`, and `EnableRunTiming` are all true without `LowAllocationMode`, validation emits `KL_CONFIG_DEBUG_SNAPSHOT_HOT_PATH_HEAVY`.

---

## Recommended production bundle

```go
cfg := keylane.ProductionDefaults() // includes LowAllocationObservabilityConfig
```

Or on a custom config:

```go
cfg.Observability = keylane.LowAllocationObservabilityConfig()
```

---

## Related

- [production-defaults.md](production-defaults.md)
- [compatibility-rules.md](compatibility-rules.md)
- [metrics.md](metrics.md)
- [pipeline-observability.md](pipeline-observability.md)
