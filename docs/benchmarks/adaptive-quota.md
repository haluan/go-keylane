# Adaptive Quota and Overload Benchmarks

This note explains how to run and interpret v0.4 benchmarks for adaptive quota, overload policy, and related observability paths.

## Commands

```bash
# Adaptive + fixed quota + submit paths (root package)
go test -bench='Benchmark(Fixed|Adaptive|Submit)' -benchmem .

# Overload hot path
go test -bench='Benchmark(Overload|CheckOverload)' -benchmem .

# Core evaluator tick (pure evaluation, no scheduler goroutines)
go test -bench='BenchmarkAdaptiveQuota' -benchmem ./internal/core

# Quota policy snapshot
go test -bench='BenchmarkQuotaSnapshot' -benchmem .

# Full suite filter from KL-1405
go test ./... -bench='Benchmark(Adaptive|Fixed|Overload|Submit)' -benchmem
```

Race coverage:

```bash
go test -race ./...
# or
make ci-race
```

## What to compare

| Benchmark | What it measures |
|-----------|------------------|
| `BenchmarkSubmitWithAdaptiveQuotaDisabled` vs `Enabled` | Submit hot-path overhead when controller is on (long eval interval) |
| `BenchmarkSubmitAdaptiveDisabled` / `BenchmarkSubmitAdaptiveEnabled` | KL-1405 spec aliases for the same submit benchmarks |
| `BenchmarkFixedQuotaCriticalAndBackground` vs `BenchmarkAdaptiveQuotaCriticalAndBackground` | Same workload; adaptive may change quotas over time |
| `BenchmarkOverloadPolicyDecision` / `BenchmarkCheckOverload` | Cost of overload evaluation on admit (keep path) |
| `BenchmarkOverloadBestEffortShedding` | Mixed critical + best-effort overload checks |
| `BenchmarkAdaptiveQuotaDecisionTick` | Evaluator cost per tick (2 lanes); see `*4Lanes`, `*16Lanes`, `*64Lanes` variants |
| `BenchmarkQuotaSnapshot` / `BenchmarkAdaptiveDebugSnapshot` | Diagnostic read cost (bounded copy-out) |

## Metrics that matter

- **ns/op** and **allocs/op** on submit benchmarks — adaptive and overload should not add large allocation churn when hooks are disabled.
- **Throughput** (ops/sec) under mixed critical/background load — workload-dependent.
- **Shed/reject counts** and **quota change counts** — use hooks, `AdaptiveDebugSnapshot`, or `StatsGCPressure` during longer runs, not single benchmark iterations.

## Interpretation

- Results are **workload-dependent**. Adaptive quota is conservative; it is not guaranteed to improve every scenario.
- Compare before/after code changes with the same Go version and `benchstat` (see [benchmarks.md](../benchmarks.md)).
- Snapshot benchmarks intentionally allocate bounded copies; that is acceptable for on-demand diagnostics.

## Related docs

- [adaptive-quota.md](../adaptive-quota.md) — configuration and hooks
- [adaptive-tuning.md](../adaptive-tuning.md) — tuning guidance
- [overload-policy.md](../overload-policy.md) — overload semantics
- [observability.md](../observability.md) — hooks and snapshots
