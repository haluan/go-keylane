# Adaptive Quota and Overload Benchmarks

## Overview

This note explains how to run and interpret v0.4 benchmarks for adaptive quota, overload policy, and related observability paths. Results are **workload-dependent**; use them to detect regressions in your environment, not as universal latency guarantees.

---

## Benchmark scenarios

| Area | Benchmarks (examples) |
|------|------------------------|
| Submit hot path | `BenchmarkSubmitWithAdaptiveQuotaDisabled` vs `Enabled`; aliases `BenchmarkSubmitAdaptiveDisabled` / `Enabled` |
| Fixed vs adaptive workload | `BenchmarkFixedQuotaCriticalAndBackground` vs `BenchmarkAdaptiveQuotaCriticalAndBackground` |
| Best-effort shedding | `BenchmarkOverloadBestEffortShedding` |
| Overload decision hot path | `BenchmarkOverloadPolicyDecision`, `BenchmarkCheckOverload` |
| Adaptive controller tick | `BenchmarkAdaptiveQuotaDecisionTick` (+ `*4Lanes`, `*16Lanes`, `*64Lanes`) |
| Quota / debug snapshot | `BenchmarkQuotaSnapshot`, `BenchmarkAdaptiveDebugSnapshot` |

---

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

# Full suite filter from v0.5.0
go test ./... -bench='Benchmark(Adaptive|Fixed|Overload|Submit)' -benchmem
```

Race coverage:

```bash
go test -race ./...
# or
make ci-race
```

---

## Fixed quota vs adaptive quota

Compare `BenchmarkFixedQuotaCriticalAndBackground` with `BenchmarkAdaptiveQuotaCriticalAndBackground` under the same load generator. Adaptive may change quotas over the run; fixed quotas stay constant. Adaptive is **not guaranteed** to improve throughput or latency in every scenario.

---

## Best-effort shedding

`BenchmarkOverloadBestEffortShedding` exercises mixed critical and best-effort overload checks. Use with overload counters or hooks to verify shed paths stay cheap when hooks are disabled.

---

## Overload decision hot path

Target **0 allocs/op** on the successful `keep` path when hooks are disabled. Any allocation regression on admit may matter because overload runs before enqueue.

---

## Adaptive controller tick

`BenchmarkAdaptiveQuotaDecisionTick` measures evaluator cost per tick (no scheduler goroutines in the core package benchmark). Scale variants show cost vs lane count. Controller tick overhead should stay bounded as you add lanes.

---

## Submit hot path

Compare submit benchmarks with adaptive enabled vs disabled (long `EvaluationInterval` in tests minimizes tick interference). **Allocation regressions** on submit matter more than small ns/op shifts when hooks are off.

---

## Reading allocations/op

- **allocs/op** on submit and overload `keep` paths should stay at or near zero with hooks disabled
- Snapshot benchmarks **intentionally allocate** bounded copies — acceptable for on-demand diagnostics, not per-request paths

---

## Reading p95/p99 queue wait

Public `StatsGCPressure()` exposes cumulative queue-wait and run timing (averages and max fields depending on config). Per-request **p95/p99** appear on `AdaptiveQuotaDecisionEvent` during evaluation when timing is enabled — use hooks or longer runs, not a single `-bench` iteration, to study tail latency.

---

## Comparing benchmark runs

- Use the same Go version and machine
- Compare before/after with `benchstat` — see [benchmarks.md](../benchmarks.md)
- Keep hooks disabled unless measuring observability overhead explicitly
- Record `QuotaChangeTotal` / overload counters from `AdaptiveDebugSnapshot` or `StatsGCPressure` during longer runs

---

## Limitations

- Benchmark results are **workload-dependent**
- Adaptive quota is **not guaranteed** to win every benchmark or improve p99
- **Allocation regressions** in the submit path are important even when ns/op looks fine
- Controller tick overhead should remain bounded; snapshot reads are for diagnostics, not hot paths
- Overload decision cost must stay low enough for pre-enqueue use on every request
- Keylane does not eliminate Go GC pauses; benchmarks do not measure GC elimination

---

## Related docs

- [adaptive-quota.md](../adaptive-quota.md) — configuration
- [adaptive-tuning.md](../adaptive-tuning.md) — tuning and rollout
- [overload-policy.md](../overload-policy.md) — overload semantics
- [adaptive-observability.md](../adaptive-observability.md) — hooks and snapshots
- [observability.md](../observability.md) — general observability
