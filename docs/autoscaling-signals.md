# Autoscaling Signals (KL-1504)

> **Status:** KL-1504 (planned) — automated scale-out/in rules are not implemented in go-keylane. This document describes how to **read** existing diagnostics so autoscaling hooks do not amplify the wrong problem.

## Three backlog shapes

| Pattern | Snapshot signals | Scale-out helps? |
|---------|------------------|------------------|
| **Localized hot key** | One shard pressured; `HotKeyCandidate` / `PerKeyAdmissionSnapshots` show one dominant `KeyHash` | Often **no** for a single-key storm — more replicas may still hit the same hash routing |
| **Hot shard** | `HotShards` lists one shard; mixed keys, no dominant candidate | Sometimes — if work can be spread by resharding or more workers per shard |
| **Distributed backlog** | Many shards in `HotShards`; several `HotLanes`; no strong per-key candidate | **Yes** — global capacity or admission tuning is the lever |

Use [per-key-admission.md](per-key-admission.md) for targeted mitigation and [shard-pressure-balancing.md](shard-pressure-balancing.md) for shard vs lane vs key interpretation.

## What to export

From `Queue.DebugSnapshot()` (poll on your metrics interval):

- `Pressure.TotalDepthRatio` — global queue pressure
- `HotShards`, `HotLanes` — concentration at shard/lane level
- `Shards[].HotKeyCandidate` — KL-1501 detection (ratios, `RejectedApprox`)
- `PerKeyAdmissionSnapshots` — active per-key throttle/reject/shed (bounded; see `MaxSnapshotsTotal`)

From `StatsGCPressure()`:

- Per-lane accepted/rejected/admission/overload counters

## Autoscaling pitfalls

1. **Scaling replicas on a hot key** can increase duplicate work and cache churn without lowering per-key depth. Prefer per-key admission, key splitting, or routing changes first.
2. **Ignoring lane class** — `HotLanes` may mean best-effort is saturated while critical is fine; scaling workers alone does not fix lane policy.
3. **Treating throttle as success** — per-key throttle increments `RejectedApprox` on the tracker (same as reject for ratio escalation). High reject ratio may tighten policy via `RejectRatioThreshold`.

## Related

- [hot-key-tuning.md](hot-key-tuning.md) — detection thresholds
- [per-key-admission.md](per-key-admission.md) — KL-1502 mitigation
- [debugging.md](debugging.md) — symptom tables
- [production-tuning.md](production-tuning.md) — worker and queue sizing
