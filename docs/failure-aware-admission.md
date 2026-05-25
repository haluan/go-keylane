# Failure-Aware Admission and Retry

Part of [v0.6.0 — Retry, Deadline & Failure Policy](v0.6-retry-deadline-failure-policy.md).

Admission control rejects **new** work before enqueue. [Retry suppression](retry-suppression.md) stops **in-worker** retries when the runtime is already unhealthy. They complement each other.

---

## How pieces fit together

```text
                    ┌─────────────────┐
   Submit ─────────►│ Lane admission  │──reject──► FailureKind: rejected
                    └────────┬────────┘
                             │ enqueue
                    ┌────────▼────────┐
                    │ Queue / shard   │◄── hot key tracker
                    │ depth pressure  │
                    └────────┬────────┘
                             │ worker runs handler
                    ┌────────▼────────┐
                    │ Handler error   │──► FailureKind classification
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
         DecideRetry   DecideRetrySafety  DecideRetrySuppression
              │              │              │
         retry sleep    duplicate-safe?  pressure / hot key / scale?
```

| Mechanism | Protects against |
|-----------|------------------|
| [Admission control](admission-control.md) | Admitting new work when queue is full |
| [Overload policy](overload-policy.md) | Shedding/degrading under global overload |
| [Per-key admission](per-key-admission-policy.md) | One key dominating a shard |
| Retry suppression | Workers retrying into a full or hot queue |
| [Scale signal](autoscaling-signals.md) | Platform missing hidden demand (v0.5) |

---

## Why overload / admission failures should not retry

When Keylane returns `overloaded` or `rejected`, the system is already signaling backpressure. In-process retry would:

- Hold workers longer
- Increase queue depth and wait time
- Encourage external clients to retry simultaneously (retry storm)

With suppression enabled, these failure kinds are not retried in-worker even if marked retryable in a custom classifier.

---

## Production scenario: DB slowdown

```text
DB slows down
  ↓
handlers fail transiently (retryable classification)
  ↓
clients retry at the edge
  ↓
Keylane queues fill (pressure rises)
  ↓
retry suppression prevents internal amplification on best-effort lanes
  ↓
ScaleSignal recommends scale-out (v0.5)
  ↓
operators scale replicas OR tune admission / per-key mitigation
```

Critical lanes may still receive **bounded** in-process retries under pressure (not under global overload). Best-effort and background lanes are suppressed first.

---

## Critical vs best-effort lanes

```go
_, _ = q.UpdateAdmissionPolicy(keylane.AdmissionPolicy{
    DefaultClass:            keylane.LaneBestEffort,
    DefaultRejectAboveRatio: 0.90,
    Lanes: []keylane.LanePolicy{
        {Lane: "critical", Class: keylane.LaneCritical, RejectAboveRatio: 0.98},
    },
})
```

Under `SuppressNonCriticalWhenPressured`, a `default` (best-effort) job gets `global_pressure` suppression while a `critical` lane job may still schedule retries — see integration tests and [retry-suppression.md](retry-suppression.md).

---

## Hot key and scale-out interaction

- **Hot key** (v0.5): tracker marks candidates; suppression blocks retries on non-critical lanes (`hot_key` reason).
- **Scale-out recommended**: suppression can block non-critical retries while autoscaler catches up.
- Hot-key **admission** (throttle/reject) affects **new** submits; suppression affects **retries** on already-running work.

---

## vs durable exactly-once

Retry suppression is **in-process and best-effort**. It does not replace idempotency keys, outbox patterns, or external deduplication. See [idempotency.md](idempotency.md).

---

## Related

- [v0.6.0 overview](v0.6-retry-deadline-failure-policy.md)
- [retry-suppression.md](retry-suppression.md)
- [admission-control.md](admission-control.md)
- [per-key-admission-policy.md](per-key-admission-policy.md)
- [v0.5-hot-key-autoscaling-signals.md](v0.5-hot-key-autoscaling-signals.md)
- [lane-priority.md](lane-priority.md)
- [failure-policy.md](failure-policy.md)
