# Failure-Aware Admission and Retry (KL-1604)

Admission control rejects **new** work before enqueue. Retry suppression stops **in-worker** retries when the runtime is already unhealthy. They complement each other.

---

## Why retry suppression exists

A retryable handler error during overload can still trigger backoff retries inside workers. That consumes worker time, increases queue wait, and encourages external client retries — a retry storm.

KL-1604 treats these failure kinds as **not candidates for immediate in-process retry** when policy is enabled:

| Failure kind | Default retry when suppression on |
|--------------|-----------------------------------|
| `retryable` | Only if pressure allows |
| `permanent`, `timeout`, `cancelled`, `deadline_exhausted`, `panic`, `unknown` | No (via `DecideRetry`) |
| `overloaded` | No |
| `rejected` (admission) | No |
| `rejected` (per-key throttle/reject/shed) | No |

---

## Lane classes

| Class | Under pressure | Under overload |
|-------|----------------|----------------|
| `critical` | May retry (bounded) | Suppressed |
| `normal` | May retry | Suppressed |
| `background` / `best_effort` | Suppressed | Suppressed |

Hot-key mitigation suppresses background/best-effort retries by default. Critical lanes require `AllowCriticalHotKeyRetry: true` for a single bounded retry when hot-key signals are active.

---

## vs durable exactly-once

Retry suppression is **in-process and best-effort**. It does not replace idempotency keys, outbox patterns, or external deduplication.

---

## Related

- [retry-suppression.md](retry-suppression.md)
- [admission-control.md](admission-control.md)
- [lane-priority.md](lane-priority.md)
