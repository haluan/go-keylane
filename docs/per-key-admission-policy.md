# Per-Key Admission Policy

v0.5.0 adds **targeted admission** for hot keys detected by [hot key detection](hot-key-detection.md). One noisy key can be throttled or rejected without punishing unrelated keys on the same shard.

---

## What per-key admission means

Lane-level and global admission apply to **all** traffic on a lane or queue. Per-key admission applies only to a **specific key hash** already tracked by the bounded hot key tracker.

Mitigation protects the rest of the queue: unrelated keys on the same shard continue to enqueue normally.

---

## When to use it

| Symptom | Likely pattern | Action |
|---------|----------------|--------|
| One `KeyHash` dominates shard depth | Hot key | Enable per-key admission |
| Many keys share load on a hot shard | Hot shard / distributed | Tune lane/global admission |
| Global pressure high, no dominant key | Distributed backlog | Overload / lane admission |

---

## Policy actions

The public API uses precise action names. Map from common operator terms:

| Operator term | API constant | Behavior |
|---------------|--------------|----------|
| **observe** | Detection on + `PerKeyMitigationAllow` or admission disabled | Records pressure only; no admission change |
| **throttle** | `PerKeyMitigationThrottle` | Reject with retryable error; includes `RetryAfter` when configured |
| **shed** | `PerKeyMitigationShed` | Drop selected work under pressure (explicit opt-in) |
| **reject** | `PerKeyMitigationReject` | Explicit admission failure; queued work for that key continues |

```go
// observe-like: detection without mitigation
HotKey: keylane.DefaultHotKeyConfig(),
PerKeyAdmission: keylane.PerKeyAdmissionConfig{Enabled: false},

// or throttle by default (recommended starting point)
PerKeyAdmission: keylane.DefaultPerKeyAdmissionConfig(),
```

---

## Policy precedence

```text
1. Shutdown / context cancel
2. Global overload policy
3. Lane admission policy (when AdmissionEnabled or SubmitRequest admission is on)
4. Per-key admission policy
5. Enqueue
```

Global overload can still reject everyone. Per-key policy only affects matching `key_hash` entries in the bounded tracker.

---

## Configuration

Requires hot key tracking with `MaxTrackedKeysPerShard > 0`:

```go
HotKey: keylane.DefaultHotKeyConfig(),
PerKeyAdmission: keylane.DefaultPerKeyAdmissionConfig(),
```

Zero `PerKeyAdmissionConfig{}` disables per-key mitigation (backward compatible).

| Field | Role |
|-------|------|
| `Enabled` | Master switch |
| `MinStatus` | `candidate` or `dominant` — minimum detection strength to act |
| `DefaultAction` | `throttle`, `reject`, or `shed` |
| `MaxQueuedPerKey` | Cap queued jobs per tracked key (0 = off) |
| `MaxInflightPerKey` | Cap in-flight jobs per tracked key (0 = off) |
| `PressureRatioThreshold` | Minimum depth/submit concentration to act |
| `RejectRatioThreshold` | Reject when `rejected/submitted` exceeds ratio |
| `Cooldown` / `RecoveryWindow` | Reduce allow/reject flapping |
| `MaxSnapshotsPerShard` | Cap mitigation rows per shard in `DebugSnapshot` |
| `MaxSnapshotsTotal` | Cap total mitigation rows globally |

See [configuration.md](configuration.md) for safe defaults.

### Avoid punishing normal traffic

- Start with `throttle` and conservative ratio thresholds
- Require `MinStatus: dominant` before `reject` in production
- Use `Cooldown` / `RecoveryWindow` to prevent flapping
- Do not enable `shed` unless you explicitly accept dropped work

---

## Errors

```go
err := q.Submit(ctx, job)
if errors.Is(err, keylane.ErrPerKeyAdmissionThrottled) { /* backoff */ }
if errors.Is(err, keylane.ErrPerKeyAdmissionRejected) { /* alternate path */ }

var pkErr keylane.PerKeyAdmissionError
if errors.As(err, &pkErr) {
    _ = pkErr.Decision.KeyHash
    _ = pkErr.Decision.Reason
}
```

---

## Debug snapshot and hooks

`DebugSnapshot().PerKeyAdmissionSnapshots` and `Mitigations` list active mitigation entries (`key_hash` only by default).

```go
Observability: keylane.ObservabilityConfig{
    EnableHooks: true,
    Hooks: keylane.Hooks{
        OnPerKeyAdmissionDecision: func(e keylane.PerKeyAdmissionDecisionEvent) {
            // e.Decision.KeyHash, Action, Reason — no raw key by default
        },
    },
},
```

Hooks recover from panics and must not block the submit path.

---

## Related docs

- [hot-key-detection.md](hot-key-detection.md) — detection
- [admission-control.md](admission-control.md) — lane admission
- [autoscaling-signals.md](autoscaling-signals.md) — when to scale vs mitigate
- [debug-snapshot.md](debug-snapshot.md) — snapshot fields
