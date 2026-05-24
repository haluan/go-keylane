# Per-Key Admission (KL-1502)

KL-1502 adds **targeted admission** for hot keys detected by [KL-1501](hot-key-mitigation.md). One noisy key can be throttled or rejected without punishing unrelated keys on the same shard.

## When to use it

| Symptom | Likely pattern | Action |
|---------|----------------|--------|
| One `KeyHash` dominates shard depth | Hot key | Enable per-key admission |
| Many keys share load on a hot shard | Hot shard / distributed | Tune lane/global admission, not per-key reject-all |
| Global pressure high, no dominant key | Distributed backlog | Overload / lane admission |

## Policy precedence

```text
1. Shutdown / context cancel
2. Global overload policy
3. Lane admission policy (when AdmissionEnabled or SubmitRequest admission is on)
4. Per-key admission policy
5. Enqueue
```

Global overload can still reject everyone. Per-key policy only affects the matching `key_hash` in the bounded tracker.

`Job.Submit` / `TrySubmit` run lane admission when `Config.AdmissionEnabled` is true (same ordering as `SubmitRequest` with admission). Use `CheckAdmission` on custom paths.

## Configuration

Requires hot key tracking:

```go
HotKey: keylane.DefaultHotKeyConfig(),
PerKeyAdmission: keylane.DefaultPerKeyAdmissionConfig(),
```

Zero `PerKeyAdmissionConfig{}` disables per-key mitigation (backward compatible).

| Field | Role |
|-------|------|
| `Enabled` | Master switch (requires `HotKey` with `MaxTrackedKeysPerShard > 0`) |
| `MinStatus` | `candidate` or `dominant` — minimum detection strength to apply policy |
| `DefaultAction` | `throttle`, `reject`, or `shed` (shed only when explicitly set) |
| `MaxQueuedPerKey` | Cap queued jobs per tracked key (0 = off) |
| `MaxInflightPerKey` | Cap in-flight jobs per tracked key (0 = off) |
| `PressureRatioThreshold` | Minimum depth/submit concentration ratio to act |
| `RejectRatioThreshold` | Reject when `rejected/submitted` exceeds ratio |
| `Cooldown` / `RecoveryWindow` | Reduce allow/reject flapping |
| `MaxSnapshotsPerShard` | Cap mitigation rows per shard in `DebugSnapshot` (default 5) |
| `MaxSnapshotsTotal` | Cap total mitigation rows across all shards (default 25) |

### MaxInflightPerKey

The limit compares against **`inflightApprox` after dequeue** (workers running jobs), not submit-time pending depth. Use it for back-pressure while work executes; pair with `MaxQueuedPerKey` for queue buildup.

### Recovery and cooldown

After mitigation, a key re-allows only when **both** `recoveryUntil` has elapsed **and** hot-key status is `none` again. Mid-cooldown submits may still see the last action via `CooldownActive`.

### Hot-path config

`CheckPerKeyAdmission` on a queue uses normalized config cached at `New` when the argument matches `Config.PerKeyAdmission` (no re-validate on the hot path). Request-level overrides in `SubmitRequest` still normalize and validate when they differ.

### Reject accounting

`throttle`, `reject`, and `shed` all increment the tracker `RejectedApprox` (feeds `RejectRatioThreshold` and debug snapshots). Allows set `HotKeyStatus` to `none` on the decision.

## Actions

| Action | Behavior |
|--------|----------|
| `allow` | Enqueue normally |
| `throttle` | Reject with `ErrPerKeyAdmissionThrottled` (retryable; includes `RetryAfter` when configured) |
| `reject` | Reject with `ErrPerKeyAdmissionRejected`; existing queued work continues |
| `shed` | Reject with `ErrPerKeyAdmissionShed` (explicit opt-in) |

## Errors

```go
err := q.Submit(ctx, job)
if errors.Is(err, keylane.ErrPerKeyAdmissionThrottled) { /* backoff */ }
if errors.Is(err, keylane.ErrPerKeyAdmissionRejected) { /* drop or alternate path */ }

var pkErr keylane.PerKeyAdmissionError
if errors.As(err, &pkErr) {
    _ = pkErr.Decision.KeyHash
    _ = pkErr.Decision.Reason
}
```

`CheckPerKeyAdmission` is also available for custom submit paths (same as `CheckAdmission`).

## Debug snapshot

`Queue.DebugSnapshot().PerKeyAdmissionSnapshots` lists active mitigation entries (bounded, `key_hash` only — no raw keys by default).

Use together with `Shards[].HotKeyCandidate` and [shard-pressure-balancing.md](shard-pressure-balancing.md).

## Hooks

```go
Observability: keylane.ObservabilityConfig{
    EnableHooks: true,
    Hooks: keylane.Hooks{
        OnPerKeyAdmissionDecision: func(e keylane.PerKeyAdmissionDecisionEvent) {
            // e.Decision.KeyHash, Action, Reason
        },
    },
},
```

Hooks are optional and must not block; they receive no raw key unless you enable `HotKey.ExposeRawKey` for detection snapshots only.

## Benchmarks

```bash
go test -bench='PerKey|per_key' -benchmem .
go test -bench='PerKey|per_key' -benchmem ./internal/core
```

Scenarios include cold keys only, one dominant hot key, many unique keys beyond `MaxTrackedKeysPerShard`, throttle/reject check paths, and `DebugSnapshot` with active mitigation entries.

## Related

- [hot-key-mitigation.md](hot-key-mitigation.md) — detection (KL-1501)
- [hot-key-tuning.md](hot-key-tuning.md) — ratio tuning
- [admission-control.md](admission-control.md) — lane admission
- [autoscaling-signals.md](autoscaling-signals.md) — hot key vs hot shard vs distributed backlog (KL-1504 stub)
