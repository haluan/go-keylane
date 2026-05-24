# Hot Key Tuning

Tuning guide for KL-1501 bounded hot key detection. See [hot-key-mitigation.md](hot-key-mitigation.md) for concepts.

## Recommended starting point

```go
HotKey: keylane.DefaultHotKeyConfig(), // Enabled: true (recommended production default)
```

Disable explicitly when not needed:

```go
HotKey: keylane.HotKeyConfig{Enabled: false},
```

Minimal enable (normalization fills ratios and window):

```go
HotKey: keylane.HotKeyConfig{Enabled: true, MaxTrackedKeysPerShard: 64},
```

## MaxTrackedKeysPerShard

- Higher values remember more key hashes before eviction (more CPU on eviction scan, still bounded).
- Lower values (16–32) suit memory-sensitive deployments.
- Must not be confused with unique key cardinality in your product — most keys are **not** tracked, only the busiest candidates.

## HotKeyDepthRatio / HotKeyWaitRatio

- **Lower ratios** (0.25–0.35) surface candidates earlier; more alerts, possible false positives when load is evenly spread.
- **Higher ratios** (0.5+) only flag strong concentration; may miss emerging hot keys.
- Use **depth ratio** when queue buildup is the symptom; **wait ratio** when queue wait dominates on a shard.

## DetectionWindow

Shorter windows react faster to bursts; longer windows smooth spikes. Align with how often you poll `DebugSnapshot()` (e.g. 10–30s scrape interval).

## MaxCandidatesPerSnapshot

Default `5`. Lower for smaller snapshots; raise only up to `MaxTrackedKeysPerShard`. Detection still evaluates all tracked slots; this cap affects ranked output only.

## ExposeRawKey

Leave `false` unless debugging in a controlled environment. When `true`, raw keys are stored only for slots currently in the bounded tracker.

## Benchmarks

```bash
go test -bench='HotKey|SubmitHotKey|QueueFull|Eviction' -benchmem .
go test -bench='HotKey|QueueFull|Eviction' -benchmem ./internal/core/...
```

Compare disabled vs enabled submit benchmarks before enabling in production.

For autoscaling and capacity decisions, see [autoscaling-signals.md](autoscaling-signals.md) (localized hot key vs distributed backlog).
