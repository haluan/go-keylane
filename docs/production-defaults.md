# Production defaults

This guide defines **production-safe defaults** for v0.8.0 and recommended configuration bundles.

A bare zero `keylane.Config` is **not** sufficient to construct a queue—core scheduler fields must be set explicitly. For a conservative starting point, use `ProductionDefaults()`.

---

## Production-safe default matrix

This matrix is enforced by `TestProductionDefaultsMatrix` in `config_defaults_test.go`. Keep docs and tests aligned when changing defaults.

| Subsystem | Default behavior |
|-----------|------------------|
| Core scheduler | Enabled when core fields are set (`ShardCount`, `WorkerCount`, `QueueSizePerLane`, `LaneQuotas`) |
| Lane quotas | Caller-defined; must be ≥ 1 per lane |
| Backpressure | `ErrQueueFull` when per-lane capacity is reached |
| Retry | **Disabled** (`Retry.Enabled == false`) |
| Continuation | **Disabled** (`Continuation.Enabled == false`) |
| Backend resources | **Disabled** (`BackendResources.Enabled == false`) |
| Pressure adapters | **Observational only**; do not auto-reject admission |
| Hot key tracking | **Disabled** at zero value; `DefaultHotKeyConfig()` is an explicit opt-in bundle |
| Autoscaling metrics | Signal export only; no built-in autoscaler |
| Raw key labels / snapshot exposure | **Disabled** (`HotKey.ExposeRawKey == false`) |
| Request ID / idempotency key labels | **Not exported** on metrics |
| Observability (unset) | Resolves to full `DefaultObservabilityConfig` at `New` — **warned**; prefer low-allocation preset |
| `ProductionDefaults()` observability | `LowAllocationObservabilityConfig()` |
| Debug snapshots | Available on demand; not per-submit hot path |

---

## Canonical constructor

```go
cfg := keylane.ProductionDefaults()
report := keylane.ValidateConfig(cfg)
if report.HasErrors() {
    return report.Err()
}
q, err := keylane.New(cfg)
```

`ProductionDefaults()` provides:

- `ShardCount: 8`, `WorkerCount: 4`, `QueueSizePerLane: 1000`, `LaneQuotas: {"default": 2}`
- All risky subsystems disabled
- `Observability: LowAllocationObservabilityConfig()`

---

## Inspect defaults

```go
report := keylane.ExplainDefaults(cfg)
for _, d := range report.Defaults {
    log.Printf("%s=%s (%s)", d.Path, d.Value, d.Reason)
}
for _, w := range report.Warnings {
    log.Printf("warning %s: %s", w.Code, w.Message)
}
```

`ExplainDefaults` documents effective gates and includes `ValidateConfig` warnings. Use `ExplainDefaultsWithMode(cfg, keylane.SafetyModeDevelopment)` for development tagging.

---

## Minimal production queue (manual)

Equivalent core fields without calling `ProductionDefaults()`:

```go
cfg := keylane.Config{
    ShardCount:       8,
    WorkerCount:      4,
    QueueSizePerLane: 1000,
    LaneQuotas:       map[keylane.Lane]int{"default": 2},
    Observability:    keylane.LowAllocationObservabilityConfig(),
}
```

---

## Full v0.5 diagnostics bundle (explicit opt-in)

For rich pressure, hot key, and autoscaling signals (see [configuration.md](configuration.md)):

```go
cfg := keylane.ProductionDefaults()
cfg.HotKey = keylane.DefaultHotKeyConfig() // Enabled: true — explicit opt-in
cfg.PerKeyAdmission = keylane.DefaultPerKeyAdmissionConfig()
cfg.ShardPressure = keylane.DefaultShardPressureConfig()
cfg.AutoscalingSignal = keylane.DefaultAutoscalingSignalConfig()
```

`DefaultHotKeyConfig()` keeps `ExposeRawKey: false`.

---

## Retry and idempotency

When enabling retry:

```go
cfg.Retry = keylane.RetryPolicy{
    Enabled:          true,
    MaxAttempts:      3,
    InitialBackoff:   10 * time.Millisecond,
}
cfg.Idempotency = keylane.IdempotencyPolicy{
    RequireForRetry: true,
}
```

---

## Experimental v0.7 subsystems

Continuations and backend resource coordination are **experimental**. Enable only with explicit caps:

```go
cfg.Continuation = keylane.ContinuationConfig{
    Enabled:    true,
    MaxPending: 256,
}
```

Inspect effective settings:

```go
snap := keylane.NormalizeConfig(cfg)
```

---

## Related docs

- [compatibility-rules.md](compatibility-rules.md)
- [observability-contract.md](observability-contract.md)
- [config-validation.md](config-validation.md)
- [production-tuning.md](production-tuning.md)
- [configuration.md](configuration.md)
