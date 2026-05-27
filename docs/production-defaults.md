# Production defaults

This guide lists **recommended configuration bundles**. A bare zero `keylane.Config` is **not** sufficient to construct a queue—core scheduler fields must be set explicitly.

---

## Minimal production queue

Required core fields:

```go
cfg := keylane.Config{
    ShardCount:       8,
    WorkerCount:      4,
    QueueSizePerLane: 1000,
    LaneQuotas: map[keylane.Lane]int{
        "default": 2,
    },
}
```

Validate before deploy:

```go
if report := keylane.ValidateConfig(cfg); report.HasErrors() {
    return report.Err()
}
q, err := keylane.New(cfg)
```

Optional subsystems (hot key, retry, continuations, backend resources) remain **disabled** at zero value.

---

## Full v0.5 diagnostics bundle

For rich pressure, hot key, and autoscaling signals (see [configuration.md](configuration.md)):

```go
cfg := keylane.Config{
    ShardCount:       8,
    WorkerCount:      4,
    QueueSizePerLane: 1000,
    LaneQuotas:       map[keylane.Lane]int{"default": 2, "payment": 3},

    HotKey:            keylane.DefaultHotKeyConfig(),
    PerKeyAdmission:   keylane.DefaultPerKeyAdmissionConfig(),
    ShardPressure:     keylane.DefaultShardPressureConfig(),
    AutoscalingSignal: keylane.DefaultAutoscalingSignalConfig(),
}
```

`DefaultHotKeyConfig()` keeps `ExposeRawKey: false` (recommended for production).

---

## Low-allocation observability

```go
cfg.Observability = keylane.LowAllocationObservabilityConfig()
```

Counters and pull APIs remain; hot-path timing and hooks are off. See [benchmarks.md](benchmarks.md).

---

## Retry and idempotency

When enabling retry:

```go
cfg.Retry = keylane.RetryPolicy{
    Enabled:      true,
    MaxAttempts:  3,
    InitialBackoff: 10 * time.Millisecond,
}
cfg.Idempotency = keylane.IdempotencyPolicy{
    RequireForRetry: true,
    // Hook: your RetrySafetyHook for requires_check jobs,
}
```

Without `RequireForRetry` or `Hook`, `ValidateConfig` emits `KL_CONFIG_UNSAFE_RETRY_WITHOUT_IDEMPOTENCY`.

---

## Experimental v0.7 subsystems

Continuations and backend resource coordination are **experimental**. Enable only with explicit caps and operational runbooks:

```go
cfg.Continuation = keylane.ContinuationConfig{
    Enabled:    true,
    MaxPending: 256, // or explicit cap; zero normalizes to 256
}
```

Inspect effective settings:

```go
snap := keylane.NormalizeConfig(cfg)
```

---

## Related docs

- [config-validation.md](config-validation.md)
- [production-tuning.md](production-tuning.md)
- [configuration.md](configuration.md)
