# Configuration validation

Keylane validates configuration on the **cold path** before workers start. Use this API to catch fatal misconfiguration early and surface non-fatal risks before production rollout.

---

## API overview

| Function | Purpose |
|----------|---------|
| `Config.Validate()` | Returns the first fatal error (backward compatible with pre-v0.8.0 code). |
| `ValidateConfig(cfg)` | Returns a `ValidationReport` with errors and warnings. |
| `NormalizeConfig(cfg)` | Returns a redacted `NormalizedConfig` snapshot after normalization. |
| `Queue.ConfigValidationWarnings()` | Warnings recorded when the queue was constructed. |

Recommended workflow:

```go
cfg := keylane.Config{ /* ... */ }

report := keylane.ValidateConfig(cfg)
if report.HasErrors() {
    return report.Err()
}
if report.HasWarnings() {
    log.Printf("config warnings: %+v", report.Issues)
}

q, err := keylane.New(cfg)
if err != nil {
    return err
}
for _, w := range q.ConfigValidationWarnings() {
    log.Printf("warning %s at %s: %s", w.Code, w.Path, w.Message)
}
```

`New` validates configuration first, then normalizes in place and constructs the scheduler. Explicitly invalid values (for example negative retry backoff or negative continuation caps) are rejected **before** normalization applies defaults.

---

## Errors vs warnings

| Severity | Blocks `New`? | Use |
|----------|---------------|-----|
| `ValidationError` | Yes | Invalid shard/worker/queue/lane settings, invalid subsystem policy, retry cap exceeded, etc. |
| `ValidationWarning` | No | Risky but allowed combinations (high worker count, raw key exposure, retry without idempotency hooks). |

Warnings are sorted deterministically by `Code`, then `Path`, then `Message`.

---

## Validation outcomes (examples)

### Valid config

`ProductionDefaults()` passes validation with no fatal errors:

```go
cfg := keylane.ProductionDefaults()
report := keylane.ValidateConfig(cfg)
// report.HasErrors() == false
q, err := keylane.New(cfg) // err == nil when no fatal issues
```

### Valid config with warnings

Risky but allowed settings produce `ValidationWarning` (queue still constructs):

```go
cfg := keylane.ProductionDefaults()
cfg.WorkerCount = 512 // likely >> GOMAXPROCS*4
cfg.Observability.ExposeRawRequestIdentifiers = true

report := keylane.ValidateConfig(cfg)
// report.HasWarnings() == true
// Codes may include:
//   KL_CONFIG_WORKER_COUNT_EXCEEDS_GOMAXPROCS
//   KL_CONFIG_RAW_REQUEST_IDENTIFIERS_IN_HOOKS

q, err := keylane.New(cfg) // succeeds; review q.ConfigValidationWarnings()
```

### Invalid config

Fatal errors block `New`:

```go
cfg := keylane.ProductionDefaults()
cfg.ShardCount = 0 // KL_CONFIG_INVALID_SHARD_COUNT

report := keylane.ValidateConfig(cfg)
// report.HasErrors() == true

_, err := keylane.New(cfg) // err != nil
```

```go
cfg := keylane.ProductionDefaults()
cfg.Retry.Enabled = true
cfg.Retry.MaxAttempts = 300 // KL_CONFIG_UNBOUNDED_RETRY

_, err := keylane.New(cfg) // ErrInvalidRetryPolicy
```

See [examples/production-minimal](../examples/production-minimal/) for a full startup flow.

---

## Stable issue codes

Fatal examples:

| Code | Typical path |
|------|----------------|
| `KL_CONFIG_INVALID_SHARD_COUNT` | `ShardCount` |
| `KL_CONFIG_INVALID_WORKER_COUNT` | `WorkerCount` |
| `KL_CONFIG_INVALID_QUEUE_CAPACITY` | `QueueSizePerLane` |
| `KL_CONFIG_MISSING_LANE_QUOTAS` | `LaneQuotas` |
| `KL_CONFIG_INVALID_LANE_QUOTA` | `LaneQuotas` |
| `KL_CONFIG_UNBOUNDED_RETRY` | `Retry` |
| `KL_CONFIG_INVALID_BACKOFF` | `Retry` |

Warning examples:

| Code | Meaning |
|------|---------|
| `KL_CONFIG_WORKER_COUNT_EXCEEDS_GOMAXPROCS` | `WorkerCount` much larger than `GOMAXPROCS*4` |
| `KL_CONFIG_HIGH_QUEUE_CAPACITY` | `QueueSizePerLane` ≥ 10_000 |
| `KL_CONFIG_UNSAFE_RETRY_WITHOUT_IDEMPOTENCY` | Retry on without `RequireForRetry` or `Hook` |
| `KL_CONFIG_RAW_KEY_EXPOSURE_ENABLED` | `HotKey.ExposeRawKey` (snapshots only; not metric/trace labels) |
| `KL_CONFIG_RAW_REQUEST_IDENTIFIERS_IN_HOOKS` | `Observability.ExposeRawRequestIdentifiers` (hooks, `ObservationForError`, `httpkeylane.ObserveFunc`) |
| `KL_CONFIG_BACKEND_RESOURCES_ENABLED` | Coordination enabled—ensure release discipline |
| `KL_CONFIG_PRESSURE_PROVIDER_OBSERVATIONAL_ONLY` | Providers configured (telemetry is observational even when coordination is enabled) |
| `KL_CONFIG_OBSERVABILITY_FULL_DEFAULTS_RESOLVED` | Unset `Observability` resolves to full defaults at `New` |
| `KL_CONFIG_DEBUG_SNAPSHOT_HOT_PATH_HEAVY` | Debug snapshot + queue-wait + run timing on workers without low-allocation mode |
| `KL_CONFIG_HIGH_CARDINALITY_LABEL_RISK` | Hooks + debug snapshot + hot keys together |

See [config-versioning.md](config-versioning.md) for compatibility expectations on codes.

---

## Retry cap

When retry is enabled, `MaxAttempts` must not exceed **256** after normalization. Larger values return `KL_CONFIG_UNBOUNDED_RETRY` and `ErrInvalidRetryPolicy`.

---

## Normalized snapshot subsystems

`NormalizeConfig` returns:

| Field | Meaning |
|-------|---------|
| `Valid` | `true` only when `ValidateConfig` has no fatal errors (same gate as `New`) |
| `Issues` | All validation errors and warnings with stable codes |
| `Warnings` | Non-fatal issues only (subset of `Issues`) |

When `Valid` is `false`, subsystem fields show normalized values for **support diagnostics only**—no queue can be constructed until errors are fixed.

The snapshot includes effective settings for adaptive quota (controller config and sorted per-lane `LaneAdaptivePolicy` bounds/targets), per-key admission, shard pressure, autoscaling, failure policy, idempotency, retry (including `Jitter` and `RetryableKinds`), retry suppression, per-lane backend limits, and observability (`SlowJobThreshold`). `AppliedDefaults` lists stable tokens for defaults applied across all normalized subsystems. Provider/hook implementations are omitted.

### Continuation timeout guidance

`KL_CONFIG_CONTINUATION_TIMEOUT_MISSING` is emitted whenever continuations are enabled. `CompletionRetention` is **reserved and not enforced**; setting a positive value does not suppress the warning. Define timeouts on pipeline stages and request contexts instead.

---

## Redaction rules

`NormalizeConfig` and validation output:

- Never include raw keys, request IDs, or idempotency key values.
- Do not serialize hook function pointers—only `HooksConfigured` boolean.
- Backend pressure provider implementations are omitted; only counts are included.

---

## httpkeylane

`httpkeylane` middleware uses an existing `keylane.Queue`. Validate `keylane.Config` before `keylane.New`; there is no separate httpkeylane config schema.

---

## Production defaults inspection

See [ProductionDefaults()](../config_defaults.go) and [ExplainDefaults](../config_defaults.go) in [production-defaults.md](production-defaults.md). Compatibility expectations: [compatibility-rules.md](compatibility-rules.md). Observability: [observability-contract.md](observability-contract.md).

---

## Related docs

- [config-versioning.md](config-versioning.md)
- [production-defaults.md](production-defaults.md)
- [compatibility-rules.md](compatibility-rules.md)
- [observability-contract.md](observability-contract.md)
- [configuration.md](configuration.md)
- [api-stability.md](api-stability.md)
