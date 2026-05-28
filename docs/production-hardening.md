# Production hardening (v0.8)

Cross-cutting guide for running go-keylane in production covering API stability, validation, defaults, observability contract, benchmarks, lifecycle, and examples.

Hub for: **API Stabilization & Production Hardening** before v1.0. See [releases/v0.8.0.md](releases/v0.8.0.md).

---

## Recommended startup

```go
cfg := keylane.ProductionDefaults()
// Apply service-specific lanes, quotas, and explicit subsystem opt-in.

report := keylane.ValidateConfig(cfg)
if report.HasErrors() {
    return report.Err()
}
for _, w := range report.Issues {
    if w.Severity == keylane.ValidationWarning {
        log.Printf("config warning %s: %s", w.Code, w.Message)
    }
}

q, err := keylane.New(cfg)
if err != nil {
    return err
}
```

Inspect effective settings:

```go
snap, _ := keylane.NormalizeConfig(cfg)
defaults := keylane.ExplainDefaults(cfg)
_ = snap
_ = defaults
```

Walkthrough: [production-minimal.md](production-minimal.md) · Example: [examples/production-minimal](../examples/production-minimal/).

---

## v0.8.0 documentation index

| Topic | Document |
|-------|----------|
| Hub (this page) | [production-hardening.md](production-hardening.md) |
| API stability | [api-stability.md](api-stability.md), [api-compatibility.md](api-compatibility.md), [public-api-inventory.md](public-api-inventory.md) |
| Config validation | [config-validation.md](config-validation.md), [config-versioning.md](config-versioning.md) |
| Production defaults | [production-defaults.md](production-defaults.md), [compatibility-rules.md](compatibility-rules.md) |
| Observability contract | [observability-contract.md](observability-contract.md) |
| Performance regression | [performance-regression.md](performance-regression.md) |
| Lifecycle (panic, shutdown, leaks, races) | [runtime-lifecycle-hardening.md](runtime-lifecycle-hardening.md) |
| Migration | [migration/v0.7-to-v0.8.md](migration/v0.7-to-v0.8.md) |
| Examples | [examples.md](examples.md), [examples/README.md](../examples/README.md) |
| Release notes | [releases/v0.8.0.md](releases/v0.8.0.md) |

---

## Governance map

| Concern | Document | API |
|---------|----------|-----|
| Fatal vs warning config | [config-validation.md](config-validation.md) | `ValidateConfig`, `KL_CONFIG_*` |
| Safe defaults | [production-defaults.md](production-defaults.md) | `ProductionDefaults`, `ExplainDefaults` |
| API stability | [api-stability.md](api-stability.md), [compatibility-rules.md](compatibility-rules.md) | apicheck snapshots |
| Metrics, hooks, traces, snapshots | [observability-contract.md](observability-contract.md) | `StableMetricDescriptors`, contract tests |
| Panic, shutdown, leaks, races | [runtime-lifecycle-hardening.md](runtime-lifecycle-hardening.md) | `Stop`, `WithDrain`, `ErrJobPanicked`, `HookPanicsRecovered` |
| Performance regression | [performance-regression.md](performance-regression.md) | `make bench-baseline` |

---

## Subsystem safety (defaults and opt-in)

### Retry

- **Default:** disabled (`Retry.Enabled == false`).
- **Enable when:** operations are idempotent or protected by `Idempotency.RequireForRetry` / hooks.
- **Do not:** enable retry for non-idempotent writes without explicit safety classification.
- **Validation:** `KL_CONFIG_UNSAFE_RETRY_WITHOUT_IDEMPOTENCY`, `KL_CONFIG_UNBOUNDED_RETRY` (cap 256 attempts).
- See [retry-policy.md](retry-policy.md), [idempotency.md](idempotency.md), [examples/safe-retry](../examples/safe-retry/).

### Continuation

- **Default:** disabled (`Continuation.Enabled == false`).
- **Enable when:** pipeline stages yield during slow I/O and you have stage/request timeouts.
- **Discipline:** do not `Await` the same queue from inside a stage; handle late completion.
- **Validation:** `KL_CONFIG_CONTINUATION_TIMEOUT_MISSING`.
- See [continuations.md](continuations.md), [runtime-lifecycle-hardening.md](runtime-lifecycle-hardening.md#continuation-late-completion).

### Backend resource coordination

- **Default:** disabled (`BackendResources.Enabled == false`).
- **Enable when:** in-process lease discipline (`defer lease.Release()`) is enforced in all paths.
- **Scope:** admission per resource/lane in-process only — not distributed locking.
- **Validation:** `KL_CONFIG_BACKEND_RESOURCES_ENABLED`.
- See [backend-resource-coordination.md](backend-resource-coordination.md).

### Pressure adapters

- **Default:** none configured; observational when added.
- **Behavior:** pool/API pressure snapshots do **not** auto-reject unless your app gates on `Saturated`.
- **Validation:** `KL_CONFIG_PRESSURE_PROVIDER_OBSERVATIONAL_ONLY`.
- See [backend-pressure-adapters.md](backend-pressure-adapters.md).

### Hot-key visibility

- **Default:** hot-key tracking disabled at zero value.
- **Raw keys:** `HotKey.ExposeRawKey` is for debug snapshots only — warns with `KL_CONFIG_RAW_KEY_EXPOSURE_ENABLED`; never use as metric labels.
- See [hot-key-detection.md](hot-key-detection.md).

### Observability labels and raw identifiers

- **Default:** hook payloads redact `Key` and `RequestID`; low-allocation preset in `ProductionDefaults()`.
- **Opt-in:** `Observability.ExposeRawRequestIdentifiers` warns with `KL_CONFIG_RAW_REQUEST_IDENTIFIERS_IN_HOOKS`.
- **Never:** raw keys, request IDs, idempotency keys, or paths as prometheus labels on stable metrics.
- See [observability-contract.md](observability-contract.md).

### Panic and hook isolation

- **Job panics:** recovered → `ErrJobPanicked`; worker continues; `FailurePanic` classification.
- **Hook panics:** `callHook` recovers; `HookPanicsRecovered()` for diagnostics.
- **Not retried:** job panics by default.
- See [runtime-lifecycle-hardening.md](runtime-lifecycle-hardening.md), [failure-policy.md](failure-policy.md).

### Shutdown and drain

- **`Stop(ctx)`:** reject new work; workers exit.
- **`Stop(ctx, WithDrain(true))`:** drain queued work until empty or context expires.
- **After stop:** `Submit` / `SubmitValue` → `ErrStopped`.
- See [phase-5-backpressure-and-shutdown.md](phase-5-backpressure-and-shutdown.md).

---

## Observability checklist

1. Use `LowAllocationObservabilityConfig()` or `ProductionDefaults()` unless you need full hooks.
2. Register [metrics/prometheus](../metrics/prometheus) with a static `scheduler` label.
3. Do not add raw keys, request IDs, or paths as metric labels.
4. Implement pipeline/backend metrics in hooks using [metrics.md](metrics.md) patterns (`experimental` stability).
5. Attach [tracing/otel](../tracing/otel) only with explicit tracer and bounded attributes.
6. Use `DebugSnapshot()` for ops pulls, not per-request hot paths.
7. Review `Queue.ConfigValidationWarnings()` after `New`.

---

## When to opt in to advanced subsystems

| Subsystem | Enable when | Validation warnings |
|-----------|-------------|---------------------|
| Retry | Idempotency hooks or `RequireForRetry` | `KL_CONFIG_UNSAFE_RETRY_WITHOUT_IDEMPOTENCY` |
| Continuation | Async I/O between stages | `KL_CONFIG_CONTINUATION_TIMEOUT_MISSING` |
| Backend resources | In-process lease discipline | `KL_CONFIG_BACKEND_RESOURCES_ENABLED` |
| Hot keys | Mitigation required | `KL_CONFIG_HIGH_CARDINALITY_LABEL_RISK` with hooks |
| Raw keys in snapshots | Debugging only | `KL_CONFIG_RAW_KEY_EXPOSURE_ENABLED` |

---

## Release and migration

- [migration/v0.7-to-v0.8.md](migration/v0.7-to-v0.8.md)
- [releases/v0.8.0.md](releases/v0.8.0.md)
