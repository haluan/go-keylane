# Production hardening (v0.8)

Cross-cutting guide for running go-keylane in production across KL-1802 (validation), KL-1803 (defaults), and KL-1804 (observability contract).

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

---

## Governance map

| Concern | Document | API |
|---------|----------|-----|
| Fatal vs warning config | [config-validation.md](config-validation.md) | `ValidateConfig`, `KL_CONFIG_*` |
| Safe defaults | [production-defaults.md](production-defaults.md) | `ProductionDefaults`, `ExplainDefaults` |
| API stability | [api-stability.md](api-stability.md), [compatibility-rules.md](compatibility-rules.md) | apicheck snapshots |
| Metrics, hooks, traces, snapshots | [observability-contract.md](observability-contract.md) | `StableMetricDescriptors`, contract tests |

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
