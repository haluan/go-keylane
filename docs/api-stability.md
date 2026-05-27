# API stability (v0.8 pre-v1.0)

KL-1801 defines how `go-keylane` treats its public API before a v1.0 compatibility promise.

> **v0.8 is pre-v1.0.** Exported APIs may still change in minor releases until v1.0. After v1.0, stable APIs are expected to remain source-compatible unless a major release documents a breaking change.

---

## Stability categories

| Category | Meaning |
|----------|---------|
| **Stable Candidate** | Intended production contract toward v1.0. Avoid casual breaking changes after v0.8. |
| **Experimental** | May change before v1.0. Marked in Go doc with `Experimental: may change before v1.0.` |
| **Internal Candidate** | Exported today but not for external use; prefer `internal/` or unexport in a future minor. |
| **Deprecated Candidate** | Remains temporarily; migrate to the replacement API. |
| **Remove Before v1.0** | Planned removal before v1.0; do not adopt in new code. |

See the full inventory: [public-api-inventory.md](public-api-inventory.md).

---

## User-facing packages

| Package | Role | Default category |
|---------|------|------------------|
| `github.com/haluan/go-keylane` | Core queue, runtime, pipeline, backend | Mixed (see inventory) |
| `github.com/haluan/go-keylane/httpkeylane` | HTTP middleware | Stable Candidate (adapter surface) |
| `github.com/haluan/go-keylane/metrics/prometheus` | Prometheus collector | Stable Candidate (optional adapter) |
| `github.com/haluan/go-keylane/tracing/otel` | OpenTelemetry hooks | Stable Candidate (optional adapter) |

`internal/core` and test-only packages are not part of the public contract.

---

## Compatibility rules (v0.8)

1. **Stable Candidate** symbols should not change signatures or semantics without migration notes and inventory updates.
2. **Experimental** symbols may change; callers must read Go doc and release notes.
3. **Config zero values** must be safe, documented, or rejected by `Config.Validate` / `New`.
4. **Hooks, metrics label names, and snapshot field names** are user-facing when exported — treat changes like API changes.
5. **Errors** — document `errors.Is` / `errors.As` expectations; do not rely on string matching.
6. **No test-only exports** from non-`_test.go` files for convenience.
7. **Adapter modules** (`httpkeylane`, `metrics/prometheus`, `tracing/otel`) must not pull observability deps into the core module (enforced by `dependency_boundary_test.go`).

---

## Configuration zero-value behavior

| Config area | Zero value |
|-------------|------------|
| `Continuation` | Disabled (`Enabled == false`); `RunContinuation` stages return `ErrContinuationDisabled` at submit |
| `BackendResources` | Disabled; `AcquireBackend` returns `ErrBackendAdmissionDisabled` |
| `HotKey`, `PerKeyAdmission`, `ShardPressure`, `AutoscalingSignal` | Disabled (v0.5 subsystems) |
| `AdaptiveQuota` | Disabled |
| `Retry` | Disabled |
| `Observability.EnableHooks` | Default from `DefaultObservabilityConfig()` when using defaults |

When `Continuation.Enabled` is true and `MaxPending == 0`, `New` applies `DefaultContinuationMaxPending`.

Core scheduler fields (`ShardCount`, `WorkerCount`, `QueueSizePerLane`, `LaneQuotas`) are **required**; a bare zero `Config` does not construct a queue. See [production-defaults.md](production-defaults.md) and [config-validation.md](config-validation.md) for per-field detail.

### Config validation API (KL-1802, stable candidate)

| Symbol | Role |
|--------|------|
| `ValidateConfig`, `ValidationReport`, `ValidationIssue` | Structured validation |
| `NormalizeConfig`, `NormalizedConfig` | Support/debug snapshots |
| `ConfigVersionV1` | Active config schema version |
| `Queue.ConfigValidationWarnings` | Non-fatal warnings from `New` |

### Production defaults API (KL-1803, stable candidate)

| Symbol | Role |
|--------|------|
| `ProductionDefaults` | Conservative bounded `Config` for production evaluation |
| `ExplainDefaults`, `ExplainDefaultsWithMode` | Inspect gates and warnings |
| `DefaultReport`, `DefaultEntry`, `SafetyMode` | Default inspection types |

---

## Repeatable export review

1. Run `go test ./internal/apicheck/...` — fails if exported symbols drift without updating snapshots.
2. Update [public-api-inventory.md](public-api-inventory.md) when adding or reclassifying symbols.
3. Regenerate snapshots after intentional export changes:

```bash
UPDATE_API_SNAPSHOTS=1 go test ./internal/apicheck/... -run TestUpdatePublicAPISnapshots -v
```

See [internal/apicheck/README.md](../internal/apicheck/README.md).

---

## Migration

- [v0.7 → v0.8](migration/v0.7-to-v0.8.md)

---

## Related

- [public-api-inventory.md](public-api-inventory.md)
- [configuration.md](configuration.md)
- [config-validation.md](config-validation.md)
- [config-versioning.md](config-versioning.md)
- [production-defaults.md](production-defaults.md)
- [compatibility-rules.md](compatibility-rules.md)
- [observability-contract.md](observability-contract.md)
- [v0.7 overview](v0.7-advanced-request-pipeline-and-resource-coordination.md)
