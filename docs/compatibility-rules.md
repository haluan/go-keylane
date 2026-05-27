# Compatibility rules for production defaults (pre-v1.0)

Production defaults are part of the user-facing contract alongside exported configuration fields. KL-1803 defines how default behavior may evolve before v1.0.

---

## Rules

1. **Defaults are contractual** — Changes that affect throughput, latency, admission, retries, observability, or data exposure require documentation in [migration/v0.7-to-v0.8.md](migration/v0.7-to-v0.8.md) or a future migration note.

2. **Risky features stay opt-in** — Retry, continuations, backend resource coordination, and raw key exposure must not become enabled by default in a patch release.

3. **Side effects require explicit enablement** — Subsystems use top-level `Enabled` gates (`Retry.Enabled`, `Continuation.Enabled`, `BackendResources.Enabled`). Setting nested fields alone must not enable a subsystem.

4. **Observability label stability** — Metric names, label sets, and tracing attributes follow [observability-contract.md](observability-contract.md). Changes require migration notes.

5. **Sensitive identifiers stay off by default** — Raw keys, request IDs, and idempotency keys must not appear as metric labels unless explicitly opted in.

6. **New config fields** — Must document zero-value behavior and appear in validation tests ([config-validation.md](config-validation.md)).

7. **New opt-in subsystems** — Must document whether they are disabled, observational, or enforcing by default.

8. **Stricter defaults allowed** — Safety may tighten (for example new validation warnings or fatal caps) when migration notes describe impact on existing valid configurations.

9. **Pressure adapters** — Remain observational unless the application gates on `Saturated` / pool pressure before `AcquireBackend`.

10. **Autoscaling** — Keylane exports signals only; it does not implement Kubernetes HPA or scale-out decisions.

---

## Inspection API

Use [ProductionDefaults()](../config_defaults.go) and [ExplainDefaults](../config_defaults.go) to inspect gates and warnings before deploy:

```go
cfg := keylane.ProductionDefaults()
report := keylane.ExplainDefaults(cfg)
```

See [production-defaults.md](production-defaults.md) for the default matrix enforced by tests.

---

## Related

- [api-stability.md](api-stability.md)
- [config-versioning.md](config-versioning.md)
- [production-defaults.md](production-defaults.md)
