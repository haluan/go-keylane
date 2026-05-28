# Configuration versioning

Keylane configuration is versioned so operators and automation can detect schema drift and interpret normalized snapshots consistently.

---

## Active version

```go
const ConfigVersionV1 ConfigVersion = "keylane.config.v1"
```

`ValidationReport.Version` and `NormalizedConfig.Version` are set to `ConfigVersionV1`.

---

## Compatibility rules (pre-v1.0)

1. **Exported config fields are user-facing API** — see [api-stability.md](api-stability.md).
2. **Existing field meaning must not change silently** — behavior changes require migration notes.
3. **New fields** must document zero-value behavior and include validation tests.
4. **Risky features** stay opt-in; do not enable experimental subsystems by default without migration notes.
5. **Removed or renamed fields** must be listed in [migration/v0.7-to-v0.8.md](migration/v0.7-to-v0.8.md).
6. **Normalized snapshot shape** — additive fields are preferred; renames or removals should be documented when they affect debugging automation.
7. **Validation issue codes** — once published, codes should remain stable for tests and operator playbooks. New codes may be added; existing codes should not change meaning.

---

## Schema evolution

Future versions (e.g. `keylane.config.v2`) would:

- Bump `ConfigVersion` constants.
- Document migration from v1 snapshots and reports.
- Keep `ValidateConfig` accepting prior shapes when feasible, or provide explicit conversion helpers.

---

## Normalized snapshots

`NormalizeConfig` captures effective settings **after** `normalizeConfigInPlace` and lists `AppliedDefaults` such as:

- `continuation.max_pending=256`
- `retry.max_attempts=3`

Snapshots are safe for logs and support tickets when [redaction rules](config-validation.md#redaction-rules) are respected.

---

## Related docs

- [config-validation.md](config-validation.md)
- [production-defaults.md](production-defaults.md)
