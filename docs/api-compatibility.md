# API compatibility (v0.8)

This page summarizes how to choose APIs for production. Full inventory and categories: [api-stability.md](api-stability.md) and [public-api-inventory.md](public-api-inventory.md).

## Categories

| Label | Meaning |
|-------|---------|
| **Stable** | Intended for production evaluation; expected to remain source-compatible through v1.0 unless a blocking issue is documented. |
| **Experimental** | Available for evaluation; may change before v1.0. Marked in Go doc where applicable. |
| **Internal** | Not part of the public contract (`internal/core`, test-only helpers). |

Avoid vague labels like “advanced” when the real status is experimental.

## What production code should use

- `ProductionDefaults()`, `ValidateConfig`, `New`, `Start`, `Stop`
- `Submit`, `SubmitValue`, `SubmitRequest`, `Await`
- `errors.Is` with `ErrQueueFull`, `ErrStopped`, `ErrJobPanicked`, etc.
- Optional adapters: `httpkeylane`, `metrics/prometheus`, `tracing/otel`

## What examples may demonstrate (opt-in)

Examples may enable subsystems that production should turn on only after validation:

- `Retry.Enabled` — [safe-retry](../examples/safe-retry/)
- `Continuation.Enabled` — [pipeline_continuation](../examples/pipeline_continuation/)
- `BackendResources.Enabled` — [backend-resource-coordination](../examples/backend-resource-coordination/)
- Full observability hooks — [observability-contract](../examples/observability-contract/)

## Maintainer workflow

After adding exported symbols:

```bash
UPDATE_API_SNAPSHOTS=1 go test ./internal/apicheck/... -run TestUpdatePublicAPISnapshots
```

Update [public-api-inventory.md](public-api-inventory.md) and release notes.

## Related

- [migration/v0.7-to-v0.8.md](migration/v0.7-to-v0.8.md)
- [examples.md](examples.md)
- [compatibility-rules.md](compatibility-rules.md)
