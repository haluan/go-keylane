# Production-minimal example

The [production-minimal](../examples/production-minimal/) example is the recommended v0.8.0 starting point for new integrations.

```bash
go run ./examples/production-minimal
```

## What it demonstrates

1. **`ProductionDefaults()`** — bounded scheduler; retry, continuation, and backend coordination disabled; low-allocation observability preset.
2. **`ValidateConfig`** — surface `KL_CONFIG_*` warnings before `New`.
3. **`Queue.ConfigValidationWarnings()`** — construction-time warnings.
4. **`SubmitValue` + `Await`** — value-returning work with a timeout context.
5. **Error taxonomy** — distinguish admission rejection (`ErrQueueFull`) from await cancellation/deadline.
6. **`Stop(ctx, WithDrain(true))`** — graceful shutdown with drain.

## Error handling boundaries

| Error | Meaning |
|-------|---------|
| `ErrQueueFull` | Admission rejected (queue at capacity) |
| `context.Canceled` | Caller context canceled during await |
| `context.DeadlineExceeded` | Await timeout |
| `ErrStopped` | Submit after shutdown (see [shutdown-submit](../examples/shutdown-submit/)) |
| `ErrJobPanicked` | User `Run` panicked; worker recovered ([runtime-lifecycle-hardening.md](runtime-lifecycle-hardening.md)) |

Job failure (business error from `Run`) is separate from admission rejection.

## Labels and logging

The example uses opaque tenant identifiers in logs. Do **not** use raw keys, request IDs, or idempotency keys as Prometheus labels. See [observability-contract.md](observability-contract.md).

## Next steps

| Topic | Doc / example |
|-------|----------------|
| Service-shaped concurrency | [request-runtime](../examples/request-runtime/), [request-runtime.md](request-runtime.md) |
| HTTP middleware | [http-middleware.md](http-middleware.md) |
| Production defaults | [production-defaults.md](production-defaults.md) |
| Lifecycle / shutdown | [runtime-lifecycle-hardening.md](runtime-lifecycle-hardening.md) |
| All examples | [examples.md](examples.md) |
