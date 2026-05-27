# Backend pool pressure adapters

Part of [v0.7.0 — Advanced Request Pipeline & Backend Resource Coordination](v0.7-advanced-request-pipeline-and-resource-coordination.md).

Bounds **in-process** backend usage via leases (`AcquireBackend` / `WithBackend`). v0.7.0 adds **optional adapters** that map external pool telemetry (`database/sql`, custom API semaphores) into the same low-cardinality `backend_resource` + `backend_lane` identity for diagnostics and hooks.

Keylane does **not** own connection pools, HTTP clients, transports, or retry semantics.

---

## Generic model

```go
type BackendPressureProvider interface {
    BackendPressure(context.Context) keylane.BackendPressureSnapshot
}
```

`snapshot` fields: `Resource`, `Lane`, `InUse`, `Capacity`, `Idle`, `WaitCount`, `WaitTime`, `Saturated`, `Pressure` (0..1 when `Capacity > 0`).

Register providers at config time:

```go
cfg.BackendResources = keylane.BackendResourceConfig{
    Enabled: true,
    Resources: /* lanes */,
    PressureProviders: []keylane.BackendPressureProvider{
        keylane.SQLDBPressureAdapter{Resource: "primary-db", Lane: keylane.BackendLaneDBRead, DB: db},
        keylane.APIClientPressureAdapter{Resource: "wallet-api", Lane: keylane.BackendLaneExternalAPI, Reader: sem},
    },
}
```

Provider registration is fixed after `keylane.New`; the slice is copied defensively into the queue.

---

## database/sql adapter

```go
adapter := keylane.SQLDBPressureAdapter{
    Resource: "primary-db",
    Lane:     keylane.BackendLaneDBRead,
    DB:       db, // *sql.DB implements Stats()
}
```

Mapping from `sql.DBStats`:

- `InUse`, `Idle` from stats
- `Capacity` from `MaxOpenConnections` when `> 0`
- `WaitCount`, `WaitTime` from wait metrics
- `Saturated` when `InUse >= MaxOpenConnections`
- When `MaxOpenConnections == 0` (unbounded), `Capacity = 0`, `Pressure = 0`, `Saturated = false`

---

## Custom API / HTTP pools

Implement `ResourcePressureReader` on your semaphore or client pool:

```go
type ResourcePressureReader interface {
    InUse, Capacity() int
    WaitCount() uint64
    WaitTime() time.Duration
    Saturated() bool
}
```

```go
adapter := keylane.APIClientPressureAdapter{
    Resource: "wallet-api",
    Lane:     keylane.BackendLaneExternalAPI,
    Reader:   myPool,
}
```

### Why not `net/http.Transport`?

Standard `http.Transport` does not expose reliable in-use/wait counters for a generic adapter. Wrap your own bounded executor or semaphore and expose `ResourcePressureReader` instead.

---

## Diagnostics and hooks

```go
pressure := q.BackendPressure(ctx)
snap := q.DebugSnapshot() // includes BackendPressure when EnableDebugSnapshot is true
```

When `Observability.EnableHooks` is true:

```go
obs.Hooks.Backend.OnBackendPressure = func(ev keylane.BackendPressureEvent) {
    // ev.Snapshot — resource, backend_lane, pressure, saturated, wait stats
}
```

Hook payloads use **hash-only** routing identity where applicable; never include DSNs, URLs, SQL, tenant IDs, or request IDs as labels.

---

## Metrics guidance

Suggested Prometheus-style metrics (implement in your hook adapter):

| Metric | Type | Labels |
|--------|------|--------|
| `keylane_backend_pressure_ratio` | Gauge | `backend_resource`, `backend_lane` |
| `keylane_backend_in_use` | Gauge | `backend_resource`, `backend_lane` |
| `keylane_backend_capacity` | Gauge | `backend_resource`, `backend_lane` |
| `keylane_backend_wait_total` | Counter | `backend_resource`, `backend_lane` |
| `keylane_backend_wait_duration_seconds` | Counter | `backend_resource`, `backend_lane` |
| `keylane_backend_saturated` | Gauge | `backend_resource`, `backend_lane` |

Do **not** label with raw URL, SQL, tenant ID, user ID, request ID, or routing key.

---

## Related

- [backend-resource-coordination.md](backend-resource-coordination.md) — leases and admission
- [request-observability.md](request-observability.md) — hook configuration
- [metrics.md](metrics.md) — metric naming
- [pipeline-observability.md](pipeline-observability.md) — `OnBackendPressure` lifecycle and low-cardinality fields
- [pipeline-testing.md](pipeline-testing.md) — pressure adapter and `DebugSnapshot.BackendPressure` tests

### Troubleshooting

- **Empty `BackendPressure` in snapshot**: confirm `PressureProviders` on `BackendResourceConfig` and `EnableDebugSnapshot`; see `debug_snapshot_pipeline_test.go`.
- **Pool saturated but admission still succeeds**: v0.7.0 observes only; applications gate on `Saturated` before `AcquireBackend` if needed.
