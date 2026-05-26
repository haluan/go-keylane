# Backend resource lane coordination (KL-1704)

Part of v0.7 — Advanced Request Pipeline & Backend Resource Coordination.

Backend coordination bounds **downstream** work (database, cache, external APIs) per configured resource and backend lane. It is separate from request [`Lane`](lane-priority.md) fairness.

```text
Shard identity end-to-end: yes.
Worker blocked end-to-end: no.
Backend resource pressure unbounded: no.
```

---

## Request lane vs backend lane

| Concept | Purpose | Examples |
|---------|---------|----------|
| **Request lane** | Caller workload fairness on the keylane queue | `default`, `payment`, `audit` |
| **Backend lane** | Downstream resource usage class inside a stage | `db_read`, `db_write`, `external_api` |

A payment request (request lane) may still perform `db_read` on `primary-db` (backend resource + backend lane).

---

## Configuration

Backend coordination is **disabled by default**. Enable it on `Config.BackendResources`:

```go
BackendResources: keylane.BackendResourceConfig{
    Enabled: true,
    Resources: map[keylane.BackendResourceName]keylane.BackendResourcePolicy{
        "primary-db": {
            Lanes: map[keylane.BackendLane]keylane.BackendLanePolicy{
                keylane.BackendLaneDBRead:  {MaxInFlight: 16, Admission: keylane.BackendAdmissionReject},
                keylane.BackendLaneDBWrite: {MaxInFlight: 4, Admission: keylane.BackendAdmissionReject},
            },
        },
        "wallet-api": {
            Lanes: map[keylane.BackendLane]keylane.BackendLanePolicy{
                keylane.BackendLaneExternalAPI: {MaxInFlight: 8, Admission: keylane.BackendAdmissionReject},
            },
        },
    },
},
```

- `MaxInFlight` — maximum concurrent leases per resource/lane (at least 1).
- `QueueLimit` — reserved for future wait-mode admission; stored in snapshots.
- `BackendAdmissionReject` — return an error when saturated (no hidden queue in v1).
- `BackendAdmissionWait` — reserved; config validation rejects it until implemented.

Resource names, backend lanes, stage names, and operation labels must stay **low-cardinality** (no tenant IDs, request IDs, raw URLs, or SQL text).

---

## Synchronous pipeline stage

```go
Run: func(ctx context.Context, state State) (State, error) {
    op := keylane.BackendOperationFromStage(ctx, "primary-db", keylane.BackendLaneDBRead)
    return keylane.WithBackend(ctx, q, op, func(ctx context.Context) (State, error) {
        user, err := repo.FindUser(ctx, state.UserID)
        if err != nil {
            return state, err
        }
        state.User = user
        return state, nil
    })
},
```

`BackendOperationFromStage` copies the active pipeline stage name from [`StageExecutionContext`](stage-execution-context.md) when present.

---

## Continuation stage

Continuations release the worker; backend leases may outlive the yield until the completer goroutine releases them:

```go
RunContinuation: func(ctx context.Context, state State) (keylane.StageResult[State], error) {
    lease, err := keylane.AcquireBackend(ctx, q, keylane.BackendOperationFromStage(ctx, "wallet-api", keylane.BackendLaneExternalAPI))
    if err != nil {
        return keylane.StageResult[State]{}, err
    }

    cont, completer := keylane.NewContinuation[State](ctx)

    go func() {
        defer lease.Release()
        wallet, err := walletClient.Fetch(ctx, state.UserID)
        if err != nil {
            completer.Fail(err)
            return
        }
        state.Wallet = wallet
        completer.Complete(state)
    }()

    return keylane.StageResult[State]{Continuation: cont}, nil
},
```

See [continuations.md](continuations.md) for yield/resume semantics.

---

## API

```go
lease, err := keylane.AcquireBackend(ctx, q, op)
if err != nil { /* saturated, unknown resource, cancelled, ... */ }
defer lease.Release()

// or
result, err := keylane.WithBackend(ctx, q, op, fn)
```

When disabled, `AcquireBackend` returns a no-op lease. `Release` is idempotent (exactly-once accounting).

Admission errors wrap `ErrBackendAdmission` and include `BackendAdmissionError.Decision` with `Reason`, `InFlight`, and `Capacity`. Cancel and deadline rejections also unwrap `context.Canceled` or `context.DeadlineExceeded` (stage budget exhaustion uses the same `Failure` shape as the pipeline) so `errors.Is` and failure classification stay consistent with request handling.

---

## Observability

When `Observability.EnableHooks` is true:

- `Hooks.Backend.OnBackendAdmission`
- `Hooks.Backend.OnBackendReleased`

Backend hooks expose `KeyHash` (not raw routing keys) for correlation with hot-key and shard diagnostics. Do not use `RequestID` or key material as metric labels.

`DebugSnapshot.BackendResources` lists configured resource/lane pressure when `EnableDebugSnapshot` is true and coordination is enabled.

---

## KL-1705 (out of scope here)

KL-1704 does **not** implement concrete `database/sql`, HTTP, Redis, or gRPC pool adapters. Use this layer for in-process admission; integrate external pool stats in KL-1705.
