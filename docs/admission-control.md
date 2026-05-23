# Admission Control

Pressure-based admission control lets the middleware reject incoming requests before they enter the scheduler queue when the runtime is overloaded. It is disabled by default.

---

## Overview

Admission control gates **new** submissions before enqueue. When enabled on a request or HTTP middleware path, Keylane evaluates per-lane policy using:

1. **Per-lane queue depth** — reject when the lane's total queued jobs reach `MaxQueueDepth`
2. **Global pressure** — reject when `TotalDepthRatio` meets or exceeds the lane's `RejectAboveRatio`

Admission runs before enqueue. Rejected requests do not enter the scheduler queue. Rejected handlers do not run. Policy updates do not drop queued work or interrupt running jobs.

For overload actions (`keep`, `reject`, `shed`, `degrade`) and `Retry-After` hints, see [overload-policy.md](overload-policy.md).

`LaneClass` is an admission priority, not a strict scheduler priority. It does not reorder FIFO work inside a lane.

- **Critical does not mean unlimited** — critical lanes reject later under pressure but still hit `MaxQueueDepth` and pressure thresholds.
- **Best-effort does not mean never runs** — best-effort work is shed earlier when overloaded; admitted jobs run normally.
- **Per-lane queue depth** — `MaxQueueDepth` caps queued jobs per lane across all shards, protecting scheduler capacity during overload even when global pressure is still low.

---

## Pressure Signal

Keylane exposes a `TotalDepthRatio`: the fraction of total queue capacity currently occupied across all shards and lanes.

```text
TotalDepthRatio = total_queued_jobs / total_queue_capacity
```

A ratio of 0.0 means all queues are empty. A ratio of 1.0 means all queues are full.

---

## Lane classes

```go
const (
    LaneCritical   // protect longer under pressure
    LaneNormal     // default
    LaneBackground // reject earlier than normal
    LaneBestEffort // earliest rejection under pressure
)
```

## Per-lane admission policy

```go
type AdmissionPolicy struct {
    DefaultClass            LaneClass
    DefaultRejectAboveRatio float64
    DefaultMaxQueueDepth    uint32
    Lanes                   []LanePolicy // optional per-lane overrides
}

type LanePolicy struct {
    Lane             Lane
    Class            LaneClass
    RejectAboveRatio float64
    MaxQueueDepth    uint32
}
```

Update at runtime (immutable snapshot, same pattern as quota policy):

```go
version, err := queue.UpdateAdmissionPolicy(keylane.AdmissionPolicy{
    DefaultClass:            keylane.LaneNormal,
    DefaultRejectAboveRatio: 0.90,
    DefaultMaxQueueDepth:    uint32(shardCount * queueSizePerLane),
    Lanes: []keylane.LanePolicy{
        {Lane: "payment", Class: keylane.LaneCritical, RejectAboveRatio: 0.98, MaxQueueDepth: 2048},
        {Lane: "report", Class: keylane.LaneBestEffort, RejectAboveRatio: 0.60, MaxQueueDepth: 256},
    },
})
snap := queue.CurrentAdmissionPolicy()
```

Lanes are fixed at queue construction; policy can only adjust rules for registered lanes.

## Request gate (`AdmissionConfig`)

```go
type AdmissionConfig struct {
    Enabled          bool    // admission control is disabled when false (default)
    RejectAboveRatio float64 // validated when enabled; thresholds come from AdmissionPolicy
    RejectStatusCode int     // HTTP status for pressure rejections (default 503)
}
```

When `Enabled` is true, per-lane thresholds come from the queue's admission policy snapshot. `RejectAboveRatio` on `AdmissionConfig` is kept for API compatibility and validation.

**HTTP defaults:**
- Pressure rejection (`pressure_above_lane_threshold`): **503** (or `RejectStatusCode`)
- Lane depth rejection (`lane_queue_depth_exceeded`): **429** Too Many Requests

---

## Enabling Admission Control

```go
middleware := httpkeylane.Middleware(queue, httpkeylane.Config{
    KeyFunc:  httpkeylane.HeaderKey("X-Tenant-ID"),
    LaneFunc: httpkeylane.MethodLaneMapper(),
    Admission: httpkeylane.AdmissionConfig{
        Enabled:          true,
        RejectAboveRatio: 0.90,
        RejectStatusCode: http.StatusServiceUnavailable,
    },
})
```

---

## Before-Enqueue Rejection

The admission check runs after key and lane validation, but before `SubmitRequest` is called:

```text
request arrives
  -> extract key (missing key → 400)
  -> extract lane (invalid lane → 400)
  -> admission check (depth or pressure → reject with 429 or 503)
  -> SubmitRequest (queue full → 503)
  -> handler runs
```

A rejected request increments the `AdmissionRejected` counter on the lane and fires `RequestHooks.OnRejected`. It does not increment `TotalQueued`.

---

## HTTP Status Codes

| Scenario | Default status | Override |
|----------|---------------|---------|
| Pressure above lane threshold | 503 Service Unavailable | `RejectStatusCode` |
| Lane queue depth exceeded | 429 Too Many Requests | — |

**When to use 503:**
The runtime is overloaded. Callers should back off and retry with exponential backoff.

**When to use 429:**
When you want rate-limit-like semantics and expect clients to respect `Retry-After`. Note that Keylane admission control is not a rate limiter — it reflects internal queue pressure, not per-client request rates.

---

## Per-Request Admission Override

`SubmitRequest` also accepts an `AdmissionConfig` on the `Request` struct for transport-agnostic admission:

```go
req := keylane.Request[Input, Output]{
    Meta:      meta,
    Admission: keylane.AdmissionConfig{Enabled: true, RejectAboveRatio: 0.85},
    Input:     input,
    Handle:    handler,
}
```

The HTTP middleware uses `Config.Admission` for this. Both paths call `keylane.CheckAdmission`.

---

## Observability

**Via `RequestHooks.OnRejected`:**

```go
obs := keylane.DefaultObservabilityConfig()
obs.Hooks.Request.OnRejected = func(o keylane.RequestObservation) {
    if o.Outcome == keylane.RequestOutcomeAdmissionRejected {
        admissionRejectedCounter.Add(1)
    }
}
cfg.Observability = obs
```

**Via `StatsGCPressure` lane counters:**

```go
snap := queue.StatsGCPressure()
for _, lane := range snap.Lanes {
    fmt.Printf("lane=%s admitted=%d admission_rejected=%d\n",
        lane.Name, lane.Counters.Accepted, lane.Counters.AdmissionRejected)
}
```

---

## Limitations

- Admission control is **process-local**. It does not provide distributed rate limiting or cluster-wide admission control.
- Pressure uses a single global `TotalDepthRatio`; per-lane policy sets different cutoffs on that signal.
- A short burst of requests can pass admission before pressure updates if the signal lags queue depth changes.
- Keylane does not provide per-tenant or per-key admission control. Implement that at the application layer.
- Fire-and-forget `Submit(Job)` does not run admission unless you add application-level gating.
