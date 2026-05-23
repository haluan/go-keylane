# Admission Control

Pressure-based admission control lets the middleware reject incoming requests before they enter the scheduler queue when the runtime is overloaded. It is disabled by default.

---

## Overview

Admission control gates requests based on Keylane's internal pressure signal. When the queue depth across all shards and lanes exceeds a configured threshold, new requests are rejected immediately — before they are enqueued — with an HTTP error response.

Admission runs before enqueue. Rejected requests do not enter the scheduler queue. Rejected handlers do not run.

---

## Pressure Signal

Keylane exposes a `TotalDepthRatio`: the fraction of total queue capacity currently occupied across all shards and lanes.

```text
TotalDepthRatio = total_queued_jobs / total_queue_capacity
```

A ratio of 0.0 means all queues are empty. A ratio of 1.0 means all queues are full.

---

## Configuration

```go
type AdmissionConfig struct {
    Enabled          bool    // admission control is disabled when false (default)
    RejectAboveRatio float64 // reject when TotalDepthRatio >= this value (default 0.90)
    RejectStatusCode int     // HTTP status code for rejected requests (default 503)
}
```

**Defaults when enabled:**
- `RejectAboveRatio`: 0.90 (reject when 90% or more of queue capacity is used)
- `RejectStatusCode`: 503 Service Unavailable

A zero `RejectAboveRatio` is treated as 0.90 after normalization.

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
  -> admission check (pressure ≥ threshold → reject with configured status)
  -> SubmitRequest (queue full → 503)
  -> handler runs
```

A rejected request increments the `AdmissionRejected` counter on the lane and fires `RequestHooks.OnRejected`. It does not increment `TotalQueued`.

---

## HTTP Status Codes

| Scenario | Default status | Override |
|----------|---------------|---------|
| Admission rejected | 503 Service Unavailable | `RejectStatusCode: 429` |

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
- Pressure is measured across all shards and lanes combined. It does not differentiate between lanes.
- A short burst of requests can pass admission before pressure updates if the signal lags queue depth changes.
- Keylane does not provide per-tenant or per-key admission control. Implement that at the application layer.
