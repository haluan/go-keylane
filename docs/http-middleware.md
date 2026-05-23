# HTTP Middleware

`httpkeylane.Middleware` integrates Keylane with `net/http`. It extracts a routing key and lane from each request, runs the handler through the Keylane scheduler, and writes the HTTP response.

---

## Overview

```text
HTTP request
  -> KeyFunc          (extract routing key)
  -> LaneFunc         (select lane)
  -> admission check  (optional pressure gate)
  -> SubmitRequest    (enqueue into scheduler)
  -> scheduled handler
  -> ResponseWriter
  -> HTTP response
```

The wrapped handler does not run until a Keylane worker picks it up. If the handler is delayed or the request is cancelled, the middleware returns an appropriate HTTP status code.

---

## Basic Usage

```go
import (
    "net/http"
    "github.com/haluan/go-keylane"
    "github.com/haluan/go-keylane/httpkeylane"
)

middleware := httpkeylane.Middleware(queue, httpkeylane.Config{
    KeyFunc:  httpkeylane.HeaderKey("X-Tenant-ID"),
    LaneFunc: httpkeylane.MethodLaneMapper(),
})

mux := http.NewServeMux()

mux.Handle("/payments", middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    _, _ = w.Write([]byte("ok"))
})))
```

`Middleware` returns a standard `func(http.Handler) http.Handler` wrapper, compatible with any `net/http`-based router or framework.

---

## Config

```go
type Config struct {
    KeyFunc       KeyFunc         // required: extract routing key
    LaneFunc      LaneFunc        // required: select execution lane
    ErrorHandler  ErrorHandler    // optional: custom error response writer
    Admission     AdmissionConfig // optional: pressure-based admission control
    OperationFunc OperationFunc   // optional: stable operation name for observability
    Observe       ObserveFunc     // optional: per-request observability callback
}
```

- `KeyFunc` and `LaneFunc` are required. A nil queue or missing function causes all requests to return 500.
- `ErrorHandler` defaults to mapping errors to HTTP status codes. See [Status Codes](#status-codes).
- `Admission` is disabled by default. See [admission-control.md](admission-control.md).
- `OperationFunc` sets `RequestMeta.Operation`. If nil, operation remains empty. Do not use `r.URL.Path` directly — it is high-cardinality. See [request-observability.md](request-observability.md).
- `Observe` fires after every request with HTTP metadata and a `RequestObservation`.

---

## KeyFunc

`KeyFunc` extracts a routing key from the HTTP request. The key must be non-empty; an empty key returns 400.

```go
type KeyFunc func(*http.Request) string
```

### Key Helpers

| Helper | Description |
|--------|-------------|
| `HeaderKey(name)` | Reads `r.Header.Get(name)`, trimmed |
| `QueryKey(name)` | Reads `r.URL.Query().Get(name)`, trimmed |
| `PathValueKey(name)` | Reads `r.PathValue(name)` (Go 1.22+ path params) |
| `CookieKey(name)` | Reads the named cookie value |
| `StaticKey(value)` | Always returns the configured string |
| `RemoteAddrKey()` | Uses `r.RemoteAddr` — not recommended behind proxies |
| `FirstNonEmptyKey(parts...)` | Returns the first non-empty key from the list |
| `CompositeKey(parts...)` | Joins non-empty parts with length-prefix encoding |

`CompositeKey` avoids delimiter collisions: `"8:tenant-42|10:customer-99"` cannot be confused with `"tenant-42|"` or `"tenant-4"`.

---

## LaneFunc

`LaneFunc` maps a request to a Keylane lane. An invalid lane returns 400.

```go
type LaneFunc func(*http.Request) keylane.Lane
```

### Lane Helpers

| Helper | Description |
|--------|-------------|
| `StaticLane(lane)` | Always returns the configured lane |
| `MethodLaneMapper()` | GET/HEAD/OPTIONS → `"read"`, others → `"write"` |
| `MethodLaneMapperWith(mapping, fallback)` | Custom method-to-lane mapping |
| `RouteLaneMapper(rules, fallback)` | Route-rule-based lane selection |

`LaneRead = Lane("read")` and `LaneWrite = Lane("write")` are the standard constants used by `MethodLaneMapper`.

---

## Route Rules

`RouteLaneMapper` lets you assign lanes based on HTTP method and URL path prefix:

```go
middleware := httpkeylane.Middleware(queue, httpkeylane.Config{
    KeyFunc: httpkeylane.HeaderKey("X-Tenant-ID"),
    LaneFunc: httpkeylane.RouteLaneMapper(
        []httpkeylane.LaneRule{
            {
                Method:     http.MethodPost,
                PathPrefix: "/payments/refunds",
                Lane:       keylane.Lane("refund-write"),
            },
            {
                Method:     http.MethodPost,
                PathPrefix: "/payments",
                Lane:       keylane.Lane("payment-write"),
            },
            {
                Method:     http.MethodGet,
                PathPrefix: "/reports",
                Lane:       keylane.Lane("report-read"),
            },
        },
        httpkeylane.MethodLaneMapper(), // fallback
    ),
})
```

**Rule evaluation:**
- Rules are evaluated in declared order.
- First match wins.
- Put more specific rules before more general rules (e.g., `/payments/refunds` before `/payments`).
- Empty `Method` matches any method. Empty `PathPrefix` matches any path.
- If no rule matches, `fallback` is called. A nil fallback returns an empty lane (→ 400).

---

## Error Handling

The default error handler maps errors to HTTP status codes and writes a plain-text body. Override it with a custom `ErrorHandler`:

```go
cfg := httpkeylane.Config{
    KeyFunc:  httpkeylane.HeaderKey("X-Tenant-ID"),
    LaneFunc: httpkeylane.MethodLaneMapper(),
    ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
        if errors.Is(err, keylane.ErrAdmissionRejected) {
            w.Header().Set("Retry-After", "1")
            http.Error(w, "overloaded", http.StatusServiceUnavailable)
            return
        }
        http.Error(w, "internal error", http.StatusInternalServerError)
    },
}
```

The custom handler is responsible for writing the full response. If the handler writes nothing, no status code is sent.

---

## Status Codes

Default status codes for middleware errors:

| Error | Status |
|-------|--------|
| Missing or empty key | 400 Bad Request |
| Invalid lane | 400 Bad Request |
| Admission rejected (default) | 503 Service Unavailable |
| Admission rejected (override 429) | 429 Too Many Requests |
| Queue full | 503 Service Unavailable |
| Queue stopped / not started | 503 Service Unavailable |
| Context deadline exceeded | 504 Gateway Timeout |
| Context cancelled | 499 (Client Closed Request) |
| Config error (nil queue, missing func) | 500 Internal Server Error |

The admission rejection status code is configurable via `AdmissionConfig.RejectStatusCode`. See [admission-control.md](admission-control.md).

---

## HTTP Status Capture

The middleware wraps `http.ResponseWriter` to capture the status code written by the handler. This code is available in the `Observe` callback via `HTTPRequestMetadata.StatusCode`. The first `WriteHeader` call wins; subsequent calls are ignored (matching standard `net/http` behavior).

---

## Observability

Use `Observe` for per-request HTTP observability:

```go
cfg := httpkeylane.Config{
    KeyFunc:  httpkeylane.HeaderKey("X-Tenant-ID"),
    LaneFunc: httpkeylane.MethodLaneMapper(),
    OperationFunc: func(r *http.Request) string {
        return r.Method + " /payments"
    },
    Observe: func(meta httpkeylane.HTTPRequestMetadata, obs keylane.RequestObservation) {
        log.Printf("method=%s path=%s status=%d outcome=%s queue_wait=%s run=%s",
            meta.Method, meta.Path, meta.StatusCode,
            obs.Outcome, obs.QueueWait, obs.Run)
    },
}
```

For queue wait and run duration, configure `RequestHooks` in `Config.Observability.Hooks.Request`. See [request-observability.md](request-observability.md).

---

## Request Context

The middleware uses `r.Context()` for both request execution and `Future.Await`. If the client disconnects (context cancelled), the middleware returns. If a handler is already running and ignores cancellation, it may continue until it returns naturally. The middleware does not wait for it.

See [cancellation-timeout.md](cancellation-timeout.md) for full cancellation semantics.
