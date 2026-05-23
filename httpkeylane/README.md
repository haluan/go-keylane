# HTTP middleware for Keylane

Optional `net/http` integration for [`github.com/haluan/go-keylane`](../). The core module does **not** import `net/http`.

**Module:** `github.com/haluan/go-keylane/httpkeylane`

## Install

```bash
go get github.com/haluan/go-keylane/httpkeylane
```

Use `replace` when developing from this repository.

## Usage

```go
mw := httpkeylane.Middleware(queue, httpkeylane.Config{
    KeyFunc: httpkeylane.HeaderKey("X-Tenant-ID"),
    LaneFunc: httpkeylane.MethodLaneMapper(),
})
handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(http.StatusOK)
    _, _ = w.Write([]byte("ok"))
}))
```

## Key and lane helpers

**Keys:** `HeaderKey`, `QueryKey`, `PathValueKey`, `CookieKey`, `StaticKey`, `RemoteAddrKey`, `CompositeKey`, `FirstNonEmptyKey`. All trim whitespace; missing values return `""` (middleware responds with 400 for empty keys).

`CompositeKey` encodes each non-empty part as `<byte-length>:<value>` joined by `|` (for example `8:tenant-42|10:customer-9`) to avoid collisions.

**Lanes:** `StaticLane`, `MethodLaneMapper` (GET/HEAD/OPTIONS → `read`, POST/PUT/PATCH/DELETE → `write`, unknown → `write`), `MethodLaneMapperWith`, `RouteLaneMapper`.

`RouteLaneMapper` matches rules in **declared order** (first match wins). Put specific path prefixes before general ones:

```go
LaneFunc: httpkeylane.RouteLaneMapper(
    []httpkeylane.LaneRule{
        {Method: http.MethodPost, PathPrefix: "/payments/refunds", Lane: keylane.Lane("refund-write")},
        {Method: http.MethodPost, PathPrefix: "/payments", Lane: keylane.Lane("payment-write")},
    },
    httpkeylane.MethodLaneMapper(),
),
```

## Default HTTP status mapping

| Condition | Status |
|-----------|--------|
| Missing/invalid middleware config | 500 |
| Empty or invalid key | 400 |
| Invalid lane | 400 |
| Queue full / stopped / not started | 503 |
| Context deadline exceeded | 504 |
| Context canceled (before handler runs) | 499 |
| Other errors | 500 |

Override with `Config.ErrorHandler`.

## Testing

```bash
cd httpkeylane && go test ./...
```

Or from the repo root: `make test-adapters`
