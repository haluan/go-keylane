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
    KeyFunc: func(r *http.Request) string {
        return r.Header.Get("X-Tenant-ID")
    },
    LaneFunc: func(r *http.Request) keylane.Lane {
        return keylane.Lane(r.Method)
    },
})
handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(http.StatusOK)
    _, _ = w.Write([]byte("ok"))
}))
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
