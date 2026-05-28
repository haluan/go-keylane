# Public API export guard (KL-1801)

Lightweight check that user-facing packages do not grow exported symbols without an explicit inventory update.

## Packages under review

- `github.com/haluan/go-keylane`
- `github.com/haluan/go-keylane/httpkeylane`
- `github.com/haluan/go-keylane/metrics/prometheus`
- `github.com/haluan/go-keylane/tracing/otel`

## Run the guard

From repository root:

```bash
go test ./internal/apicheck/...
```

## Update snapshots after intentional API changes

```bash
UPDATE_API_SNAPSHOTS=1 go test ./internal/apicheck/... -run TestUpdatePublicAPISnapshots -v
```

Commit the changed files under `internal/apicheck/testdata/exports_*.txt` together with [docs/public-api-inventory.md](../../docs/public-api-inventory.md).

## Manual review commands

```bash
go list ./... | grep -v '/internal/' | grep -v '/examples/' | grep -v '/benchmarks'
go doc -all github.com/haluan/go-keylane | less
go doc -all github.com/haluan/go-keylane/httpkeylane
```

Full export name lists are in `testdata/exports_*.txt` (one symbol per line, sorted).
