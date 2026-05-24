# Prometheus metrics adapter

The core [`github.com/haluan/go-keylane`](../) module does **not** import Prometheus. Optional metrics live in:

**Module:** `github.com/haluan/go-keylane/metrics/prometheus`

## Quick start

```go
import (
    keylaneprom "github.com/haluan/go-keylane/metrics/prometheus"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

reg := prometheus.NewRegistry()
reg.MustRegister(keylaneprom.NewCollector(q, keylaneprom.CollectorOptions{
    SchedulerName: "my-service",
}))
http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
```

See [examples/prometheus](../examples/prometheus/) for a minimal runnable sample.

## Exported metrics

| Metric | Type | Labels |
|--------|------|--------|
| `keylane_jobs_submitted_total` | Counter | `scheduler`, `lane` |
| `keylane_jobs_completed_total` | Counter | `scheduler`, `lane` |
| `keylane_jobs_failed_total` | Counter | `scheduler`, `lane` |
| `keylane_queue_full_total` | Counter | `scheduler`, `lane` |
| `keylane_lane_depth` | Gauge | `scheduler`, `lane` |
| `keylane_shard_depth` | Gauge | `scheduler`, `shard_id` |
| `keylane_inflight_jobs` | Gauge | `scheduler`, `shard_id`, `lane` |
| `keylane_queue_wait_seconds` | Summary | `scheduler`, `lane` |
| `keylane_run_duration_seconds` | Summary | `scheduler`, `lane` |
| `keylane_pressure_ratio` | Gauge | `scheduler` |

v0.5 scale, shard pressure, and hot key metrics are documented in [metrics.md](metrics.md). The collector exports them when v0.5 config is enabled on the queue.

Data is read from `Queue.StatsGCPressure()` and `Queue.Pressure()` on each scrape. Timing metrics are Prometheus **summaries** built from cumulative scheduler stats (`sample_count`, `sample_sum`, and snapshot quantiles at 0.5 mean and 1.0 max). They are not native per-observation histogram buckets; rate/increase across scrapes reflects scheduler totals, not individual job samples.

## Label guidance

Allowed: `scheduler` (your deployment name), static `lane` names, `shard_id`.

Avoid: job `Key`, request IDs, tenant IDs, or dynamic label maps — they cause high cardinality and memory pressure. See [metrics.md](metrics.md) for the full forbidden-label list.

## Low-allocation mode

Prometheus scraping is **off the submit hot path**. With `LowAllocationObservabilityConfig()`, depth and counter metrics still work; queue-wait and run-duration summaries report zero count/sum unless timing is enabled in config.

## Testing adapters

```bash
make test-adapters
# or:
cd metrics/prometheus && go test ./...
```
