# Prometheus adapter for Keylane

Optional pull-based metrics for [`github.com/haluan/go-keylane`](../../). The core module has **no** Prometheus dependency.

## Install

```bash
go get github.com/haluan/go-keylane/metrics/prometheus
```

Requires a separate module; use `replace` when developing from this repo.

## Usage

```go
import (
    "github.com/haluan/go-keylane"
    keylaneprom "github.com/haluan/go-keylane/metrics/prometheus"
    "github.com/prometheus/client_golang/prometheus"
)

reg := prometheus.NewRegistry()
reg.MustRegister(keylaneprom.NewCollector(q, keylaneprom.CollectorOptions{
    SchedulerName: "billing",
}))
```

Each scrape calls `Queue.StatsGCPressure()` and `Queue.Pressure()` (on-demand allocation only).

## Metrics

| Name | Type | Labels |
|------|------|--------|
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

Timing summaries export cumulative `sample_count` / `sample_sum` plus snapshot quantiles (mean at 0.5, max at 1.0) from scheduler stats at scrape time. They are not per-job observation histograms.

## Labels

Use low-cardinality `lane` names only. **Do not** use job `Key` as a label.

## Low-allocation mode

With `keylane.LowAllocationObservabilityConfig()`, counters and depths still update; timing summaries stay at zero count unless `EnableQueueWaitTiming` / `EnableRunTiming` are enabled.
