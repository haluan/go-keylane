# Performance regression review

KL-1805 defines how go-keylane measures and compares performance across releases. Benchmarks are **review triggers**, not CI pass/fail gates and not product SLAs.

## Goals

- Measure **hot paths** separately from integration and scenario benches.
- Track **allocation** (`B/op`, `allocs/op`) before claiming latency improvements.
- Track **queue wait** (fairness benches), not only job execution time.
- Use **p50/p95/p99** where fairness benchmarks report distribution metrics.
- Prefer **stable, bounded** scenarios over unbounded goroutine or queue growth.
- Document hardware and Go version whenever publishing numbers.
- Never treat a single benchmark run as a release performance claim.

See also [benchmarks/README.md](../benchmarks/README.md), [benchmarks.md](benchmarks.md), and [observability-contract.md](observability-contract.md).

## v0.8 baseline

The v0.8.0 baseline is captured with:

```bash
make bench-baseline
# or: ./benchmarks/scripts/run-baseline.sh
```

Artifacts:

- [benchmarks/baselines/v0.8.0.md](../benchmarks/baselines/v0.8.0.md) — what the baseline means
- [benchmarks/baselines/v0.8.0.json](../benchmarks/baselines/v0.8.0.json) — structured snapshot (refreshed by the script)

## Running benchmarks

### Standard local run

```bash
go test ./... -run '^$' -bench . -benchmem -count=5
```

### Production suite (stable names)

```bash
make bench-production
# or:
go test ./benchmarks -run '^$' -bench='Keylane|Fairness|GCPressure' -benchmem -count=5
```

### Focused package runs

```bash
go test ./benchmarks -run '^$' -bench='BenchmarkKeylaneSubmit' -benchmem -count=10
go test . -run '^$' -bench='BenchmarkPipeline|BenchmarkBackend' -benchmem -count=5
go test ./internal/core -run '^$' -bench='ProcessShard' -benchmem -count=5
```

### Makefile targets

| Target | Purpose |
|--------|---------|
| `bench-production` | `./benchmarks` Keylane/Fairness/GCPressure |
| `bench-pipeline` | Root pipeline + backend |
| `bench-continuation` | Root continuation lifecycle |
| `bench-core` | `internal/core` scheduler |
| `bench-gc-pressure` | Stats snapshot + submit/processShard guardrails |
| `bench-baseline` | Full capture + JSON refresh |
| `bench-compare` | `benchstat` two text files (`OLD=... NEW=...`) |

## Capturing a baseline

```bash
./benchmarks/scripts/run-baseline.sh
```

Environment variables:

| Variable | Default | Meaning |
|----------|---------|---------|
| `VERSION` | `v0.8.0` | Label for output files |
| `COUNT` | `10` | `-count` for each benchmark |
| `OUT_DIR` | `/tmp` | Text output directory |

The script writes merged bench output to `benchmarks/baselines/v0.8.0.json` via `parsebench`.

## Comparing two releases or refs

Requires [benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat):

```bash
go install golang.org/x/perf/cmd/benchstat@latest
```

**v0.7 vs v0.8** on the same machine:

```bash
git checkout v0.7.0
./benchmarks/scripts/run-baseline.sh
mv /tmp/go-keylane-bench-v0.8.0.txt /tmp/go-keylane-v0.7.0.txt

git checkout <v0.8-branch>
./benchmarks/scripts/run-baseline.sh

./benchmarks/scripts/compare-baseline.sh /tmp/go-keylane-v0.7.0.txt /tmp/go-keylane-bench-v0.8.0.txt
```

Or with Make:

```bash
make bench-compare OLD=/tmp/go-keylane-v0.7.0.txt NEW=/tmp/go-keylane-bench-v0.8.0.txt
```

## Interpreting metrics

| Metric | Meaning |
|--------|---------|
| `ns/op` | Wall time per benchmark loop iteration |
| `B/op` | Bytes allocated per iteration (heap) |
| `allocs/op` | Heap allocations per iteration |
| `wait_p50_ns`, `wait_p95_ns`, `wait_p99_ns` | Queue wait distribution (fairness benches) |
| `completed_jobs/op`, `rejected_jobs/op` | Scenario counters under load |

Fairness and exploratory benches are useful for diagnosing starvation and pressure; stable microbenches are better for release-to-release comparison.

## Regression thresholds (review triggers)

These are **not** automatic CI failures. Regressions may be acceptable when they buy correctness, safety, or contract stability — but must be **documented** in the PR or release note.

| Signal | Threshold | Action |
|--------|-----------|--------|
| Hot-path `allocs/op` | Any increase | Must explain |
| Hot-path `B/op` | > 5% slower / larger | Must explain |
| Hot-path `ns/op` | > 10% slower | Must explain |
| Queue wait p95/p99 | > 10% worse | Must explain |
| Throughput (`completed_jobs/op`, etc.) | > 10% worse | Must explain |
| Goroutine count after bench | Growth | Investigate; see leak tests below |

Stable hot-path benchmarks include `BenchmarkKeylaneSubmit`, `BenchmarkKeylaneSubmitValue`, `BenchmarkKeylaneAwaitCompleted`, and `BenchmarkSubmitHotPathAllocGuardrail`.

## Stable vs exploratory scenarios

**Stable** (release comparison on same host):

- `BenchmarkKeylaneSubmit*`, `BenchmarkKeylaneSubmitValue*`, `BenchmarkKeylaneAwaitCompleted`
- `BenchmarkGCPressureSubmitBurst`, `BenchmarkKeylaneSubmitOneHotKey`
- `BenchmarkSubmitHotPathAllocGuardrail`, `BenchmarkPipelineSingleStage`, `BenchmarkBackendAcquireReleaseDisabled`

**Exploratory** (design review, not single-number claims):

- Fairness distribution benches (`BenchmarkFairness*`)
- Continuation storms (`BenchmarkPipelineContinuation*`)
- Backend saturated reject, full `go test ./... -bench .`

## Goroutine leak checks

Benchmark helpers use `b.Cleanup` for queue stop and context cancel. For behavioral leak coverage, run unit tests:

- `TestAwaitTimeoutNoGoroutineLeak`
- `TestPipelineContinuationNoGoroutineLeak`
- `TestV05GoroutineLeakAfterHotKeyBurstShutdown`
- `TestRejectedSubmitNoGoroutineLeak`

## Performance-sensitive pull request checklist

- Does this change touch submit, await, queue, worker, shard, lane, hook, continuation, or backend resource code?
- Does this change add allocation to a hot path?
- Does this change add a map lookup, string formatting, reflection, closure capture, or interface boxing to a hot path?
- Does this change add lock contention or extend lock hold time?
- Does this change affect queue wait, not only execution time?
- Does this change affect observability overhead when disabled?
- Does this change affect shutdown, cancellation, or timeout behavior?
- Were relevant benchmarks run before and after?
- Is any regression explained in the PR or release note?

## Release notes guidance

When citing performance in v0.8+ release notes:

1. State Go version, commit, OS/arch, and CPU.
2. Reference stable benchmark names only (see [baselines/v0.8.0.md](../benchmarks/baselines/v0.8.0.md)).
3. Use `benchstat` with `-count≥5` on the same machine for before/after.
4. Do not claim universal production performance from local microbenchmarks.

Example statement (v0.8):

> v0.8 introduces a documented benchmark baseline and regression review process for scheduler, Future/Await, lane fairness, shard pressure, pipeline, continuation, backend resource, and observability paths.

## Compatibility

Performance work must not break:

- Public API compatibility (KL-1801)
- Configuration validation (KL-1802)
- Production safety defaults (KL-1803)
- Observability contract stability (KL-1804)

Do not add benchmark-only code paths or weaken production defaults to improve benchmark numbers.
