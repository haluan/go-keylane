.PHONY: all fmt format test test-race bench bench-production bench-low-alloc bench-core bench-submit bench-gc-pressure

all: format test

fmt: format

format:
	gofmt -w .

test:
	go test -v ./...

test-race:
	go test -race -v ./...

bench:
	go test -v ./... -bench=. -benchmem

bench-production:
	go test -bench='Keylane|Fairness|GCPressure' -benchmem ./benchmarks

bench-low-alloc:
	go test -bench='BenchmarkKeylaneSubmit.*Observability|BenchmarkKeylaneSubmitValue.*Observability|BenchmarkKeylaneDebugSnapshotOnDemand' -benchmem ./benchmarks
	go test -bench='BenchmarkKeylaneWorker.*Observability' -benchmem ./internal/core

bench-core:
	go test -v ./internal/core -bench=. -benchmem

bench-submit:
	go test -v ./... -bench 'BenchmarkSubmit|BenchmarkSubmitValue' -benchmem

bench-gc-pressure:
	go test -v . -bench 'BenchmarkStatsGCPressure|BenchmarkSubmit' -benchmem
	go test -v ./internal/core -bench 'BenchmarkStatsGCPressure|BenchmarkProcessShardSingleLane' -benchmem
