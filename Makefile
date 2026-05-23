.PHONY: all fmt format test test-race test-adapters ci ci-race bench bench-production bench-low-alloc bench-core bench-submit bench-gc-pressure

all: format test

fmt: format

format:
	gofmt -w .

test:
	go test -v ./...

test-race:
	go test -race -v ./...

test-adapters:
	cd metrics/prometheus && go test ./...
	cd tracing/otel && go test ./...
	cd httpkeylane && go test ./...

ci:
	test -z "$$(gofmt -l .)"
	go mod tidy
	git diff --exit-code go.mod
	test ! -f go.sum || git diff --exit-code go.sum
	go vet ./...
	go test ./...
	cd httpkeylane && go vet ./... && go test ./...
	cd metrics/prometheus && go test ./...
	cd tracing/otel && go test ./...

ci-race:
	go test -race ./...
	cd httpkeylane && go test -race ./...
	cd metrics/prometheus && go test -race ./...
	cd tracing/otel && go test -race ./...

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
