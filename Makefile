.PHONY: all fmt format test test-race bench bench-core bench-submit

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

bench-core:
	go test -v ./internal/core -bench=. -benchmem

bench-submit:
	go test -v ./... -bench 'BenchmarkSubmit|BenchmarkSubmitValue' -benchmem
