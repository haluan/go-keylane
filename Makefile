.PHONY: all fmt format test test-race

all: format test

fmt: format

format:
	gofmt -w .

test:
	go test ./...

test-race:
	go test -race ./...

bench:
	go test ./... -bench=. -benchmem

bench-core:
	go test ./internal/core -bench=. -benchmem

bench-submit:
	go test ./... -bench 'BenchmarkSubmit|BenchmarkSubmitValue' -benchmem

bench-race:
	go test -race ./...
