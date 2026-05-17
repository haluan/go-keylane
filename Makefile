.PHONY: all fmt format test test-race

all: format test

fmt: format

format:
	gofmt -w .

test:
	go test ./...

test-race:
	go test -race ./...
