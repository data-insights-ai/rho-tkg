SHELL := /bin/bash

.DEFAULT_GOAL := build

.PHONY: build test test-v test-race test-integration bench cover vet fmt fmt-check security vulncheck check ci clean

# Build (verify compilation)
build:
	go build ./...

# Run unit tests
test:
	go test -short -count=1 ./...

# Run tests with verbose output
test-v:
	go test -short -count=1 -v ./...

# Run tests with race detector
test-race:
	go test -short -race -count=1 ./...

# Run integration tests (long-running, not short)
test-integration:
	go test -count=1 -run Integration ./...

# Run benchmarks (long-running, not short)
bench:
	go test -v -count=1 -run TestBench ./pkg/types/

# Run tests with coverage report
cover:
	go test -short -count=1 -coverpkg=./... -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Run go vet
vet:
	go vet ./...

# Format code
fmt:
	gofmt -w .

# Verify formatting (fails if any file needs formatting)
fmt-check:
	@test -z "$$(gofmt -l .)" || (echo "Files need formatting:"; gofmt -l .; exit 1)

# Static security analysis
security:
	gosec ./...

# Known vulnerability check
vulncheck:
	govulncheck ./...

# Quick pre-commit check
check: vet build test

# Full CI pipeline
ci: fmt-check vet build test-race security vulncheck

# Clean generated files
clean:
	rm -f coverage.out coverage.html
