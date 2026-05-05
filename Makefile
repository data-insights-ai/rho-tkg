SHELL := /bin/bash

.DEFAULT_GOAL := build

.PHONY: build test test-v test-race test-integration bench bench-graph-baseline bench-graph-production bench-graph-production-small bench-graph-production-large bench-graph-all bench-graph-all-large cover vet fmt fmt-check security vulncheck check ci clean

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

# Run graph performance baseline benchmarks for benchstat comparisons.
bench-graph-baseline:
	go test -run '^$$' -bench 'BenchmarkGraphBaseline|BenchmarkAddNode|BenchmarkAddRelationship|BenchmarkAddNodeLabel|BenchmarkRemoveNodeLabel' -benchmem -benchtime=1s -count=10 ./pkg/graph

# Run the default small production-shaped graph benchmark profile.
bench-graph-production: bench-graph-production-small

# Run production-shaped graph benchmarks sized for routine benchstat comparisons.
bench-graph-production-small:
	TKG_BENCH_NODES=10000 \
	TKG_BENCH_FANOUT=5 \
	TKG_BENCH_HUB_DEGREE=1000 \
	TKG_BENCH_HISTORY_NODES=128 \
	TKG_BENCH_HISTORY_DAYS=30 \
	TKG_BENCH_EXPORT_NODES=2000 \
	TKG_BENCH_EXPORT_FANOUT=2 \
	TKG_BENCH_BATCH_NODES=256 \
	TKG_BENCH_TIERED_CASES=1024 \
	TKG_BENCH_TIERED_WARM_SIGNALS=2048 \
	TKG_BENCH_TIERED_HOT_SIGNALS=2048 \
	TKG_BENCH_SURFACE_NODES=2048 \
	go test -run '^$$' -bench 'BenchmarkGraphProduction' -benchmem -benchtime=1s -count=5 ./pkg/graph

# Run production-shaped graph benchmarks with large stress fixtures.
bench-graph-production-large:
	TKG_BENCH_NODES=100000 \
	TKG_BENCH_FANOUT=10 \
	TKG_BENCH_HUB_DEGREE=10000 \
	TKG_BENCH_HISTORY_NODES=32 \
	TKG_BENCH_HISTORY_DAYS=3000 \
	TKG_BENCH_EXPORT_NODES=10000 \
	TKG_BENCH_EXPORT_FANOUT=3 \
	TKG_BENCH_BATCH_NODES=1024 \
	TKG_BENCH_TIERED_CASES=10000 \
	TKG_BENCH_TIERED_WARM_SIGNALS=20000 \
	TKG_BENCH_TIERED_HOT_SIGNALS=20000 \
	TKG_BENCH_SURFACE_NODES=10000 \
	go test -run '^$$' -bench 'BenchmarkGraphProduction' -benchmem -benchtime=1s -count=3 ./pkg/graph

# Run both routine baseline and production-shaped benchmark suites.
bench-graph-all: bench-graph-baseline bench-graph-production-small

# Run both baseline and large production-shaped benchmark suites.
bench-graph-all-large: bench-graph-baseline bench-graph-production-large

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
