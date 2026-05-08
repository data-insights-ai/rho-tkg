SHELL := /bin/bash

.DEFAULT_GOAL := build

.PHONY: build test test-v test-race test-integration bench bench-graph-baseline bench-graph-production bench-graph-production-small bench-graph-production-large bench-graph-all bench-graph-all-large cover cover-gate vet fmt fmt-check lint security vulncheck check ci clean

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

# Coverage gate: fails if total ./pkg/... coverage falls below COVER_MIN
# (default 80, project rule). Wrappers in the sub-API accessors are pinned by
# pkg/graph/subapi_smoke_test.go and pkg/graph/subapi_smoke_extra_test.go;
# the gate exists so a future regression in a non-wrapper package surfaces
# instead of silently lowering the floor.
COVER_MIN ?= 80
cover-gate:
	@go test -short -count=1 -coverpkg=./pkg/... -coverprofile=coverage.pkg.out ./pkg/... > /dev/null
	@total=$$(go tool cover -func=coverage.pkg.out | awk '/^total:/ {gsub("%","",$$3); print $$3}'); \
	awk -v t=$$total -v min=$(COVER_MIN) 'BEGIN { if (t+0 < min+0) { printf "coverage gate: total=%s%% < min=%s%% — failing\n", t, min; exit 1 } else { printf "coverage gate: total=%s%% >= min=%s%% — ok\n", t, min } }'

# Run go vet
vet:
	go vet ./...

# Format code
fmt:
	gofmt -w .

# Verify formatting (fails if any file needs formatting). Skips .claude/
# (agent worktree copies — not part of the source tree).
fmt-check:
	@unformatted=$$(gofmt -l . | grep -v '^\.claude/' || true); \
		test -z "$$unformatted" || (echo "Files need formatting:"; echo "$$unformatted"; exit 1)

# Run golangci-lint
lint:
	golangci-lint run ./...

# Static security analysis
security:
	gosec ./...

# Known vulnerability check
vulncheck:
	govulncheck ./...

# Quick pre-commit check
check: vet build test

# Full CI pipeline
ci: fmt-check vet lint build test-race security vulncheck cover-gate

# Clean generated files
clean:
	rm -f coverage.out coverage.html coverage.pkg.out
