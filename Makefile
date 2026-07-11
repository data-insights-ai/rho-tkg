SHELL := /bin/bash

.DEFAULT_GOAL := build

.PHONY: build test test-v test-race test-integration bench-types-footprint bench bench-baseline bench-check bench-graph-baseline bench-graph-production bench-graph-production-small bench-graph-production-large bench-graph-all bench-graph-all-large bench-compare cover cover-gate vet fmt fmt-check lint security vulncheck lint-docker security-docker vulncheck-docker ci-docker check ci clean

BENCH_COUNT ?= 1
BENCH_TIME ?= 1s
PROD_BENCH_COUNT ?= 1

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

# Run the pkg/types memory-footprint / struct-size regression tests (renamed
# from the historical "bench" target so that name is free for the bench/
# suite below — these are ordinary `go test` assertions, not `testing.B`
# benchmarks).
bench-types-footprint:
	go test -v -count=1 -run TestBench ./pkg/types/

# Run the bench/ cross-backend performance suite (see bench/README.md).
# Fixed -benchtime keeps a full run fast and reproducible; -count=1 is the
# quick local/CI signal (bench-check below runs its own fresh comparison
# pass under the same flags).
bench:
	go test -bench=. -benchmem -benchtime=0.3s -count=1 ./bench

# Capture a per-machine baseline for bench-check. Baselines are never
# committed (see .gitignore) — machine-specific, re-capture after any
# hardware/load change or before starting a perf investigation.
bench-baseline:
	go test -bench=. -benchmem -benchtime=0.3s -count=1 -run '^$$' ./bench | tee bench/local-baseline.txt

# Compare bench/local-baseline.txt against a fresh run via benchstat and
# fail if any scenario regressed time by more than 15% (see
# bench/bench-check.sh for the threshold logic and its documented rationale).
bench-check:
	./bench/bench-check.sh

# Run graph performance baseline benchmarks for benchstat comparisons.
bench-graph-baseline:
	go test -run '^$$' -bench 'BenchmarkGraphBaseline|BenchmarkAddNode|BenchmarkAddRelationship|BenchmarkAddNodeLabel|BenchmarkRemoveNodeLabel' -benchmem -benchtime=$(BENCH_TIME) -count=$(BENCH_COUNT) ./pkg/graph/internal/core

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
	go test -run '^$$' -bench 'BenchmarkGraphProduction' -benchmem -benchtime=$(BENCH_TIME) -count=$(PROD_BENCH_COUNT) ./pkg/graph/internal/core

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
	go test -run '^$$' -bench 'BenchmarkGraphProduction' -benchmem -benchtime=$(BENCH_TIME) -count=$(PROD_BENCH_COUNT) ./pkg/graph/internal/core

# Run both routine baseline and production-shaped benchmark suites.
bench-graph-all: bench-graph-baseline bench-graph-production-small

# Run both baseline and large production-shaped benchmark suites.
bench-graph-all-large: bench-graph-baseline bench-graph-production-large

# Compare the common graph benchmarks against historical release tags.
bench-compare:
	./scripts/bench-compare-revisions.sh HEAD 4ee8c9e d0706de

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
	gofmt -w $$(git ls-files '*.go')

# Verify formatting (fails if any tracked Go source file needs formatting).
fmt-check:
	@unformatted=$$(gofmt -l $$(git ls-files '*.go')); \
		test -z "$$unformatted" || (echo "Files need formatting:"; echo "$$unformatted"; exit 1)

# Run golangci-lint
lint:
	golangci-lint run ./...

# Static security analysis
security:
	gosec $$(go list -f '{{.Dir}}' ./...)

# Known vulnerability check
vulncheck:
	govulncheck $$(go list ./...)

# --- Dockerized lint/security/vulncheck ------------------------------------
# golangci-lint, gosec, and govulncheck are often NOT installed on the host.
# Docker is always available here, so run them inside the go.mod-matching
# toolchain image (guarantees Go-version compatibility) with cached named
# volumes so repeated runs are fast. GO_IMAGE auto-tracks go.mod.
GO_VERSION := $(shell awk '/^go /{print $$2}' go.mod)
GO_IMAGE   ?= golang:$(GO_VERSION)
DOCKER_GO  = docker run --rm -v "$(CURDIR)":/src -w /src \
	-v rho-tkg-gocache:/go -v rho-tkg-buildcache:/root/.cache/go-build $(GO_IMAGE)

lint-docker:
	$(DOCKER_GO) sh -c 'go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest && golangci-lint run ./...'

security-docker:
	$(DOCKER_GO) sh -c 'go install github.com/securego/gosec/v2/cmd/gosec@latest && gosec -quiet ./...'

vulncheck-docker:
	$(DOCKER_GO) sh -c 'go install golang.org/x/vuln/cmd/govulncheck@latest && govulncheck ./...'

# Full CI gate using the dockerized tools for the three host-optional binaries
# (fmt-check/vet/build/test-race/cover-gate run natively on the host).
ci-docker: fmt-check vet lint-docker build test-race security-docker vulncheck-docker cover-gate

# Quick pre-commit check
check: vet build test

# Full CI pipeline
ci: fmt-check vet lint build test-race security vulncheck cover-gate

# Clean generated files
clean:
	rm -f coverage.out coverage.html coverage.pkg.out
