SHELL := /bin/bash

.DEFAULT_GOAL := build

.PHONY: tools build test test-v test-race test-race-script test-integration bench-types-footprint bench bench-baseline bench-check bench-graph-baseline bench-graph-production bench-graph-production-small bench-graph-production-large bench-graph-all bench-graph-all-large bench-compare cover cover-gate vet fmt fmt-check lint security vulncheck lint-docker security-docker vulncheck-docker ci-docker ci-docker-run ci-fast ci-fast-run hooks fuzz-smoke check ci clean check-metakv-reap

BENCH_COUNT ?= 1
BENCH_TIME ?= 1s
PROD_BENCH_COUNT ?= 1
# CI opts into four exhaustive core batches; local defaults remain unsplit.
ROOT_RACE_CORE_SHARDS ?= 4
GOLANGCI_LINT_VERSION ?= v2.13.2
GOSEC_VERSION ?= v2.29.0
GOVULNCHECK_VERSION ?= v1.7.0

# Build (verify compilation)
# The library also builds for 32-bit targets (ARM32: DICT broke it unnoticed).
build:
	go build ./...
	GOOS=linux GOARCH=386 go build ./...

# Run unit tests
test:
	go test -short -count=1 ./...

# Run tests with verbose output
test-v:
	go test -short -count=1 -v ./...

# Run tests with race detector. Local default (since v4.76.0): the parts of
# scripts/test-race.sh in sequence, so each heavy package (core shards, graph,
# memory, badger) has the machine to itself; one unsplit `go test ./...` ran
# them in parallel and badger hit its 10-minute timeout on a busy machine.
# CI: ROOT_RACE_CORE_SHARDS=4 plus RACE_SHARD=<part> runs one matrix part
# (scripts/test-race.sh; RACE_SHARD=list prints the parts).
test-race:
	$(SHELL) scripts/test-race.sh "$(ROOT_RACE_CORE_SHARDS)"

# The long tier: every test a plain `testing.Short()` skips (no race exception),
# run without -short or race (scripts/test-long.sh; LONG_TIER=list prints them).
test-long:
	$(SHELL) scripts/test-long.sh

# Tests of the race-gate script itself (fake go, no toolchain needed).
test-race-script:
	$(SHELL) scripts/test-race-test.sh

# Run integration tests (long-running, not short)
test-integration:
	go test -count=1 -run Integration ./...

# Run the pkg/types memory-footprint / struct-size regression tests (renamed
# from the historical "bench" target so that name is free for the bench/
# suite below — these are ordinary `go test` assertions, not `testing.B`
# benchmarks).
bench-types-footprint:
	go test -v -count=1 -run TestBench ./pkg/types/

# Run the bench/ cross-backend performance suite
# (see docs/CONTRIBUTING.md "Benchmarks").
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

# Compare the common graph micro-benchmarks across revisions (worktree-per-ref).
# Defaults: HEAD vs v4.27.0 vs v4.26.0. Pass explicit refs to override, e.g.
#   make bench-compare ARGS='HEAD v4.28.1 v4.25.0'
# (ARGS is not a Make variable here — call the script directly for custom refs.)
bench-compare:
	./bench/bench-compare-revisions.sh

# Run tests with coverage report
cover:
	go test -short -count=1 -coverpkg=./... -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Coverage gate: fails if total ./pkg/... coverage falls below COVER_MIN
# (default 80, project rule), so a regression surfaces instead of silently
# lowering the floor. The sub-API wrappers are covered by their packages' own
# forwarding tests.
COVER_MIN ?= 80
cover-gate:
	@go test -short -count=1 -coverpkg=./pkg/... -coverprofile=coverage.pkg.out ./pkg/... > coverage.test.log 2>&1 || { grep -E -B2 -A20 -- '--- FAIL|^FAIL|panic:' coverage.test.log | head -200; exit 1; }
	@total=$$(go tool cover -func=coverage.pkg.out | awk '/^total:/ {gsub("%","",$$3); print $$3}'); \
	awk -v t=$$total -v min=$(COVER_MIN) 'BEGIN { if (t+0 < min+0) { printf "coverage gate: total=%s%% < min=%s%% — failing\n", t, min; exit 1 } else { printf "coverage gate: total=%s%% >= min=%s%% — ok\n", t, min } }'

# Run go vet
vet:
	go vet ./...

# BACKLOG 13l: fails if pkg/graph/internal/core persists a MetaKV key with no
# explicit reap-vs-preserve classification for Admin.Reset/ChangeClear — see
# metakv_reap_policy.go and metakv_reap_policy_check_test.go.
check-metakv-reap:
	scripts/test-named.sh ./pkg/graph/internal/core/ TestMetaKVReapPolicyCoversEveryKey --

# Install the pre-commit gate (scripts/hooks/pre-commit) for every worktree of this clone.
hooks:
	git config core.hooksPath scripts/hooks

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
	gosec -quiet $$(go list -f '{{.Dir}}' ./...)

# Known vulnerability check
vulncheck:
	govulncheck $$(go list ./...)

# Pin security tools so CI results change only when this repository chooses to
# update the analyzers, rather than whenever an upstream latest tag moves.
tools:
	go install github.com/securego/gosec/v2/cmd/gosec@$(GOSEC_VERSION)
	go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)

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
	$(DOCKER_GO) sh -c 'go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) && golangci-lint run ./...'

security-docker:
	$(DOCKER_GO) sh -c 'go install github.com/securego/gosec/v2/cmd/gosec@$(GOSEC_VERSION) && gosec -quiet ./...'

vulncheck-docker:
	$(DOCKER_GO) sh -c 'go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) && govulncheck ./...'

# Minor release gate, local (GitHub CI on a tag only builds, vets and runs the
# short tests): the dockerized lint/gosec/govulncheck, the full race suite,
# coverage, the meta-key reap policy, a short fuzz of every trust-boundary
# harness, the long tier and the ledger check.
ci-docker:
	@scripts/gate-time.sh ci-docker $(MAKE) ci-docker-run

ci-docker-run: fmt-check vet lint-docker build test-race test-long security-docker vulncheck-docker cover-gate check-metakv-reap fuzz-smoke
	scripts/check-ledger.sh

# fuzz-smoke runs each trust-boundary fuzz harness for FUZZTIME (nothing runs
# them on a schedule); a crasher lands in testdata/fuzz/ and is committed with
# its fix.
FUZZTIME ?= 20s
fuzz-smoke:
	scripts/test-named.sh ./pkg/graph/internal/core/ 'FuzzImport|FuzzApplyChange' --
	scripts/test-named.sh ./pkg/graph/internal/storeutil/ 'FuzzWireToNodeChecked|FuzzWireToRelChecked|FuzzUnmarshalNodeWireWithKeys' --
	go test -run '^$$' -fuzz=FuzzImport -fuzztime=$(FUZZTIME) ./pkg/graph/internal/core/
	go test -run '^$$' -fuzz=FuzzApplyChange -fuzztime=$(FUZZTIME) ./pkg/graph/internal/core/
	go test -run '^$$' -fuzz=FuzzWireToNodeChecked -fuzztime=$(FUZZTIME) ./pkg/graph/internal/storeutil/
	go test -run '^$$' -fuzz=FuzzWireToRelChecked -fuzztime=$(FUZZTIME) ./pkg/graph/internal/storeutil/
	go test -run '^$$' -fuzz=FuzzUnmarshalNodeWireWithKeys -fuzztime=$(FUZZTIME) ./pkg/graph/internal/storeutil/

# Patch release gate (todo 115/116; minors use ci-docker). scripts/patchgate plans it from the batch since the
# last release tag and refuses new exported API (a door = a minor). Runs fmt-check, vet, build, lint, -short of
# packages whose shipped code changed, -race of only the changed tests (whole package for a changed helper or
# tool), the docs tests and the ledger check.
ci-fast:
	@scripts/gate-time.sh ci-fast $(MAKE) ci-fast-run

ci-fast-run: fmt-check vet build lint-docker
	@base=$$(git tag --sort=-creatordate | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$$' | head -1); \
	echo "ci-fast: batch $$base..work tree"; \
	targets=$$(go run ./scripts/patchgate "$$base") || exit 1; \
	short=$$(echo "$$targets" | awk '$$1=="short"{print $$2}'); \
	if [ -n "$$short" ]; then go test -short -count=1 $$short || exit 1; fi; \
	echo "$$targets" | while read -r kind pkg run; do \
	  [ "$$kind" = race ] || continue; \
	  if [ -n "$$run" ]; then \
	    names=$${run#^(}; scripts/test-named.sh "$$pkg" "$${names%)\$$}" -- -short -race || exit 1; \
	  else \
	    go test -short -race -count=1 "$$pkg" || exit 1; \
	  fi; \
	done || exit 1; \
	go test -count=1 -run 'Docs|Consistency|MD|Errors' ./... >/dev/null || { echo "ci-fast: docs tests failed"; exit 1; }; \
	scripts/test-named.sh ./pkg/graph/internal/core/ 'TestNoMDFileOutsideTheDocumentation|TestDocsMetadataMatchesSourceOfTruth' -- >/dev/null || exit 1; \
	scripts/check-ledger.sh && echo "ci-fast: ok"

# Quick pre-commit check
check: vet build test

# Full CI pipeline
ci: fmt-check vet lint build test-race security vulncheck cover-gate check-metakv-reap

# Clean generated files
clean:
	rm -f coverage.out coverage.html coverage.pkg.out
